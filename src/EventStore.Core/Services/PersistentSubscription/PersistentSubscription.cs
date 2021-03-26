#nullable enable
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.PersistentSubscription.ConsumerStrategy;
using ILogger = Serilog.ILogger;

namespace EventStore.Core.Services.PersistentSubscription {
	//TODO GFY REFACTOR TO USE ACTUAL STATE MACHINE
	public class PersistentSubscription {
		private static readonly ILogger Log = Serilog.Log.ForContext<PersistentSubscription>();

		public string SubscriptionId {
			get { return _settings.SubscriptionId; }
		}

		public string EventSource {
			get { return _settings.EventSource.ToString(); }
		}

		public string GroupName {
			get { return _settings.GroupName; }
		}

		public bool ResolveLinkTos {
			get { return _settings.ResolveLinkTos; }
		}

		internal PersistentSubscriptionClientCollection _pushClients;
		private readonly PersistentSubscriptionStats _statistics;
		private readonly Stopwatch _totalTimeWatch;
		private readonly OutstandingMessageCache _outstandingMessages;
		internal StreamBuffer StreamBuffer => _streamBufferSource.Task.Result;
		private readonly TaskCompletionSource<StreamBuffer> _streamBufferSource;
		private PersistentSubscriptionState _state = PersistentSubscriptionState.NotReady;
		private IPersistentSubscriptionStreamPosition _nextEventToPullFrom;
		private bool _skipFirstEvent;
		private DateTime _lastCheckPointTime = DateTime.MinValue;
		private readonly PersistentSubscriptionParams _settings;
		private long _nextSequenceNumber;
		private long _lastCheckpointedSequenceNumber;
		private long _lastKnownSequenceNumber;
		private IPersistentSubscriptionStreamPosition _lastKnownMessage;
		private readonly object _lock = new object();

		public bool HasClients {
			get { return _pushClients.Count > 0; }
		}

		public int ClientCount {
			get { return _pushClients.Count; }
		}

		public bool HasReachedMaxClientCount {
			get { return _settings.MaxSubscriberCount != 0 && _pushClients.Count >= _settings.MaxSubscriberCount; }
		}

		public PersistentSubscriptionState State {
			get { return _state; }
		}

		public int OutstandingMessageCount {
			get { return _outstandingMessages.Count; } //use outstanding not connections as http shows up here too
		}

		public PersistentSubscription(PersistentSubscriptionParams persistentSubscriptionParams) {
			Ensure.NotNull(persistentSubscriptionParams.StreamReader, "eventLoader");
			Ensure.NotNull(persistentSubscriptionParams.CheckpointReader, "checkpointReader");
			Ensure.NotNull(persistentSubscriptionParams.CheckpointWriter, "checkpointWriter");
			Ensure.NotNull(persistentSubscriptionParams.MessageParker, "messageParker");
			Ensure.NotNull(persistentSubscriptionParams.SubscriptionId, "subscriptionId");
			Ensure.NotNull(persistentSubscriptionParams.EventSource, "eventSource");
			Ensure.NotNull(persistentSubscriptionParams.GroupName, "groupName");
			if (persistentSubscriptionParams.ReadBatchSize >= persistentSubscriptionParams.BufferSize) {
				throw new ArgumentOutOfRangeException($"{nameof(persistentSubscriptionParams.ReadBatchSize)} may not be greater than or equal to {nameof(persistentSubscriptionParams.BufferSize)}");
			}
			_nextEventToPullFrom = _settings.EventSource.StreamStartPosition;
			_totalTimeWatch = new Stopwatch();
			_settings = persistentSubscriptionParams;
			_totalTimeWatch.Start();
			_statistics = new PersistentSubscriptionStats(this, _settings, _totalTimeWatch);
			_outstandingMessages = new OutstandingMessageCache();
			_streamBufferSource = new TaskCompletionSource<StreamBuffer>(TaskCreationOptions.RunContinuationsAsynchronously);
			InitAsNew();
		}

		public void InitAsNew() {
			_state = PersistentSubscriptionState.NotReady;
			_nextSequenceNumber = 0L;
			_lastCheckpointedSequenceNumber = -1L;
			_lastKnownSequenceNumber = -1L;
			_lastKnownMessage = _settings.EventSource.StreamStartPosition;
			_statistics.SetLastKnownEventNumber(-1);
			_settings.CheckpointReader.BeginLoadState(SubscriptionId, OnCheckpointLoaded);

			_pushClients = new PersistentSubscriptionClientCollection(_settings.ConsumerStrategy);
		}

		private void OnCheckpointLoaded(string? checkpoint) {
			lock (_lock) {
				_state = PersistentSubscriptionState.Behind;
				if (checkpoint == null) {
					Log.Debug("Subscription {subscriptionId}: no checkpoint found", _settings.SubscriptionId);

					Log.Debug("Start from = " + _settings.StartFrom);

					_nextEventToPullFrom = _settings.StartFrom.IsLivePosition ? _settings.EventSource.StreamStartPosition : _settings.StartFrom;
					_streamBufferSource.SetResult(new StreamBuffer(_settings.BufferSize, _settings.LiveBufferSize, -1,
					!_settings.StartFrom.IsLivePosition));
					TryReadingNewBatch();
				} else {
					_nextEventToPullFrom = _settings.EventSource.GetStreamPositionFor(checkpoint);
					_skipFirstEvent = true; //skip the checkpointed event
					Log.Debug("Subscription {subscriptionId}: read checkpoint {checkpoint}", _settings.SubscriptionId,
						checkpoint);
					_streamBufferSource.SetResult(new StreamBuffer(_settings.BufferSize, _settings.LiveBufferSize, -1, true));
					_settings.MessageParker.BeginLoadStats(TryReadingNewBatch);
				}
			}
		}

		public void TryReadingNewBatch() {
			lock (_lock) {
				if ((_state & PersistentSubscriptionState.OutstandingPageRequest) > 0)
					return;
				if (StreamBuffer.Live) {
					SetLive();
					return;
				}

				if (!StreamBuffer.CanAccept(_settings.ReadBatchSize))
					return;
				_state |= PersistentSubscriptionState.OutstandingPageRequest;
				_settings.StreamReader.BeginReadEvents(_settings.EventSource, _nextEventToPullFrom,
					Math.Max(_settings.ReadBatchSize, 10), _settings.ReadBatchSize, _settings.ResolveLinkTos,
					HandleReadCompleted);
				}
		}

		private void SetLive() {
			//TODO GFY this is hacky and just trying to keep the state at this level when it
			//lives in the streambuffer its for reporting reasons and likely should be revisited
			//at some point.
			_state &= ~PersistentSubscriptionState.Behind;
			_state |= PersistentSubscriptionState.Live;
		}

		private void SetBehind() {
			_state |= PersistentSubscriptionState.Behind;
			_state &= ~PersistentSubscriptionState.Live;
		}

		public void HandleReadCompleted(ResolvedEvent[] events, IPersistentSubscriptionStreamPosition newPosition, bool isEndOfStream) {
			lock (_lock) {
				if ((_state & PersistentSubscriptionState.OutstandingPageRequest) == 0)
					return;
				_state &= ~PersistentSubscriptionState.OutstandingPageRequest;
				if (StreamBuffer.Live)
					return;
				foreach (var ev in events) {
					if (_skipFirstEvent) {
						_skipFirstEvent = false;
						continue;
					}
					StreamBuffer.AddReadMessage(new OutstandingMessage(ev.OriginalEvent.EventId, null, ev, 0, null));
				}

				if (events.Length > 0) {
					_statistics.SetLastKnownEventNumber(events[events.Length - 1].OriginalEventNumber);
				}

				if (StreamBuffer.Live) {
					SetLive();
				}

				if (isEndOfStream) {
					if (StreamBuffer.TryMoveToLive()) {
						SetLive();
						return;
					}
				}

				_nextEventToPullFrom = newPosition;
				TryReadingNewBatch();
				TryPushingMessagesToClients();
			}
		}

		private void TryPushingMessagesToClients() {
			lock (_lock) {
				if (_state == PersistentSubscriptionState.NotReady)
					return;

				foreach (StreamBuffer.OutstandingMessagePointer messagePointer in StreamBuffer.Scan()) {
					OutstandingMessage message = messagePointer.Message;
					ConsumerPushResult result =
						_pushClients.PushMessageToClient(message.ResolvedEvent, message.RetryCount);
					if (result == ConsumerPushResult.Sent) {
						messagePointer.MarkSent();
						MarkBeginProcessing(message, out var sequenceNumber);
						if (!message.IsReplayedEvent && sequenceNumber.HasValue && sequenceNumber.Value > _lastKnownSequenceNumber) {
							_lastKnownSequenceNumber = sequenceNumber.Value;
							_lastKnownMessage = _settings.EventSource.GetStreamPositionFor(message.ResolvedEvent);
						}
					} else if (result == ConsumerPushResult.Skipped) {
						// The consumer strategy skipped the message so leave it in the buffer and continue.
					} else if (result == ConsumerPushResult.NoMoreCapacity) {
						return;
					}
				}
			}
		}

		public void NotifyLiveSubscriptionMessage(ResolvedEvent resolvedEvent) {
			lock (_lock) {
				if (_settings.StartFrom is PersistentSubscriptionSingleStreamPosition streamCheckpoint) {
					if(resolvedEvent.OriginalEvent.EventNumber < streamCheckpoint.StreamEventNumber)
						return;
				}
				if (_state == PersistentSubscriptionState.NotReady)
					return;
				_statistics.SetLastKnownEventNumber(resolvedEvent.OriginalEventNumber);
				var waslive = StreamBuffer.Live; //hacky
				StreamBuffer.AddLiveMessage(new OutstandingMessage(resolvedEvent.OriginalEvent.EventId, null,
					resolvedEvent, 0, null));
				if (!StreamBuffer.Live) {
					SetBehind();
					if (waslive)
						_nextEventToPullFrom = _settings.EventSource.GetStreamPositionFor(resolvedEvent);
				}

				TryPushingMessagesToClients();
			}
		}

		public IEnumerable<(ResolvedEvent ResolvedEvent, int RetryCount)> GetNextNOrLessMessages(int count) {
			lock (_lock) {
				foreach (var messagePointer in StreamBuffer.Scan().Take(count)) {
					messagePointer.MarkSent();
					MarkBeginProcessing(messagePointer.Message, out var sequenceNumber);
					if (!messagePointer.Message.IsReplayedEvent && sequenceNumber.HasValue && sequenceNumber.Value > _lastKnownSequenceNumber) {
						_lastKnownSequenceNumber = sequenceNumber.Value;
						_lastKnownMessage = _settings.EventSource.GetStreamPositionFor(messagePointer.Message.ResolvedEvent);
					}
					yield return (messagePointer.Message.ResolvedEvent, messagePointer.Message.RetryCount);
				}
			}
		}

		private void MarkBeginProcessing(OutstandingMessage message, out long? eventSequenceNumber) {
			_statistics.IncrementProcessed();
			if (!message.EventSequenceNumber.HasValue && !message.IsReplayedEvent) {
				//assign sequence number to the event that has just been pushed to a client
				//if it doesn't already have one (i.e it's a retried message) and is not a parked message being replayed
				eventSequenceNumber = _nextSequenceNumber++;
				message = new OutstandingMessage(message.EventId, message.HandlingClient,
					message.ResolvedEvent, message.RetryCount, eventSequenceNumber);
			} else if (message.EventSequenceNumber.HasValue) { //retried message
				eventSequenceNumber = message.EventSequenceNumber;
			} else { //replayed parked message
				eventSequenceNumber = null;
			}

			StartMessage(message,
				_settings.MessageTimeout == TimeSpan.MaxValue
					? DateTime.MaxValue
					: DateTime.UtcNow + _settings.MessageTimeout);
		}

		public void AddClient(Guid correlationId, Guid connectionId, string connectionName, IEnvelope envelope, int maxInFlight, string user,
			string @from) {
			lock (_lock) {
				var client = new PersistentSubscriptionClient(correlationId, connectionId, connectionName, envelope, maxInFlight, user,
					@from, _totalTimeWatch, _settings.ExtraStatistics);
				_pushClients.AddClient(client);
				TryPushingMessagesToClients();
			}
		}

		public void Shutdown() {
			lock (_lock) {
				_pushClients.ShutdownAll();
			}
		}

		public void RemoveClientByConnectionId(Guid connectionId) {
			lock (_lock) {
				var lostMessages =
					_pushClients.RemoveClientByConnectionId(connectionId).OrderBy(v => v.ResolvedEvent.OriginalEventNumber);
				foreach (var m in lostMessages) {
					if (ActionTakenForRetriedMessage(m))
						return;
					RetryMessage(m.ResolvedEvent, m.RetryCount, m.EventSequenceNumber);
				}

				TryPushingMessagesToClients();
			}
		}

		public void RemoveClientByCorrelationId(Guid correlationId, bool sendDropNotification) {
			lock (_lock) {
				var lostMessages = _pushClients.RemoveClientByCorrelationId(correlationId, sendDropNotification)
					.OrderBy(v => v.ResolvedEvent.OriginalEventNumber);
				foreach (var m in lostMessages) {
					RetryMessage(m.ResolvedEvent, m.RetryCount, m.EventSequenceNumber);
				}

				TryPushingMessagesToClients();
			}
		}

		public void TryMarkCheckpoint(bool isTimeCheck) {
			lock (_lock) {
				ResolvedEvent? lowestEvent;
				long lowestSequenceNumber;

				(lowestEvent, lowestSequenceNumber)  = _outstandingMessages.GetLowestPosition();
				var (lowestRetryEvent, lowestRetrySequenceNumber) = StreamBuffer.GetLowestRetry();

				if (lowestRetrySequenceNumber < lowestSequenceNumber) {
					lowestSequenceNumber = lowestRetrySequenceNumber;
					lowestEvent = lowestRetryEvent;
				}

				IPersistentSubscriptionStreamPosition lowestPosition;

				if (lowestSequenceNumber != long.MaxValue) {
					Debug.Assert(lowestEvent.HasValue);
					// Subtract 1 from retry and outstanding as those messages have not been processed yet.
					lowestSequenceNumber--;
					lowestPosition = _settings.EventSource.GetStreamPositionFor(lowestEvent.Value);
				} else {
					Debug.Assert(!lowestEvent.HasValue);
					lowestSequenceNumber = _lastKnownSequenceNumber;
					lowestPosition = _lastKnownMessage;
				}

				if (lowestSequenceNumber == -1)
					return;

				//no outstanding messages. in this case we can say that the last known
				//event would be our checkpoint place (we have already completed it)
				var difference = lowestSequenceNumber - _lastCheckpointedSequenceNumber;
				var now = DateTime.UtcNow;
				var timedifference = now - _lastCheckPointTime;
				if (timedifference < _settings.CheckPointAfter && difference < _settings.MaxCheckPointCount)
					return;
				if ((difference >= _settings.MinCheckPointCount && isTimeCheck) ||
					difference >= _settings.MaxCheckPointCount) {
					_lastCheckPointTime = now;
					_lastCheckpointedSequenceNumber = lowestSequenceNumber;
					_settings.CheckpointWriter.BeginWriteState(lowestPosition);
					_statistics.SetLastCheckPoint(lowestPosition);
				}
			}
		}

		public void AcknowledgeMessagesProcessed(Guid correlationId, Guid[] processedEventIds) {
			lock (_lock) {
				RemoveProcessingMessages(processedEventIds);
				TryMarkCheckpoint(false);
				TryReadingNewBatch();
				TryPushingMessagesToClients();
			}
		}

		public void NotAcknowledgeMessagesProcessed(Guid correlationId, Guid[] processedEventIds, NakAction action,
			string reason) {
			lock (_lock) {
				foreach (var id in processedEventIds) {
					Log.Information("Message NAK'ed id {id} action to take {action} reason '{reason}'", id, action,
						reason ?? "");
					HandleNackedMessage(action, id, reason);
				}

				RemoveProcessingMessages(processedEventIds);
				TryMarkCheckpoint(false);
				TryReadingNewBatch();
				TryPushingMessagesToClients();
			}
		}

		private void HandleNackedMessage(NakAction action, Guid id, string reason) {
			OutstandingMessage e;
			switch (action) {
				case NakAction.Retry:
				case NakAction.Unknown:
					if (_outstandingMessages.GetMessageById(id, out e)) {
						if (!ActionTakenForRetriedMessage(e)) {
							RetryMessage(e.ResolvedEvent, e.RetryCount, e.EventSequenceNumber);
						}
					}

					break;
				case NakAction.Park:
					if (_outstandingMessages.GetMessageById(id, out e)) {
						ParkMessage(e.ResolvedEvent, "Client explicitly NAK'ed message.\n" + reason, 0);
					}

					break;
				case NakAction.Stop:
					StopSubscription();
					break;
				case NakAction.Skip:
					SkipMessage(id);
					break;
				default:
					SkipMessage(id);
					break;
			}
		}

		private void ParkMessage(ResolvedEvent resolvedEvent, string reason, int count) {
			_settings.MessageParker.BeginParkMessage(resolvedEvent, reason, (e, result) => {
				if (result != OperationResult.Success) {
					if (count < 5) {
						Log.Information("Unable to park message {stream}/{eventNumber} operation failed {e} retrying",
							e.OriginalStreamId,
							e.OriginalEventNumber, result);
						ParkMessage(e, reason, count + 1);
						return;
					}

					Log.Error(
						"Unable to park message {stream}/{eventNumber} operation failed {e} after retries. Possible message loss",
						e.OriginalStreamId,
						e.OriginalEventNumber, result);
				}

				lock (_lock) {
					_outstandingMessages.Remove(e.OriginalEvent.EventId);
					_pushClients.RemoveProcessingMessages(e.OriginalEvent.EventId);
					TryPushingMessagesToClients();
				}
			});
		}

		
		public void RetryParkedMessages(long? stopAt) {
			lock (_lock) {
				if ((_state & PersistentSubscriptionState.ReplayingParkedMessages) > 0)
					return; //already replaying
				_state |= PersistentSubscriptionState.ReplayingParkedMessages;
				_settings.MessageParker.BeginReadEndSequence(end => {
					if (!end.HasValue) {
						_state ^= PersistentSubscriptionState.ReplayingParkedMessages;
						return; //nothing to do.
					}

					var stopRead = stopAt.HasValue ? Math.Min(stopAt.Value, end.Value + 1) : end.Value + 1;
					TryReadingParkedMessagesFrom(0,stopRead);
				});
			}
		}

		private void TryReadingParkedMessagesFrom(long position, long stopAt) {
			if ((_state & PersistentSubscriptionState.ReplayingParkedMessages) == 0)
				return; //not replaying

			Ensure.Positive(stopAt - position, "count");

			var count = (int)Math.Min(stopAt - position, _settings.ReadBatchSize);
			_settings.StreamReader.BeginReadEvents(
				new PersistentSubscriptionSingleStreamEventSource(_settings.ParkedMessageStream),
				new PersistentSubscriptionSingleStreamPosition(position),
				count,
				_settings.ReadBatchSize, true,
				(events, newposition, isstop) => HandleParkedReadCompleted(events, newposition, isstop, stopAt));
		}

		public void HandleParkedReadCompleted(ResolvedEvent[] events, IPersistentSubscriptionStreamPosition newPosition, bool isEndofStream, long stopAt) {
			lock (_lock) {
				if ((_state & PersistentSubscriptionState.ReplayingParkedMessages) == 0)
					return;

				foreach (var ev in events) {
					if (ev.OriginalEventNumber == stopAt) {
						break;
					}

					Log.Debug("Retrying event {eventId} {stream}/{eventNumber} on subscription {subscriptionId}",
						ev.OriginalEvent.EventId, ev.OriginalStreamId, ev.OriginalEventNumber,
						_settings.SubscriptionId);
					StreamBuffer.AddRetry(new OutstandingMessage(ev.OriginalEvent.EventId, null, ev, 0, null));
				}

				TryPushingMessagesToClients();

				var newStreamPosition = newPosition as PersistentSubscriptionSingleStreamPosition;
				Ensure.NotNull(newStreamPosition, "newStreamPosition");

				if (isEndofStream || stopAt <= newStreamPosition.StreamEventNumber) {
					var replayedEnd = newStreamPosition.StreamEventNumber == -1 ? stopAt : Math.Min(stopAt, newStreamPosition.StreamEventNumber);
					_settings.MessageParker.BeginMarkParkedMessagesReprocessed(replayedEnd);
					_state ^= PersistentSubscriptionState.ReplayingParkedMessages;
				} else {
					TryReadingParkedMessagesFrom(newStreamPosition.StreamEventNumber, stopAt);
				}
			}
		}

		private void StartMessage(OutstandingMessage message, DateTime expires) {
			var result = _outstandingMessages.StartMessage(message, expires);

			if (result == StartMessageResult.SkippedDuplicate) {
				Log.Warning("Skipping message {stream}/{eventNumber} with duplicate eventId {eventId}",
					message.ResolvedEvent.OriginalStreamId,
					message.ResolvedEvent.OriginalEventNumber,
					message.EventId);
			}
		}

		private void SkipMessage(Guid id) {
			_outstandingMessages.Remove(id);
		}

		private void StopSubscription() {
			//TODO CC Stop subscription?
		}

		private void RemoveProcessingMessages(Guid[] processedEventIds) {
			_pushClients.RemoveProcessingMessages(processedEventIds);
			foreach (var id in processedEventIds) {
				_outstandingMessages.Remove(id);
			}
		}

		public void NotifyClockTick(DateTime time) {
			lock (_lock) {
				if (_state == PersistentSubscriptionState.NotReady)
					return;

				foreach (var message in _outstandingMessages.GetMessagesExpiringBefore(time)) {
					if (!ActionTakenForRetriedMessage(message)) {
						RetryMessage(message.ResolvedEvent, message.RetryCount, message.EventSequenceNumber);
					}
				}

				TryPushingMessagesToClients();
				TryMarkCheckpoint(true);
				if ((_state & PersistentSubscriptionState.Behind |
					 PersistentSubscriptionState.OutstandingPageRequest) ==
					PersistentSubscriptionState.Behind)
					TryReadingNewBatch();
			}
		}

		private bool ActionTakenForRetriedMessage(OutstandingMessage message) {
			if (message.RetryCount < _settings.MaxRetryCount)
				return false;
			ParkMessage(message.ResolvedEvent, string.Format("Reached retry count of {0}", _settings.MaxRetryCount), 0);
			return true;
		}

		private void RetryMessage(ResolvedEvent @event, int count, long? eventSequenceNumber) {
			Log.Debug("Retrying message {subscriptionId} {stream}/{eventNumber}", SubscriptionId,
				@event.OriginalStreamId, @event.OriginalEventNumber);
			_outstandingMessages.Remove(@event.OriginalEvent.EventId);
			_pushClients.RemoveProcessingMessages(@event.OriginalEvent.EventId);
			StreamBuffer.AddRetry(new OutstandingMessage(@event.OriginalEvent.EventId, null, @event, count + 1, eventSequenceNumber));
		}

		public MonitoringMessage.SubscriptionInfo GetStatistics() {
			return _statistics.GetStatistics();
		}

		public void RetrySingleMessage(ResolvedEvent @event, long? eventSequenceNumber) {
			StreamBuffer.AddRetry(new OutstandingMessage(@event.OriginalEvent.EventId, null, @event, 0, eventSequenceNumber));
		}

		public void Delete() {
			_settings.CheckpointWriter.BeginDelete(x => { });
			_settings.MessageParker.BeginDelete(x => { });
		}
	}
}
