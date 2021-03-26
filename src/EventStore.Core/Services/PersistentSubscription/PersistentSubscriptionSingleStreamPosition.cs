using System;
using EventStore.Core.Data;

namespace EventStore.Core.Services.PersistentSubscription {
	public class PersistentSubscriptionSingleStreamPosition : IPersistentSubscriptionStreamPosition {
		public bool IsSingleStreamPosition => true;
		public long StreamEventNumber { get; }
		public bool IsAllStreamPosition => false;
		public bool IsLivePosition => StreamEventNumber == -1L;
		public (long Commit, long Prepare) TFPosition => throw new InvalidOperationException();
		public PersistentSubscriptionSingleStreamPosition(long eventNumber) {
			StreamEventNumber = eventNumber;
		}
		public override string ToString() => StreamEventNumber.ToString();
	}
}
