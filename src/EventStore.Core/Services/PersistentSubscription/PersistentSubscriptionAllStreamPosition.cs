using System;
using EventStore.Core.Data;

namespace EventStore.Core.Services.PersistentSubscription {
	public class PersistentSubscriptionAllStreamPosition : IPersistentSubscriptionStreamPosition {
		public bool IsSingleStreamPosition => false;
		public long StreamEventNumber => throw new InvalidOperationException();
		public bool IsAllStreamPosition => true;
		public bool IsLivePosition => _commitPosition == -1L && _preparePosition == -1L;
		public (long Commit, long Prepare) TFPosition => (_commitPosition, _preparePosition);
		private readonly long _commitPosition;
		private readonly long _preparePosition;

		public PersistentSubscriptionAllStreamPosition(long commitPosition, long preparePosition) {
			_commitPosition = commitPosition;
			_preparePosition = preparePosition;
		}
		public override string ToString() => $"C:{_commitPosition}/P:{_preparePosition}";
	}
}
