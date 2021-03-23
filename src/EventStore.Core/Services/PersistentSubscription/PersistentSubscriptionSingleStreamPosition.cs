using System;

namespace EventStore.Core.Services.PersistentSubscription {
	public class PersistentSubscriptionSingleStreamPosition : IPersistentSubscriptionStreamPosition {
		public bool IsSingleStreamPosition => true;
		public long StreamEventNumber { get; }
		public bool IsAllStreamPosition => false;
		public (long Commit, long Prepare) TFPosition => throw new InvalidOperationException();
		public PersistentSubscriptionSingleStreamPosition(long eventNumber) {
			StreamEventNumber = eventNumber;
		}
		public override string ToString() => StreamEventNumber.ToString();
	}
}
