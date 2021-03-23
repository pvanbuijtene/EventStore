using System;

namespace EventStore.Core.Services.PersistentSubscription {
	public interface IPersistentSubscriptionStreamPosition {
		bool IsSingleStreamPosition { get; }
		long StreamEventNumber { get; }
		bool IsAllStreamPosition { get; }
		(long Commit, long Prepare) TFPosition { get; }
		string ToString();
	}
}
