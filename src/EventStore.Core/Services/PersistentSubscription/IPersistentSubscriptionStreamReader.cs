using System;
using EventStore.Core.Data;

namespace EventStore.Core.Services.PersistentSubscription {
	public interface IPersistentSubscriptionStreamReader {
		void BeginReadEvents(IPersistentSubscriptionEventSource eventSource, IPersistentSubscriptionStreamPosition startPosition, int countToLoad, int batchSize, bool resolveLinkTos,
			Action<ResolvedEvent[], IPersistentSubscriptionStreamPosition, bool> onEventsFound);
	}
}
