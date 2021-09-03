using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.Data;
using Serilog;

namespace EventStore.Core.Tests.Integration {
	//[TestFixture(typeof(LogFormat.V2), typeof(string))]
	
	[TestFixture(typeof(LogFormat.V3), typeof(uint))]
	public class when_restarting_one_node_at_a_time<TLogFormat, TStreamId> : specification_with_cluster<TLogFormat, TStreamId> {
		protected override async Task Given() {
			await base.Given();
			
			for (int i = 0; i < 9; i++) {
				try {
					Log.Debug($"### Stopping node {i % 3} i={i}...");
					await _nodes[i % 3].Shutdown(keepDb: true);
				} catch (Exception) {
					Log.Debug($"### Failed to stop node {i % 3} i={i}...");
					throw;
				}
				await Task.Delay(2000);
				
				var node = CreateNode(i % 3, _nodeEndpoints[i % 3],
					new[] {_nodeEndpoints[(i+1)%3].HttpEndPoint, _nodeEndpoints[(i+2)%3].HttpEndPoint});
				
				try {
					Log.Debug($"### Starting node {i % 3} i={i}...");
					node.Start();
				} catch (Exception) {
					Log.Debug($"### Failed to start node {i % 3} i={i}...");
					throw;
				}

				_nodes[i % 3] = node;

				Log.Debug($"### Waiting for all nodes to be started i={i}...");

				try {
					await Task.WhenAll(_nodes.Select(x => x.Started)).WithTimeout(TimeSpan.FromSeconds(30));
				} catch (System.TimeoutException) {
					Log.Debug($"### Waiting timed out!!!");
					for (int j=0; j < _nodes.Length; j++) {
						Log.Debug($"### Node {j} is in State: {_nodes[j].NodeState}, Current Node {i%3}, i={i}");
					}
					throw;
				}
			}
		}

		[Test]
		public void cluster_should_stabilize() {
			var leaders = 0;
			var followers = 0;
			var acceptedStates = new[] {VNodeState.Leader, VNodeState.Follower};

			for (int i = 0; i < 3; i++) {
				AssertEx.IsOrBecomesTrue(() => acceptedStates.Contains(_nodes[i].NodeState),
					TimeSpan.FromSeconds(5), $"node {i} failed to become a leader/follower");

				var state = _nodes[i].NodeState;
				if (state == VNodeState.Leader) leaders++;
				else if (state == VNodeState.Follower) followers++;
				else throw new Exception($"node {i} in unexpected state {state}");
			}

			Assert.AreEqual(1, leaders);
			Assert.AreEqual(2, followers);
		}
	}
}
