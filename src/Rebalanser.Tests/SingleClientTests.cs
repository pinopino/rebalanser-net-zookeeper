using Rebalanser.Core;
using Rebalanser.ZooKeeper.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Rebalanser.ZooKeeper.Tests
{
    public class SingleClientTests : IDisposable
    {
        private ZkHelper zkHelper;

        public SingleClientTests()
        {
            this.zkHelper = new ZkHelper();
        }

        [Fact]
        public async Task ResourceBarrier_GivenSingleClient_ThenGetsAllResourcesAssigned()
        {
            await GivenSingleClient_ThenGetsAllResourcesAssigned();
        }

        private async Task GivenSingleClient_ThenGetsAllResourcesAssigned()
        {
            // ARRANGE
            var groupName = Guid.NewGuid().ToString();
            await this.zkHelper.InitializeAsync("/rebalanser", TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30));
            await this.zkHelper.PrepareResourceGroupAsync(groupName, "res", 5);

            var expectedAssignedResources = new List<string>() { "res0", "res1", "res2", "res3", "res4" };

            // ACT
            var (client, testEvents) = CreateClient();
            await client.StartAsync(groupName, new ClientOptions() { AutoRecoveryOnError = false });

            await Task.Delay(TimeSpan.FromSeconds(10));

            // ASSERT
            Assert.Equal(1, testEvents.Count);
            Assert.Equal(EventType.Assignment, testEvents[0].EventType);
            Assert.True(ResourcesMatch(expectedAssignedResources, testEvents[0].Resources.ToList()));

            await client.StopAsync(TimeSpan.FromSeconds(30));
        }

        private (RebalanserClient, List<TestEvent> testEvents) CreateClient()
        {
            var client1 = new RebalanserClient(ZkHelper.ZooKeeperHosts,
                "/rebalanser",
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(5),
                new TestOutputLogger());
            var testEvents = new List<TestEvent>();
            client1.OnAssignment += (sender, args) =>
            {
                testEvents.Add(new TestEvent()
                {
                    EventType = EventType.Assignment,
                    Resources = args.Resources
                });
            };

            client1.OnUnassignment += (sender, args) =>
            {
                testEvents.Add(new TestEvent()
                {
                    EventType = EventType.Unassignment
                });
            };

            client1.OnAborted += (sender, args) =>
            {
                testEvents.Add(new TestEvent()
                {
                    EventType = EventType.Error
                });
                Console.WriteLine($"OnAborted: {args.Exception.ToString()}");
            };

            return (client1, testEvents);
        }

        private bool ResourcesMatch(List<string> expectedRes, List<string> actualRes)
        {
            return expectedRes.OrderBy(x => x).SequenceEqual(actualRes.OrderBy(x => x));
        }

        public void Dispose()
        {
            if (this.zkHelper != null)
                Task.Run(async () => await this.zkHelper.CloseAsync()).Wait();
        }
    }
}

