using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Shouldly;
using Xunit;

namespace Pollka.Tests
{
    public class QueueTests
    {
        readonly List<object> receivedMessages;

        public QueueTests()
        {
            receivedMessages = new List<object>();
        }

        [Fact]
        public void ReceivesSubscribedMessage()
        {
            var queue = new Queue();
            queue.ReceiveNext("client", new List<string> { "channel" }, receivedMessages.AddRange);
            queue.Send(Guid.NewGuid(), "channel", new {Something = 1});
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void DoesNotReceiveMessageOnOtherChannel()
        {
            var queue = new Queue();
            queue.ReceiveNext("client", new List<string> { "channel1" }, receivedMessages.AddRange);
            queue.Send(Guid.NewGuid(), "channel2", new {Something = 1});
            receivedMessages.Count.ShouldBe(0);
        }

        [Fact]
        public void ReceivesOnlyOneNewMessagePerCallToRecieve()
        {
            var queue = new Queue();
            queue.ReceiveNext("client", new List<string> { "channel" }, objects => receivedMessages.AddRange(objects));
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            queue.Send(Guid.NewGuid(), "channel", new { Something = 2 });
            Thread.Sleep(1000);
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void MultipleOldMessagesAreReceivedAtOnce()
        {
            var queue = new Queue();
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            queue.Send(Guid.NewGuid(), "channel", new { Something = 2 });
            queue.ReceiveNext("client", new List<string> { "channel" }, x => x.Count().ShouldBe(2));
        }

        [Fact]
        public void ReceivesUnreceivedMessagesEvenAfterTheyAreSent()
        {
            var queue = new Queue();
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            queue.ReceiveNext("client", new List<string> { "channel" }, receivedMessages.AddRange);
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void TwoClientsCanReceiveSameMessage()
        {
            var queue = new Queue();
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            queue.ReceiveNext("client1", new List<string> { "channel" }, receivedMessages.AddRange);
            queue.ReceiveNext("client2", new List<string> { "channel" }, receivedMessages.AddRange);
            receivedMessages.Count.ShouldBe(2);
        }

        [Fact]
        public void SameClientOnlyReceivesMessageOncePerQueue()
        {
            var queue = new Queue();
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            queue.ReceiveNext("client1", new List<string> { "channel" }, receivedMessages.AddRange);
            queue.ReceiveNext("client1", new List<string> { "channel" }, receivedMessages.AddRange);
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void SameClientOnlyReceivesMessageOncePerQueueEvenUnderLoad()
        {
            var queue = new Queue();
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            Parallel.For(0, 10,
                         x => queue.ReceiveNext("client1", new List<string> { "channel" },
                                                messages =>
                                                {
                                                    receivedMessages.AddRange(messages);

                                                    // wait a little, so another receive is initiated before
                                                    // this message is marked as received
                                                    Thread.Sleep(500);
                                                }));
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void MessageIsLostAfterGivenTime()
        {
            var queue = new Queue(TimeSpan.FromSeconds(1));
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            Thread.Sleep(TimeSpan.FromSeconds(2));
            queue.ReceiveNext("client", new List<string> { "channel" }, receivedMessages.AddRange);
            receivedMessages.Count.ShouldBe(0);
        }

    }
}