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
            queue.ReceiveNext("client", "channel", receivedMessages.AddRange);
            queue.Send("channel", new {Something = 1});
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void DoesNotReceiveMessageOnOtherChannel()
        {
            var queue = new Queue();
            queue.ReceiveNext("client", "channel1", receivedMessages.AddRange);
            queue.Send("channel2", new {Something = 1});
            receivedMessages.Count.ShouldBe(0);
        }

        [Fact]
        public void ReceivesOnlyOneNewMessagePerCallToRecieve()
        {
            var queue = new Queue();
            queue.ReceiveNext("client", "channel", objects => receivedMessages.AddRange(objects));
            queue.Send("channel", new {Something = 1});
            queue.Send("channel", new {Something = 2});
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void MultipleOldMessagesAreReceivedAtOnce()
        {
            var queue = new Queue();
            queue.Send("channel", new { Something = 1 });
            queue.Send("channel", new { Something = 2 });
            queue.ReceiveNext("client", "channel", x => x.Count().ShouldBe(2));
        }

        [Fact]
        public void ReceivesUnreceivedMessagesEvenAfterTheyAreSent()
        {
            var queue = new Queue();
            queue.Send("channel", new {Something = 1});
            queue.ReceiveNext("client", "channel", receivedMessages.AddRange);
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void TwoClientsCanReceiveSameMessage()
        {
            var queue = new Queue();
            queue.Send("channel", new {Something = 1});
            queue.ReceiveNext("client1", "channel", receivedMessages.AddRange);
            queue.ReceiveNext("client2", "channel", receivedMessages.AddRange);
            receivedMessages.Count.ShouldBe(2);
        }

        [Fact]
        public void SameClientOnlyReceivesMessageOncePerQueue()
        {
            var queue = new Queue();
            queue.Send("channel", new {Something = 1});
            queue.ReceiveNext("client1", "channel", receivedMessages.AddRange);
            queue.ReceiveNext("client1", "channel", receivedMessages.AddRange);
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void SameClientOnlyReceivesMessageOncePerQueueEvenUnderLoad()
        {
            var queue = new Queue();
            queue.Send("channel", new {Something = 1});
            Parallel.For(0, 10,
                         x => queue.ReceiveNext("client1", "channel",
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
            queue.Send("channel", new {Something = 1});
            Thread.Sleep(TimeSpan.FromSeconds(2));
            queue.ReceiveNext("client", "channel", receivedMessages.AddRange);
            receivedMessages.Count.ShouldBe(0);
        }

    }
}