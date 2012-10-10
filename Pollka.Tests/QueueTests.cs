using System;
using System.Collections.Generic;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using Shouldly;
using Xunit;

namespace Pollka.Tests
{
    public class QueueTests
    {
        readonly List<object> receivedMessages;
        readonly Subject<Request> clients;

        public QueueTests()
        {
            receivedMessages = new List<object>();
            clients = new Subject<Request>();
        }

        [Fact]
        public void ReceivesSubscribedMessage()
        {
            var queue = new Queue(clients);
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            queue.Send(Guid.NewGuid(), "channel", new {Something = 1});
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void DoesNotReceiveMessageOnOtherChannel()
        {
            var queue = new Queue(clients);
            clients.OnNext(new Request("client", new List<string> { "channel1" }, receivedMessages.AddRange));
            queue.Send(Guid.NewGuid(), "channel2", new {Something = 1});
            receivedMessages.Count.ShouldBe(0);
        }

        [Fact]
        public void MultipleOldMessagesAreReceivedAtOnce()
        {
            var queue = new Queue(clients);
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            queue.Send(Guid.NewGuid(), "channel", new { Something = 2 });
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(2);
        }

        [Fact]
        public void ReceivesUnreceivedMessagesEvenAfterTheyAreSent()
        {
            var queue = new Queue(clients);
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void TwoClientsCanReceiveSameMessage()
        {
            var queue = new Queue(clients);
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            clients.OnNext(new Request("client1", new List<string> { "channel" }, receivedMessages.AddRange));
            clients.OnNext(new Request("client2", new List<string> { "channel" }, receivedMessages.AddRange));
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(2);
        }

        [Fact]
        public void SameClientOnlyReceivesMessageOncePerQueue()
        {
            var queue = new Queue(clients);
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            clients.OnNext(new Request("client1", new List<string> { "channel" }, receivedMessages.AddRange));
            Thread.Sleep(50);
            clients.OnNext(new Request("client1", new List<string> { "channel" }, receivedMessages.AddRange));
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void SameClientOnlyReceivesMessageOncePerQueueEvenUnderLoad()
        {
            var queue = new Queue(clients);
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            Parallel.For(0, 10,
                         x => clients.OnNext(new Request("client1", new List<string> { "channel" },
                                                messages =>
                                                {
                                                    receivedMessages.AddRange(messages);

                                                    // wait a little, so another receive is initiated before
                                                    // this message is marked as received
                                                    Thread.Sleep(500);
                                                })));

            Thread.Sleep(200);

            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void MessageIsLostAfterGivenTime()
        {
            var queue = new Queue(clients, TimeSpan.FromSeconds(1));
            queue.Send(Guid.NewGuid(), "channel", new { Something = 1 });
            Thread.Sleep(TimeSpan.FromSeconds(2));
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(0);
        }
    }
}