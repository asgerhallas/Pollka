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
        public void ReceivesMessage()
        {
            var queue = new Queue(clients);
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            queue.NewMessage(Guid.NewGuid(), "channel", new {Something = 1});
            
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void BuffersMultipleMessageIntoOneResponse()
        {
            var queue = new Queue(clients);
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            queue.NewMessage(Guid.NewGuid(), "channel", new {Something = 1});
            queue.NewMessage(Guid.NewGuid(), "channel", new {Something = 2});
            
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(2);
        }

        [Fact]
        public void SameRequestDoesNotReceiveMessagesAfterBufferTimeout()
        {
            var queue = new Queue(clients, bufferTimeout: TimeSpan.FromMilliseconds(500));
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            queue.NewMessage(Guid.NewGuid(), "channel", new {Something = 1});
            Thread.Sleep(1000);
            queue.NewMessage(Guid.NewGuid(), "channel", new {Something = 2});
            
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void OtherRequestDoesReceiveMessagesAfterFirstRequestsBufferTimeout()
        {
            var queue = new Queue(clients, bufferTimeout: TimeSpan.FromMilliseconds(500));
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            queue.NewMessage(Guid.NewGuid(), "channel", new {Something = 1});
            Thread.Sleep(1000);
            queue.NewMessage(Guid.NewGuid(), "channel", new {Something = 2});
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(2);
        }

        [Fact]
        public void RequestWaitsForAtLeastOneMessage()
        {
            var queue = new Queue(clients);
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            Thread.Sleep(500);
            queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void DoesNotReceiveMessageOnOtherChannel()
        {
            var queue = new Queue(clients);
            clients.OnNext(new Request("client", new List<string> { "channel1" }, receivedMessages.AddRange));
            queue.NewMessage(Guid.NewGuid(), "channel2", new {Something = 1});
            receivedMessages.Count.ShouldBe(0);
        }

        [Fact]
        public void ReceivesLatentMessages()
        {
            var queue = new Queue(clients);
            queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(1);
        }

        [Fact]
        public void MultipleLatentMessagesAreReceivedAtOnce()
        {
            var queue = new Queue(clients);
            queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
            queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 2 });
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(2);
        }

        [Fact]
        public void TwoClientsCanReceiveSameMessage()
        {
            var queue = new Queue(clients);
            queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
            clients.OnNext(new Request("client1", new List<string> { "channel" }, receivedMessages.AddRange));
            clients.OnNext(new Request("client2", new List<string> { "channel" }, receivedMessages.AddRange));
            
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(2);
        }

        [Fact]
        public void SameClientOnlyReceivesMessageOncePerQueue()
        {
            var queue = new Queue(clients);
            queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
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
            queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
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
            var queue = new Queue(clients, messageTimeout: TimeSpan.FromSeconds(1));
            queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
            Thread.Sleep(TimeSpan.FromSeconds(2));
            clients.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
            
            Thread.Sleep(200);
            receivedMessages.Count.ShouldBe(0);
        }
    }
}