using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Threading;
using Microsoft.Reactive.Testing;
using Shouldly;
using Xunit;

namespace Pollka.Tests
{
    public class QueueTests
    {
        readonly TimeSpan bufferTimeout;
        readonly TimeSpan messageTimeout;
        readonly List<Recorded<Notification<MessageWrapper>>> messages;
        readonly TimeSpan requestTimeout;
        readonly List<Recorded<Notification<Request>>> requests;
        readonly TestScheduler scheduler;
        List<Response> responses;

        public QueueTests()
        {
            scheduler = new TestScheduler();
            requests = new List<Recorded<Notification<Request>>>();
            messages = new List<Recorded<Notification<MessageWrapper>>>();

            messageTimeout = TimeSpan.FromSeconds(30);
            bufferTimeout = TimeSpan.FromMilliseconds(50);
            requestTimeout = TimeSpan.FromSeconds(30);
        }

        void Request(int milliseconds)
        {
            requests.Add(ReactiveTest.OnNext(TimeSpan.FromMilliseconds(milliseconds).Ticks,
                                             new Request("a", new List<string> {"b"}, x => { })));
        }

        void Message(int milliseconds)
        {
            messages.Add(ReactiveTest.OnNext(TimeSpan.FromMilliseconds(milliseconds).Ticks,
                                             new MessageWrapper(Guid.NewGuid(), "a", new {Hello = "World"})));
        }

        ITestableObserver<Response> Go()
        {
            var observableRequests = scheduler.CreateHotObservable(requests.ToArray());
            var observableMessages = scheduler.CreateHotObservable(messages.ToArray());

            var observer = scheduler.Start(
                () => new Queue(observableRequests,
                                observableMessages,
                                messageTimeout,
                                bufferTimeout,
                                requestTimeout)
                          .Listen(scheduler),
                TimeSpan.FromMilliseconds(300).Ticks);

            responses = observer.Messages.Select(x => x.Value.Value).ToList();

            return observer;
        }


        [Fact]
        public void Yes()
        {
            Request(100);
            Message(100);

            Go();

            responses.Count.ShouldBe(1);
            responses[0].Messages.Count.ShouldBe(1);
        }

        [Fact]
        public void BuffersMultipleMessageIntoOneResponse()
        {
            Request(100);
            Message(200);
            Message(225);

            Go();

            responses.Count.ShouldBe(1);
            responses[0].Messages[0].MessageId.ShouldBe("a");.Count.ShouldBe(2);
        }

        [Fact]
        public void SameRequestDoesNotReceiveMessagesAfterBufferTimeout()
        {
            Request(100);
            Message(200);
            Message(300);

            Go();

            responses.Count.ShouldBe(1);
            responses[0].Messages.Count.ShouldBe(1);
        }

        //[Fact]
        //public void OtherRequestDoesReceiveMessagesAfterFirstRequestsBufferTimeout()
        //{
        //    var queue = new Queue(requests, bufferTimeout: TimeSpan.FromMilliseconds(500));
        //    requests.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
        //    queue.NewMessage(Guid.NewGuid(), "channel", new {Something = 1});
        //    Thread.Sleep(1000);
        //    queue.NewMessage(Guid.NewGuid(), "channel", new {Something = 2});
        //    requests.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));

        //    Thread.Sleep(200);
        //    receivedMessages.Count.ShouldBe(2);
        //}

        //[Fact]
        //public void RequestWaitsForAtLeastOneMessage()
        //{
        //    var queue = new Queue(requests);
        //    requests.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));
        //    Thread.Sleep(500);
        //    queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
        //    Thread.Sleep(200);
        //    receivedMessages.Count.ShouldBe(1);
        //}

        //[Fact]
        //public void DoesNotReceiveMessageOnOtherChannel()
        //{
        //    var queue = new Queue(requests);
        //    requests.OnNext(new Request("client", new List<string> { "channel1" }, receivedMessages.AddRange));
        //    queue.NewMessage(Guid.NewGuid(), "channel2", new {Something = 1});
        //    receivedMessages.Count.ShouldBe(0);
        //}

        //[Fact]
        //public void ReceivesLatentMessages()
        //{
        //    var queue = new Queue(requests);
        //    queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
        //    requests.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));

        //    Thread.Sleep(200);
        //    receivedMessages.Count.ShouldBe(1);
        //}

        //[Fact]
        //public void MultipleLatentMessagesAreReceivedAtOnce()
        //{
        //    var queue = new Queue(requests);
        //    queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
        //    queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 2 });
        //    requests.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));

        //    Thread.Sleep(200);
        //    receivedMessages.Count.ShouldBe(2);
        //}

        //[Fact]
        //public void TwoClientsCanReceiveSameMessage()
        //{
        //    var queue = new Queue(requests);
        //    queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
        //    requests.OnNext(new Request("client1", new List<string> { "channel" }, receivedMessages.AddRange));
        //    requests.OnNext(new Request("client2", new List<string> { "channel" }, receivedMessages.AddRange));

        //    Thread.Sleep(200);
        //    receivedMessages.Count.ShouldBe(2);
        //}

        //[Fact]
        //public void SameClientOnlyReceivesMessageOncePerQueue()
        //{
        //    var queue = new Queue(requests);
        //    queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
        //    requests.OnNext(new Request("client1", new List<string> { "channel" }, receivedMessages.AddRange));
        //    Thread.Sleep(50);
        //    requests.OnNext(new Request("client1", new List<string> { "channel" }, receivedMessages.AddRange));

        //    Thread.Sleep(200);
        //    receivedMessages.Count.ShouldBe(1);
        //}

        //[Fact]
        //public void SameClientOnlyReceivesMessageOncePerQueueEvenUnderLoad()
        //{
        //    var queue = new Queue(requests);
        //    queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
        //    Parallel.For(0, 10,
        //                 x => requests.OnNext(new Request("client1", new List<string> { "channel" },
        //                                        messages =>
        //                                        {
        //                                            receivedMessages.AddRange(messages);

        //                                            // wait a little, so another receive is initiated before
        //                                            // this message is marked as received
        //                                            Thread.Sleep(500);
        //                                        })));

        //    Thread.Sleep(200);
        //    receivedMessages.Count.ShouldBe(1);
        //}

        //[Fact]
        //public void MessageIsLostAfterGivenTime()
        //{
        //    var queue = new Queue(requests, messageTimeout: TimeSpan.FromSeconds(1));
        //    queue.NewMessage(Guid.NewGuid(), "channel", new { Something = 1 });
        //    Thread.Sleep(TimeSpan.FromSeconds(2));
        //    requests.OnNext(new Request("client", new List<string> { "channel" }, receivedMessages.AddRange));

        //    Thread.Sleep(200);
        //    receivedMessages.Count.ShouldBe(0);
        //}
    }
}