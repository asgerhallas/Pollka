using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using Microsoft.Reactive.Testing;
using Xunit;
using Shouldly;

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
        int messageNumber = 1;
        ITestableObserver<Response> observer;

        public QueueTests()
        {
            scheduler = new TestScheduler();
            requests = new List<Recorded<Notification<Request>>>();
            messages = new List<Recorded<Notification<MessageWrapper>>>();

            messageTimeout = TimeSpan.FromSeconds(30);
            bufferTimeout = TimeSpan.FromMilliseconds(50);
            requestTimeout = TimeSpan.FromSeconds(30);
        }

        [Fact]
        public void ReceivesMessage()
        {
            Request(100);
            Message(100);

            Go();

            Response(1).ShouldHaveMessage(1);
        }

        [Fact]
        public void ReceivesLatentMessages()
        {
            Message(100);
            Request(500);

            Go();

            Response(1).ShouldHaveMessage(1);
        }

        [Fact]
        public void MultipleLatentMessagesAreReceivedAtOnce()
        {
            Message(100);
            Message(200);
            Request(300);

            Go();

            Response(1).ShouldHaveMessage(1, 2);
        }

        [Fact]
        public void RequestWaitsForAtLeastOneMessage()
        {
            Request(100);
            Message(1000);

            Go();

            Response(1).ShouldHaveMessage(1);
        }

        [Fact]
        public void BuffersMultipleMessageIntoOneResponse()
        {
            Request(100);
            Message(200);
            Message(225);

            Go();

            Response(1).ShouldHaveMessage(1, 2);
        }

        [Fact]
        public void ARequestDoesNotReceiveMessagesAfterBufferTimeout()
        {
            Request(100);
            Message(200);
            Message(300);

            Go();

            Response(1).ShouldHaveMessage(1);
        }

        [Fact]
        public void OtherRequestFromSameClientDoesReceiveMessagesAfterFirstRequestsBufferTimeout()
        {
            Request(100);
            Message(200);
            Message(300);
            Request(400);

            Go();

            Response(1).ShouldHaveMessage(1);
            Response(2).ShouldHaveMessage(2);
        }

        [Fact]
        public void MessageIsLostAfterGivenTime()
        {
            Message(100);
            Request((int)messageTimeout.TotalMilliseconds + 200);

            Go();

            Response(1).ShouldHaveNoMessages();
        }

        [Fact]
        public void TwoClientsCanReceiveSameMessage()
        {
            Request(100, "a");
            Message(200);
            Request(300, "b");

            Go();

            Response(1).ShouldHaveMessage(1);
            Response(2).ShouldHaveMessage(1);
        }

        [Fact]
        public void OneClientOnlyReceivesMessageOnce()
        {
            Request(100, "a");
            Message(200);
            Request(300, "a");

            Go();

            Response(1).ShouldHaveMessage(1);
            //Responses().Count.ShouldBe(1);
        }

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
        //public void DoesNotReceiveMessageOnOtherChannel()
        //{
        //    var queue = new Queue(requests);
        //    requests.OnNext(new Request("client", new List<string> { "channel1" }, receivedMessages.AddRange));
        //    queue.NewMessage(Guid.NewGuid(), "channel2", new {Something = 1});
        //    receivedMessages.Count.ShouldBe(0);
        //}

        void Request(int milliseconds, string clientId = "a")
        {
            requests.Add(ReactiveTest.OnNext(TimeSpan.FromMilliseconds(milliseconds).Ticks,
                                             new Request(clientId, new List<string> {"b"}, x => { })));
        }

        void Message(int milliseconds)
        {
            messages.Add(ReactiveTest.OnNext(TimeSpan.FromMilliseconds(milliseconds).Ticks,
                                             new MessageWrapper(Guid.NewGuid(), "a", messageNumber++)));
        }

        void Go(long millisecondsToRun = 0)
        {
            var observableRequests = scheduler.CreateHotObservable(requests.ToArray());
            var observableMessages = scheduler.CreateHotObservable(messages.ToArray());

            var ticksToRun = millisecondsToRun > 0
                                 ? TimeSpan.FromMilliseconds(millisecondsToRun).Ticks
                                 : Math.Max(requests.Max(x => x.Time), messages.Max(x => x.Time))
                                   + bufferTimeout.Ticks // The request needs its timeout before reponding
                                   + 100000; // And to give some slack

            observer = scheduler.Start(
                () => new Queue(observableRequests,
                                observableMessages,
                                messageTimeout,
                                bufferTimeout,
                                requestTimeout)
                          .Listen(scheduler),
                ticksToRun);
        }

        Response Response(int number)
        {
            return Responses()[number - 1];
        }

        List<Response> Responses()
        {
            if (observer == null)
                throw new Exception("You forgot to say GO!");

            return observer.Messages.Select(x => x.Value.Value).ToList();
        }
    }
}