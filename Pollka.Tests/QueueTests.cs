using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
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
        public void ReceivesMessageOnlyWhenRequestIsPresent()
        {
            Request(100);
            Message(200);
            Message(300);

            Go();

            Responses().Count.ShouldBe(1);
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
        public void BuffersMultipleLatentMessageIntoOneResponse()
        {
            Message(100);
            Message(200);
            Message(300);
            Request(400);

            Go();

            Response(1).ShouldHaveMessage(1, 2, 3);
        }


        [Fact]
        public void ARequestDoesNotReceiveMessagesAfterBufferTimeout()
        {
            Request(100);
            Message(200);
            Message(201 + (int) bufferTimeout.TotalMilliseconds);

            Go();

            Response(1).ShouldHaveMessage(1);
        }

        [Fact]
        public void OtherRequestFromSameClientDoesReceiveMessagesAfterFirstRequestsBufferTimeout()
        {
            Request(100);
            Message(200);
            Message(201 + (int) bufferTimeout.TotalMilliseconds);
            Request(400);

            Go();

            Response(1).ShouldHaveMessage(1);
            Response(2).ShouldHaveMessage(2);
        }

        [Fact]
        public void MessageIsLostAfterGivenTime()
        {
            Message(100);
            Request(101 + (int) messageTimeout.TotalMilliseconds);

            Go((int) messageTimeout.TotalMilliseconds
               + (int) requestTimeout.TotalMilliseconds
               + 102);

            Response(1).ShouldHaveNoMessages();
        }

        [Fact]
        public void RequestTimesOutAfterGivenTime()
        {
            Request(100);

            Go(101 + (int) requestTimeout.TotalMilliseconds);

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
            Responses().Count.ShouldBe(1);
        }

        [Fact]
        public void DoesNotReceiveMessageOnOtherChannel()
        {
            Request(100, channels: "a");
            Message(200, channel: "b");

            Go();

            Response(1).ShouldHaveNoMessages();
        }

        void Request(int milliseconds, string clientId = "a", params string[] channels)
        {
            requests.Add(ReactiveTest.OnNext(
                TimeSpan.FromMilliseconds(milliseconds).Ticks,
                new Request(clientId,
                            channels.Length > 0 ? channels.ToList() : new List<string> {"a"})));
        }

        void Message(int milliseconds, string channel = "a")
        {
            messages.Add(ReactiveTest.OnNext(
                TimeSpan.FromMilliseconds(milliseconds).Ticks,
                new MessageWrapper(Guid.NewGuid(), channel, messageNumber++)));
        }

        void Go(long millisecondsToRun = 0)
        {
            var observableRequests = scheduler.CreateHotObservable(requests.ToArray());
            var observableMessages = scheduler.CreateHotObservable(messages.ToArray());

            var ticksToRun = millisecondsToRun > 0
                                 ? TimeSpan.FromMilliseconds(millisecondsToRun).Ticks
                                 : Math.Max(requests.Max(x => (long?) x.Time) ?? 0, messages.Max(x => (long?) x.Time) ?? 0)
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