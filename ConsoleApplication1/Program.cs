using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using Shouldly;
using Xunit;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {
            var incomingMessages = Observable.Interval(TimeSpan.FromSeconds(3));
                //Observable.Create<long>(observer =>
                //    {
                //        observer.OnNext(0);
                //        return Disposable.Empty;
                //    });

            var messages2 = incomingMessages.Replay(1).RefCount();
    
            var requests = Observable.Interval(TimeSpan.FromSeconds(1));

            var events = requests.GroupJoin(messages2, 
                                            request =>
                                                {
                                                    Console.WriteLine("Begin request " + request);
                                                    return messages2.Delay(TimeSpan.FromSeconds(0.5)).Do(x => Console.WriteLine("End request " + request));
                                                },
                                            message =>
                                                {
                                                    Console.WriteLine("Begin message " + message);
                                                    return Observable.Timer(TimeSpan.FromSeconds(5));
                                                },
                                            (request, groupedMessages) =>
                                            groupedMessages.ToList().Select(x => new {message = x, request = request}));


            events.Merge().Subscribe(x =>
                {
                    Console.Write(x.request + ": ");
                    foreach (var m in x.message)
                    {
                        Console.Write(m);
                    }
                    Console.WriteLine();
                    
                    //x.request.Callback(t);
                });


            Console.ReadLine();
        }
    }

    public class TestListener : IDisposable
    {
        Subject<int> requests = new Subject<int>();
        Subject<int> messages = new Subject<int>();
        private string results = "";
        private IDisposable disposable;

        public TestListener()
        {
            disposable = ListenForAWhile();
        }

        public IDisposable ListenForAWhile()
        {
            var messageTimeout = TimeSpan.FromSeconds(5);
            var bufferTimeout = TimeSpan.FromMilliseconds(100);
            var everyMessageAndAShadow = messages.Replay(1, messageTimeout).RefCount();
            var events = (from request in requests
                    join message in everyMessageAndAShadow
                        on everyMessageAndAShadow.Delay(bufferTimeout)
                        equals Observable.Timer(messageTimeout)
                        into groupedMessages
                    select groupedMessages
                        .ToList()
                        .Select(x => new
                        {
                            Request = request,
                            Messages = x.ToList()
                        }))
                .Merge();

            return events.Subscribe(x =>
                {
                    if (results != "")
                        results += ",";

                    results += (x.Request + ":" + string.Join("", x.Messages));
                });
        }

        [Fact]
        public void ReceivesMessage()
        {
            requests.OnNext(1);
            messages.OnNext(1);
            
            Thread.Sleep(200);
            results.ShouldBe("1:1");
        }

        [Fact]
        public void GroupsMultipleMessageIntoOneResponseWithinThe50msWindow()
        {
            requests.OnNext(1);
            messages.OnNext(1);
            messages.OnNext(2);

            Thread.Sleep(200);
            results.ShouldBe("1:12");
        }

        [Fact]
        public void DoesNotGroupMessagesAfter50msWindowHasClosed()
        {
            requests.OnNext(1);
            messages.OnNext(1);
            Thread.Sleep(200);
            messages.OnNext(2);

            Thread.Sleep(200);
            results.ShouldBe("1:1");
        }

        [Fact]
        public void RequestWaitsForAtLeastOneMessage()
        {
            requests.OnNext(1);
            Thread.Sleep(500);
            messages.OnNext(1);

            Thread.Sleep(200);
            results.ShouldBe("1:1");
        }

        [Fact]
        public void ReceivesLatentMessages()
        {
            messages.OnNext(1);
            requests.OnNext(1);

            Thread.Sleep(200);
            results.ShouldBe("1:1");
        }

        [Fact]
        public void MultipleLatentMessagesAreReceivedAtOnce()
        {
            messages.OnNext(1);
            messages.OnNext(2);
            requests.OnNext(1);

            Thread.Sleep(200);
            results.ShouldBe("1:12");
        }

        [Fact]
        public void TwoRequestsCanReceiveSameMessage()
        {
            requests.OnNext(1);
            requests.OnNext(2);
            messages.OnNext(1);

            Thread.Sleep(200);
            results.ShouldContain("1:1");
            results.ShouldContain("2:1");
        }


        [Fact]
        public void TwoRequestsCanReceiveSameLatentMessage()
        {
            messages.OnNext(1);
            requests.OnNext(1);
            requests.OnNext(2);

            Thread.Sleep(200);
            results.ShouldContain("1:1");
            results.ShouldContain("2:1");
        }       
        
        [Fact]
        public void MessageIsLostAfterAWhile()
        {
            messages.OnNext(1);
            Thread.Sleep(6000);
            requests.OnNext(1);

            Thread.Sleep(200);
            results.ShouldBe("");
        }

        public void Dispose()
        {
            disposable.Dispose();
        }
    }
}
