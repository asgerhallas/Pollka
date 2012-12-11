using System;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using Shouldly;
using Xunit;

namespace ConsoleApplication1
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var messages = Observable.Interval(TimeSpan.FromSeconds(1));
            //Observable.Create<long>(observer =>
            //    {
            //        observer.OnNext(0);
            //        return Disposable.Empty;
            //    });

            var requests = Observable.Interval(TimeSpan.FromSeconds(3)).Select(x => new {Client = x%2, Request = x});
            //Observable.Create<string>(observer =>
            //    {
            //        observer.OnNext("a");
            //        observer.OnNext("b");
            //        observer.OnNext("a");

            //        return Disposable.Empty;
            //    });

            var messageTimeout = TimeSpan.FromSeconds(5);
            var bufferTimeout = TimeSpan.FromMilliseconds(100);

            var clients = requests.GroupBy(x => x.Client);

            var messagesPerClient =
                (clients.GroupJoin(messages,
                                   client => client,
                                   message => Observable.Timer(messageTimeout),
                                   (client, groupedmessages) => groupedmessages.ToList().Select(y => new { client, groupedmessages = y })));

            messagesPerClient.Merge().Subscribe(x => Console.WriteLine(string.Join(",", x.groupedmessages)));

            //var requestsPerClient = messagesPerClient.Select(x =>
            //    {
            //        var r = x.Item1;
            //        var m = x.Item2.Replay(1).RefCount();
            //        var events = (from request in r
            //                      join message in m
            //                          on m.Delay(bufferTimeout)
            //                          equals Observable.Timer(messageTimeout)
            //                          into groupedMessages
            //                      select groupedMessages
            //                          .ToList()
            //                          .Select(y => new
            //                              {
            //                                  Request = request,
            //                                  Messages = y.ToList()
            //                              }))
            //            .Merge();
            //        return events;
            //    });

            //requestsPerClient.Merge().Subscribe(obj =>
            //    {
            //        Console.WriteLine(obj.Request + ":" + string.Join(",", obj.Messages));
            //    });

            //messagesPerClient.Subscribe(x =>
            //    {
            //        Console.WriteLine(x.Item1.Key + ":" + x.Item2);
            //        x.Item1.Subscribe(y => Console.WriteLine(y));
            //    });


            //var everyMessageAndAShadow = messages.Replay(1, messageTimeout).RefCount();
            //var events = (from request in clients
            //              join message in everyMessageAndAShadow
            //                  on everyMessageAndAShadow.Delay(bufferTimeout)
            //                  equals Observable.Timer(messageTimeout)
            //                  into groupedMessages
            //              select groupedMessages
            //                  .ToList()
            //                  .Select(x => new
            //                      {
            //                          Request = request,
            //                          Messages = x.ToList()
            //                      }))
            //    .Merge();


            //events.Subscribe(x =>
            //    {
            //        Console.Write(x.Request.Key + ": ");
            //        foreach (var m in x.Messages)
            //        {
            //            Console.Write(m);
            //        }
            //        Console.WriteLine();
            //    });


            Console.ReadLine();
        }
    }

    public class TestListener : IDisposable
    {
        readonly IDisposable disposable;
        readonly Subject<int> messages = new Subject<int>();
        readonly Subject<int> requests = new Subject<int>();
        string results = "";

        public TestListener()
        {
            disposable = ListenForAWhile();
        }

        public void Dispose()
        {
            disposable.Dispose();
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
    }
}