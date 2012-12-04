using System;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {
            var incomingMessages = //Observable.Timer(TimeSpan.FromSeconds(0)).Publish().RefCount();
            Observable.Create<long>(observer =>
                {
                    observer.OnNext(0);
                    return Disposable.Empty;
                }).Publish().RefCount();

            var messages2 = new ReplaySubject<long>(1);
            incomingMessages.Do(x => messages2.OnNext(x));
    
            var requests = Observable.Interval(TimeSpan.FromSeconds(3));

            var events = requests.GroupJoin(incomingMessages.Do(x => Console.WriteLine("incoming message 1")), 
                                            request =>
                                                {
                                                    Console.WriteLine("Request " + request);
                                                    return messages2.Delay(TimeSpan.FromSeconds(0.5)).Do(x => Console.WriteLine("incoming message 2"));
                                                },
                                            message =>
                                                {
                                                    Console.WriteLine("Message " + message);
                                                    return Observable.Timer(TimeSpan.FromSeconds(60));
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
}
