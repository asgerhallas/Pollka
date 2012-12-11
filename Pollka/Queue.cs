using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;

namespace Pollka
{
    public class Queue
    {
        private readonly TimeSpan bufferTimeout;
        private readonly TimeSpan messageTimeout;
        private readonly IObservable<MessageWrapper> messages;
        private readonly TimeSpan requestTimeout;
        private readonly IObservable<Request> requests;

        public Queue(IObservable<Request> requests,
                     IObservable<MessageWrapper> messages,
                     TimeSpan? messageTimeout = null,
                     TimeSpan? bufferTimeout = null,
                     TimeSpan? requestTimeout = null)
        {
            this.requests = requests;
            this.messages = messages;
            this.messageTimeout = messageTimeout ?? TimeSpan.FromSeconds(30);
            this.bufferTimeout = bufferTimeout ?? TimeSpan.FromMilliseconds(50);
            this.requestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30);
        }

        public IObservable<Response> Listen(IScheduler scheduler)
        {
            var clients = requests.GroupBy(x => x.ClientId).Do(x => Console.WriteLine("New client " + x.Key));

            var a = clients.GroupJoin(messages, 
                        Observable.Never,
                        x => Observable.Timer(messageTimeout, scheduler),
                        (requestsPerClient, messagegroup) => new {requestsPerClient, messagegroup });

            var b = a.SelectMany(x =>
                {
                    var everyMessageAndHisShadow = x.messagegroup.Replay(1, messageTimeout).RefCount();

                    return everyMessageAndHisShadow.Do(y => Console.WriteLine("Message for client " + x.requestsPerClient.Key))
                        .Window(
                        Observable.Amb(
                            x.requestsPerClient.Select(_ => Unit.Default), everyMessageAndHisShadow.Select(_ => Unit.Default)),
                                                    _ => everyMessageAndHisShadow.Delay(bufferTimeout, scheduler).Do(y => Console.WriteLine("Closing request")))
                                            .Select(y => y.ToList().Select(z => new Response
                                                {
                                                    Messages = z.ToList()
                                                })).Merge();


                    //var requestsPerClient = x.requestsPerClient.Replay(1).RefCount();
                    //return (from request in requestsPerClient.Do(y => Console.WriteLine("Request for client " + x.requestsPerClient.Key))
                    //        join message in everyMessageAndHisShadow.Do(y => Console.WriteLine("Message for client " + x.requestsPerClient.Key))
                    //            on Observable.Amb(
                    //                from _ in everyMessageAndHisShadow.Delay(bufferTimeout, scheduler) select Unit.Default,
                    //                from _ in Observable.Timer(requestTimeout, scheduler) select Unit.Default).Do(y => Console.WriteLine("Closing request"))
                    //            equals requestsPerClient.Do(y => Console.WriteLine("Closing message"))
                    //            into groupedMessages
                    //        select groupedMessages
                    //            .Do(y => Console.WriteLine("Group"), () => Console.WriteLine("Group complete"))    
                    //            .ToList()
                    //            .Select(y => new Response
                    //            {
                    //                Request = request,
                    //                Messages = y.ToList()
                    //            }))
                    //    .Merge().Do(y => Console.WriteLine("Merge"));
                });

            return b;
        }
    }

    public class Q
    {
    }

    public class Response
    {
        public Request Request { get; set; }
        public List<MessageWrapper> Messages { get; set; }
    }

    public class MessageWrapper
    {
        public MessageWrapper(Guid messageId, string channel, object message)
        {
            MessageId = messageId;
            Message = message;
            Channel = channel;
        }

        public Guid MessageId { get; private set; }
        public string Channel { get; private set; }
        public object Message { get; private set; }
    }

    public class Request
    {
        public Request(string clientId, List<string> channels, Action<IEnumerable<object>> callback)
        {
            ClientId = clientId;
            Channels = channels;
            Callback = callback;
        }

        public string ClientId { get; private set; }
        public List<string> Channels { get; private set; }
        public Action<IEnumerable<object>> Callback { get; private set; }

        public bool IsListeningTo(string channel)
        {
            return Channels.Contains(channel);
        }
    }
}