using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using Microsoft.FSharp.Collections;
using System.Linq;

namespace Pollka
{
    public class Queue
    {
        long messageCounter;
        readonly IObservable<Request> requests;
        readonly TimeSpan messageTimeout;
        readonly TimeSpan bufferTimeout;
        readonly TimeSpan requestTimeout;
        readonly Subject<MessageWrapper> incomingMessages;
        readonly ConcurrentDictionary<string, long?> lastMessageSeenByClient;

        public Queue(IObservable<Request> requests, TimeSpan? messageTimeout = null, TimeSpan? bufferTimeout = null, TimeSpan? requestTimeout = null)
        {
            this.requests = requests;
            this.messageTimeout = messageTimeout ?? TimeSpan.FromSeconds(30);
            this.bufferTimeout = bufferTimeout ?? TimeSpan.FromMilliseconds(50);
            this.requestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30);
            lastMessageSeenByClient = new ConcurrentDictionary<string, long?>();
            incomingMessages = new Subject<MessageWrapper>();
            Listen();
        }

        public void Listen()
        {
            incomingMessages.Join(requests,
                                  _ => Observable.Timer(messageTimeout),
                                  _ => Observable.Create<long>(o =>
                                  {
                                      return Observable.Timer(bufferTimeout).Subscribe(o);
                                  }),
                                  (message, request) => new {message, request})
                .Where(@event => @event.request.IsListeningTo(@event.message.Channel))
                .Distinct(x => string.Format("{0}/{1}", x.request.ClientId, x.message.MessageId))
                .Subscribe(@event =>
                {
                    @event.request.Callback(new[]
                    {
                        new
                        {
                            id = @event.message.MessageId,
                            channel = @event.message.Channel,
                            type = @event.message.Message.GetType().Name,
                            message = @event.message.Message
                        }
                    });
                });



            // could we gate clients
            //requests.Subscribe(request =>
            //{
            //    long? lastSeen;
            //    lastMessageSeenByClient.TryGetValue(request.ClientId, out lastSeen);
            //    incomingMessages
            //        .Where(x => lastSeen == null || x.SequentialId > lastSeen)
            //        .Where(message => request.Channels.Contains(message.Channel))
            //        .Timeout(TimeSpan.FromSeconds(30))
            //        .Buffer(TimeSpan.FromMilliseconds(50))
            //        .Where(x => x.Count > 0)
            //        .Take(1)
            //        .Subscribe(messages =>
            //        {
            //            var messageAggregation = messages.Aggregate(
            //                new { Messages = FSharpList<object>.Empty, SequentialId = 0L },
            //                (acc, message) => new
            //                {
            //                    Messages = FSharpList<object>.Cons(new
            //                    {
            //                        id = message.MessageId,
            //                        channel = message.Channel,
            //                        type = message.Message.GetType().Name,
            //                        message = message.Message
            //                    }, acc.Messages),
            //                    SequentialId = Math.Max(acc.SequentialId, message.SequentialId)
            //                });

            //            request.Callback(messageAggregation.Messages);
            //            lastMessageSeenByClient[request.ClientId] = messageAggregation.SequentialId;
            //        },
            //        ex => request.Callback(Enumerable.Empty<object>()));
            //});
        }

        public void NewMessage(Guid messageId, string channel, object message)
        {
            incomingMessages.OnNext(new MessageWrapper(Interlocked.Increment(ref messageCounter), messageId, channel, message));
        }

        class MessageWrapper
        {
            public MessageWrapper(long sequentialId, Guid messageId, string channel, object message)
            {
                SequentialId = sequentialId;
                MessageId = messageId;
                Message = message;
                Channel = channel;
            }

            public long SequentialId { get; private set; }
            public Guid MessageId { get; private set; }
            public string Channel { get; private set; }
            public object Message { get; private set; }
        }
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
