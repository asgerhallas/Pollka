using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;

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

        public Queue(IObservable<Request> requests, TimeSpan? messageTimeout = null, TimeSpan? bufferTimeout = null, TimeSpan? requestTimeout = null)
        {
            this.requests = requests;
            this.messageTimeout = messageTimeout ?? TimeSpan.FromSeconds(30);
            this.bufferTimeout = bufferTimeout ?? TimeSpan.FromMilliseconds(50);
            this.requestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30);
            incomingMessages = new Subject<MessageWrapper>();
            Listen();
        }

        public void Listen()
        {
            var events = from request in requests
                         join message in incomingMessages
                         on incomingMessages.Delay(bufferTimeout)
                         equals Observable.Timer(messageTimeout)
                         into groupedMessages
                         let listOfGroupedMessages = groupedMessages.ToList()
                         select groupedMessages.ToList().Select(x => new { message = x, request = request });

            events.Merge().Subscribe(x => 
            {
                var t = x.message.Select(y => 
                    new { 
                        id = y.MessageId,
                        channel = y.Channel,
                        type = y.GetType().Name,
                        message = y.Message
                    });
                x.request.Callback(t);
            });

            //requests.Join(incomingMessages, 
            //        _ => Observable.Never<Unit>(),
            //        _ => Observable.Timer(messageTimeout).Amb(Observable.Timer(bufferTimeout)),
            //        (request, message) => new { request, message })
            //    .Where(@event => @event.request.IsListeningTo(@event.message.Channel))
            //    .Distinct(x => string.Format("{0}/{1}", x.request.ClientId, x.message.MessageId))
            //    .Subscribe(@event =>
            //    {
            //        @event.request.Callback(new[]
            //        {
            //            new
            //            {
            //                id = @event.message.MessageId,
            //                channel = @event.message.Channel,
            //                type = @event.message.Message.GetType().Name,
            //                message = @event.message.Message
            //            }
            //        });
            //    });


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
