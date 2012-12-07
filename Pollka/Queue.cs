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
            var everyMessageAndAShadow = messages.Replay(1, messageTimeout).RefCount();
            return (from request in requests
                    join message in everyMessageAndAShadow
                        on Observable.Amb(
                            from _ in everyMessageAndAShadow.Delay(bufferTimeout, scheduler) select Unit.Default,
                            from _ in Observable.Timer(requestTimeout, scheduler) select Unit.Default)
                        equals Observable.Timer(messageTimeout, scheduler)
                        into groupedMessages
                    select groupedMessages
                        .ToList()
                        .Select(x => new Response
                            {
                                Request = request,
                                Messages = x.ToList()
                            }))
                .Merge();
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