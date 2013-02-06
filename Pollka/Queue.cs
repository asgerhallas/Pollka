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
        readonly TimeSpan bufferTimeout;
        readonly TimeSpan messageTimeout;
        readonly IObservable<MessageWrapper> messages;
        readonly TimeSpan requestTimeout;
        readonly IObservable<Request> requests;

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
            var test = from request in requests
                       group request by request.ClientId
                       into clients
                       join message in messages
                           on (from _ in Observable.Never<Unit>() select Unit.Default)
                           equals (from _ in Observable.Timer(messageTimeout, scheduler) select Unit.Default)
                           into messagesForClient
                       let messagesPerChannel = messagesForClient.GroupBy(x => x.Channel)
                       let everyMessageAndHisShadow = messagesForClient.Publish().RefCount()
                       let buffer = everyMessageAndHisShadow.Buffer(() => 
                           Observable.Amb(
                               from _ in everyMessageAndHisShadow.Delay(bufferTimeout, scheduler) select Unit.Default,
                               from _ in Observable.Timer(requestTimeout, scheduler) select Unit.Default))
                       select Observable.Zip(buffer, clients, (list, request) => new Response
                       {
                           Messages = list.Where(x => request.Channels.Contains(x.Channel)).ToList(),
                           Request = request
                       });

            return test.Merge();
        }
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
        public Request(string clientId, List<string> channels)
        {
            ClientId = clientId;
            Channels = channels;
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