using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        readonly ISubject<Client> clients;
        readonly ReplaySubject<MessageWrapper> incomingMessages;
        readonly ConcurrentDictionary<string, long?> lastMessageSeenByClient;

        public Queue() : this(TimeSpan.FromSeconds(30))
        {
        }

        public Queue(TimeSpan messageTimeout)
        {
            lastMessageSeenByClient = new ConcurrentDictionary<string, long?>();
            incomingMessages = new ReplaySubject<MessageWrapper>(messageTimeout);
            clients = new Subject<Client>();
            Listen();
        }

        public void Listen()
        {
            // could we gate clients
            clients.Subscribe(client =>
            {
                long? lastSeen;
                lastMessageSeenByClient.TryGetValue(client.Id, out lastSeen);
                incomingMessages
                    .Where(x => lastSeen == null || x.SequentialId > lastSeen)
                    .Where(message => client.Channels.Contains(message.Channel))
                    .Timeout(TimeSpan.FromSeconds(30))
                    .Buffer(TimeSpan.FromMilliseconds(50))
                    .Where(x => x.Count > 0)
                    .Take(1)
                    .Subscribe(messages =>
                    {
                        var messageAggregation = messages.Aggregate(
                            new { Messages = FSharpList<object>.Empty, SequentialId = 0L },
                            (acc, message) => new
                            {
                                Messages = FSharpList<object>.Cons(new
                                {
                                    id = message.MessageId,
                                    channel = message.Channel,
                                    type = message.Message.GetType().Name,
                                    message = message.Message
                                }, acc.Messages),
                                SequentialId = Math.Max(acc.SequentialId, message.SequentialId)
                            });

                        client.Callback(messageAggregation.Messages);
                        lastMessageSeenByClient[client.Id] = messageAggregation.SequentialId;
                    },
                    ex => client.Callback(Enumerable.Empty<object>()));
            });
        }

        public void Send(Guid messageId, string channel, object message)
        {
            incomingMessages.OnNext(new MessageWrapper(Interlocked.Increment(ref messageCounter), messageId, channel, message));
        }

        public void ReceiveNext(string clientId, List<string> channels, Action<IEnumerable<object>> callback)
        {
            clients.OnNext(new Client(clientId, channels, callback));
        }

        class Client
        {
            public Client(string id, List<string> channels, Action<IEnumerable<object>> callback)
            {
                Id = id;
                Channels = channels;
                Callback = callback;
            }

            public string Id { get; private set; }
            public List<string> Channels { get; private set; }
            public Action<IEnumerable<object>> Callback { get; private set; }
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
}
