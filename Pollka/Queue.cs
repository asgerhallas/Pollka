using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Pollka
{
    public class Queue
    {
        readonly ISubject<Client> clients;
        readonly ISubject<MessageWrapper> messages;

        public Queue() : this(TimeSpan.FromSeconds(30)) {}

        public Queue(TimeSpan messageTimeout)
        {
            messages = new ReplaySubject<MessageWrapper>(messageTimeout);
            clients = new Subject<Client>();
            Listen();
        }

        public void Listen()
        {
            clients.Subscribe(client =>
                              {
                                  messages
                                      .Where(message => message.Channels.Contains(client.Channel)
                                                        && !message.IsReceivedBy(client.Id))
                                      .Buffer(TimeSpan.FromSeconds(1))
                                      .Take(1)
                                      .Subscribe(x =>
                                                 {
                                                     client.Callback(x);
                                                     foreach (var messageWrapper in x)
                                                     {
                                                        messageWrapper.MarkAsReceivedFor(client.Id);
                                                     }
                                                 });
                              });
        }

        public void Send(string channel, object message)
        {
            messages.OnNext(new MessageWrapper(new List<string> {channel}, message));
        }

        public void ReceiveNext(string clientId, string channel, Action<IEnumerable<object>> callback)
        {
            clients.OnNext(new Client(clientId, channel, callback));
        }

        //public void ReceiveNext(string clientId, string channel, Action<IEnumerable<object>> callback)
        //{
        //    var subscription = Disposable.Empty;
        //    subscription = messages
        //        .Where(message => message.Channels.Contains(channel))
        //        
        //        .Subscribe(listOfMessages =>
        //                   {
        //                       callback(ListModule.Reverse(listOfMessages));
        //                       subscription.Dispose();
        //                   });
        //}

        class Client
        {
            public Client(string id, string channel, Action<IEnumerable<object>> callback)
            {
                Id = id;
                Channel = channel;
                Callback = callback;
            }

            public string Id { get; private set; }
            public string Channel { get; private set; }
            public Action<IEnumerable<object>> Callback { get; private set; }
        }

        class MessageWrapper
        {
            readonly object message;
            readonly List<string> receivers;

            public MessageWrapper(List<string> channels, object message)
            {
                this.message = message;
                receivers = new List<string>();
                Channels = channels;
            }

            public List<string> Channels { get; private set; }

            public bool TryDispatchTo(string clientId, Action<IEnumerable<object>> handler)
            {
                lock (receivers)
                {
                    if (IsReceivedBy(clientId)) return false;
                    //handler(message);
                    MarkAsReceivedFor(clientId);
                    return true;
                }
            }

            public void MarkAsReceivedFor(string clientId)
            {
                receivers.Add(clientId);
            }

            public bool IsReceivedBy(string clientId)
            {
                return receivers.Contains(clientId);
            }
        }
    }
}