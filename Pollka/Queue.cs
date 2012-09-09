using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Pollka
{
    public class Queue
    {
        readonly ISubject<InternalMessage> messages = new Subject<InternalMessage>();
        
        public void Send(string channel, object message)
        {
            messages.OnNext(new InternalMessage(new List<string> { channel }, message));
        }

        public void Receive(string channel, Action<object> callback)
        {
            messages
                .Where(message => message.Channels.Contains(channel))
                .Subscribe(message => callback(message.Message));
        }

        class InternalMessage
        {
            public InternalMessage(List<string> channels, object message)
            {
                Channels = channels;
                Message = message;
            }

            public List<string> Channels { get; private set; }
            public object Message { get; private set; }
        }
    }
}
