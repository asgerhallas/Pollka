using System.Collections.Generic;
using NUnit.Framework;
using Shouldly;

namespace Pollka.Tests
{
    public class QueueTests
    {
        [Test]
        public void ReceivesSubscribedMessage()
        {
            object receivedMessage = null;
            var queue = new Queue();
            queue.Receive("channel", message => receivedMessage = message);
            queue.Send("channel", new { Something = 1 });
            receivedMessage.ShouldNotBe(null);
        }

        [Test]
        public void ReceivesOnlyOneMessagePerCallToRecieve()
        {
            var receivedMessages = new List<object>();
            var queue = new Queue();
            queue.Receive("channel", receivedMessages.Add);
            queue.Send("channel", new { Something = 1 });
            queue.Send("channel", new { Something = 2 });
            receivedMessages.Count.ShouldBe(1);
        }

        [Test]
        public void DoesNotReceiveMessageOnOtherChannel()
        {
            object receivedMessage = null;
            var queue = new Queue();
            queue.Receive("channel1", message => receivedMessage = message);
            queue.Send("channel2", new { Something = 1 });
            receivedMessage.ShouldBe(null);
        }
    }
}
