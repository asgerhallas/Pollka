using System;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Web.Mvc;
using Pollka;

namespace AspNetIntegration.Controllers
{
    public class PollkaController : AsyncController
    {
        public static Queue queue;
        static readonly IObservable<Response> responses;
        static readonly Subject<Request> requests;
        static readonly Subject<MessageWrapper> messages;

        static PollkaController()
        {
            requests = new Subject<Request>();
            messages = new Subject<MessageWrapper>();
            queue = new Queue(requests, messages);
            responses = queue.Listen(Scheduler.Default);
        }

        public void WaitForAsync(string client, string channel)
        {
            AsyncManager.OutstandingOperations.Increment();

            var request = new Request(client, new[] { channel }.ToList());
            responses.Where(x => x.Request == request)
                     .Subscribe(message =>
                     {
                         AsyncManager.Parameters.Add("Message", message);
                         AsyncManager.OutstandingOperations.Decrement();
                     });

            requests.OnNext(request);
        }

        public ActionResult WaitForCompleted(object message)
        {
            return Json(message);
        }

        public ActionResult Message()
        {
            messages.OnNext(new MessageWrapper(Guid.NewGuid(), "a", "HEJ"));
            return new EmptyResult();
        }
    }
}