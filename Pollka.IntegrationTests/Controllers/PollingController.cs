using System.Web.Mvc;

namespace Pollka.IntegrationTests.Controllers
{
    public class PollingController : AsyncController
    {
        public static Queue q = new Queue();

        public void WaitForAsync(string client, string channel)
        {
            AsyncManager.OutstandingOperations.Increment();
            //q.ReceiveNext(client, channel, message =>
            //                               {
            //                                   AsyncManager.Parameters.Add("Message", message);
            //                                   AsyncManager.OutstandingOperations.Decrement();
            //                               });
        }

        public ActionResult WaitForCompleted(object message)
        {
            return Json(message);
        }
    }
}