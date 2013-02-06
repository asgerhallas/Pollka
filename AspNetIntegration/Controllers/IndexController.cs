using System.Collections.Generic;
using System.Web.Mvc;

namespace AspNetIntegration.Controllers
{
    public class IndexController : Controller
    {
        public ActionResult Index()
        {
            return View();
        }
    }
}