using System.Linq;
using Shouldly;

namespace Pollka.Tests
{
    public static class ReponseAssertion
    {
        public static void ShouldHaveMessage(this Response response, params int[] numbers)
        {
            response.Messages.Count.ShouldBe(numbers.Length);
            foreach (var number in numbers)
            {
                response.Messages.Select(x => x.Message).ShouldContain(number);
            }
        }

        public static void ShouldHaveNoMessages(this Response response)
        {
            response.Messages.Count.ShouldBe(0);
        }
    }
}