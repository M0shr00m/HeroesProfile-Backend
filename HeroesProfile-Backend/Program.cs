using System;

namespace HeroesProfile_Backend
{
    class Program
    {
        static void Main(string[] args)
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();

            while (true)
            {
                try
                {
                    var data = new GrabHotsApiData();

                }
                catch (Exception e)
                {
                }
                Console.WriteLine("Sleeping for 10 seconds");
                System.Threading.Thread.Sleep(10000);
            }
        }
    }
}