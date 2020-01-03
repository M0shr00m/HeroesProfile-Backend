using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
                    GrabHotsAPIData data = new GrabHotsAPIData();

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
