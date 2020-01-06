using System;
using Microsoft.Extensions.Configuration;

namespace HeroesProfile_Backend
{
    public class ConsoleApp
    {
        private readonly IConfiguration _configuration;

        public ConsoleApp(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        
        public void Run()
        {
            
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