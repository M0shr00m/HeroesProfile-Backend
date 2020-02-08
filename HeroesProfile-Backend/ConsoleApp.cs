using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace HeroesProfile_Backend
{
    public class ConsoleApp
    {
        private readonly IConfiguration _configuration;
        private readonly GrabHotsApiDataService _grabHotsApiDataService;

        public ConsoleApp(IConfiguration configuration, GrabHotsApiDataService grabHotsApiDataService)
        {
            _configuration = configuration;
            _grabHotsApiDataService = grabHotsApiDataService;
        }
        
        public async Task Run()
        {
            
            while (true)
            {
                try
                {
                    await _grabHotsApiDataService.GrabHotsApiData();

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