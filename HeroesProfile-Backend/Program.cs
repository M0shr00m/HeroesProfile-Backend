using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace HeroesProfile_Backend
{
    class Program
    {
        static void Main(string[] args)
        {
            // Create service collection and configure our services
            var services = ConfigureServices();
            // Generate a provider
            var serviceProvider = services.BuildServiceProvider();
   
            // Kick off our actual code
            serviceProvider.GetService<ConsoleApp>().Run();
        }
        
        private static IServiceCollection ConfigureServices()
        {
            IServiceCollection services = new ServiceCollection();
            
            // Set up the objects we need to get to configuration settings
            var config = LoadConfiguration();
            // Add the config to our DI container for later user
            services.AddSingleton(config);
            
            // IMPORTANT! Register our application entry point
            services.AddTransient<ConsoleApp>();
            return services;
        }

        private static IConfiguration LoadConfiguration()
        {
            var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: true, 
                            reloadOnChange: true);
            return  builder.Build();
        }
    }
}