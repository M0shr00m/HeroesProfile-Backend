using System;
using System.IO;
using HeroesProfile_Backend.Models;
using HeroesProfileDb.HeroesProfile;
using HeroesProfileDb.HeroesProfileBrawl;
using HeroesProfileDb.HeroesProfileCache;
using Microsoft.EntityFrameworkCore;
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
            var apiSettings = config.GetSection("HotsApi").Get<ApiSettings>();

            // EF Db config
            services.AddDbContext<HeroesProfileContext>(options => options.UseMySql(config.GetConnectionString("HeroesProfile")));
            services.AddDbContext<HeroesProfileCacheContext>(options => options.UseMySql(config.GetConnectionString("HeroesProfileCache")));
            services.AddDbContext<HeroesProfileBrawlContext>(options => options.UseMySql(config.GetConnectionString("HeroesProfileBrawl")));
            // services.AddDbContext<HeroesProfileContext>(options => options.UseMySql(config.GetConnectionString("HeroesProfile")));

            // Add the config to our DI container for later use
            services.AddSingleton(config);
            services.AddSingleton(apiSettings);
            services.AddScoped<GrabHotsApiDataService>();
            services.AddScoped<ParseStormReplayService>();
            
            // IMPORTANT! Register our application entry point
            services.AddTransient<ConsoleApp>();
            return services;
        }

        private static IConfiguration LoadConfiguration()
        {
            var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: true, 
                            reloadOnChange: true)
                    .AddEnvironmentVariables();
            return  builder.Build();
        }
    }
}