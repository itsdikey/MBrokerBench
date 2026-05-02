using MBrokerConsumer.Configuration;
using MBrokerConsumer.Models;
using MBrokerConsumer.Services;
using MBrokerConsumer.Services.Implementations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MBrokerConsumer;

class Program
{
    static async Task Main(string[] args)
    {
        var builder = new ConfigurationBuilder()
            .AddSnakeCaseEnvironmentVariables()
            .Build();

        var envConfig = builder.Get<ConsumerEnvConfig>() ?? new ConsumerEnvConfig();

        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
        });

        var logger = loggerFactory.CreateLogger<Program>();

        var rateLimiter = new TokenBucketLimiter(
            (int)envConfig.MaxRateLimit,
            TimeSpan.FromSeconds(1));

        var services = new ServiceCollection()
            .AddSingleton(logger)
            .AddSingleton(envConfig)
            .AddSingleton(rateLimiter)
            .AddSingleton<IMainProgram, MainProgram>()
            .BuildServiceProvider();

        var mainProgram = services.GetRequiredService<IMainProgram>();
        await mainProgram.Run();
    }
}
