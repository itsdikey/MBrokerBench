using Confluent.Kafka;
using MBrokerConsumer.Models;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace MBrokerConsumer.Services.Implementations;

internal sealed class MainProgram : IMainProgram
{
    private readonly ILogger<Program> _logger;
    private readonly ConsumerEnvConfig _envConfig;
    private readonly TokenBucketLimiter _rateLimiter;

    public MainProgram(
        ILogger<Program> logger,
        ConsumerEnvConfig envConfig,
        TokenBucketLimiter rateLimiter)
    {
        _logger = logger;
        _envConfig = envConfig;
        _rateLimiter = rateLimiter;
    }

    public async Task Run()
    {
        if (!ValidateConfig(_envConfig))
        {
            _logger.LogError("Invalid configuration. Please check environment variables.");
            return;
        }

        LogConfig(_envConfig);

        var config = _envConfig.ToConsumerConfig();

        using var consumer = new ConsumerBuilder<Ignore, byte[]>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka Error: {Reason}", e.Reason))
            .Build();

        consumer.Subscribe(_envConfig.Topic);

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        _logger.LogInformation("Consumption loop started.");

        var windowStopwatch = Stopwatch.StartNew();
        long windowCount = 0;
        long totalMessages = 0;

        try
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    await _rateLimiter.ConsumeAsync();

                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));

                    if (consumeResult != null)
                    {
                        windowCount++;
                        totalMessages++;

                        // Log rate every second
                        if (windowStopwatch.Elapsed.TotalSeconds >= 1.0)
                        {
                            _logger.LogInformation(
                                "Consumed {Count} msgs in {Elapsed:F2}s. Total: {Total}",
                                windowCount, windowStopwatch.Elapsed.TotalSeconds, totalMessages);
                            windowCount = 0;
                            windowStopwatch.Restart();
                        }
                    }
                }
                catch (ConsumeException e)
                {
                    _logger.LogError("Error occurred: {Reason}", e.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Closing consumer...");
        }
        finally
        {
            if (windowCount > 0)
            {
                _logger.LogInformation(
                    "Final batch: {Count} msgs in {Elapsed:F2}s. Total: {Total}",
                    windowCount, windowStopwatch.Elapsed.TotalSeconds, totalMessages);
            }
            consumer.Close();
        }
    }

    private void LogConfig(ConsumerEnvConfig envConfig)
    {
        _logger.Log(envConfig.GetLogLevel(), "MBrokerConsumer starting...");
        _logger.Log(envConfig.GetLogLevel(), "Bootstrap Servers: {Servers}", envConfig.BootstrapServers);
        _logger.Log(envConfig.GetLogLevel(), "Topic: {Topic}", envConfig.Topic);
        _logger.Log(envConfig.GetLogLevel(), "Group ID: {GroupId}", envConfig.GroupId);
        _logger.Log(envConfig.GetLogLevel(), "Max Rate Limit: {Rate} msgs/s", envConfig.MaxRateLimit);
        _logger.Log(envConfig.GetLogLevel(), "Consumer Profile: {Profile}", envConfig.ConsumerProfile);
        _logger.Log(envConfig.GetLogLevel(), "Log Level: {Level}", envConfig.LogLevel);
        _logger.Log(envConfig.GetLogLevel(), "Drain Timeout Seconds: {Seconds}", envConfig.DrainTimeoutSeconds);
        _logger.Log(envConfig.GetLogLevel(), "Commit Interval Seconds: {Seconds}", envConfig.CommitIntervalSeconds);
    }

    private bool ValidateConfig(ConsumerEnvConfig? envConfig)
    {
        if (envConfig == null)
            return false;
        if (string.IsNullOrEmpty(envConfig.BootstrapServers))
        {
            _logger.LogError("BootstrapServers is required.");
            return false;
        }
        if (string.IsNullOrEmpty(envConfig.Topic))
        {
            _logger.LogError("Topic is required.");
            return false;
        }
        if (string.IsNullOrEmpty(envConfig.GroupId))
        {
            _logger.LogError("GroupId is required.");
            return false;
        }
        if (envConfig.MaxRateLimit <= 0)
        {
            _logger.LogError("MaxRateLimit must be greater than 0.");
            return false;
        }
        return true;
    }
}
 