using Confluent.Kafka;
using MBrokerConsumer.Models;
using Microsoft.Extensions.Logging;
using System.Threading;

namespace MBrokerConsumer.Services.Implementations;

internal sealed class MainProgram : IMainProgram
{
    private readonly ILogger<Program> _logger;
    private readonly ConsumerEnvConfig _envConfig;
    private readonly TokenBucketLimiter _rateLimiter;
    private readonly IHealthProbeService _healthProbe;
    private readonly ICommitTracker _commitTracker;
    private readonly IRateLogger _rateLogger;

    private readonly CancellationTokenSource _cancellationToken = new();
    private readonly ManualResetEventSlim _drainComplete = new(false);
    public MainProgram(
        ILogger<Program> logger,
        ConsumerEnvConfig envConfig,
        TokenBucketLimiter rateLimiter,
        ITerminationService terminationService,
        IHealthProbeService healthProbe,
        ICommitTracker commitTracker,
        IRateLogger rateLogger
        )
    {
        _logger = logger;
        _envConfig = envConfig;
        _rateLimiter = rateLimiter;
        _healthProbe = healthProbe;
        _commitTracker = commitTracker;
        _rateLogger = rateLogger;
        terminationService.TerminationRequested += () =>
        {
            _logger.LogInformation("Termination requested. Initiating shutdown...");
            OnTerminate();
        };
    }

    private void OnTerminate()
    {
        _cancellationToken.Cancel();
        _logger.LogInformation("Draining in-flight messages for up to {Seconds}s...", _envConfig.DrainTimeoutSeconds);

        if (!_drainComplete.Wait(TimeSpan.FromSeconds(_envConfig.DrainTimeoutSeconds)))
        {
            _logger.LogWarning("Drain timeout exceeded ({Seconds}s). Forcing exit.", _envConfig.DrainTimeoutSeconds);
            Environment.Exit(1);
        }
    }

    public async Task Run()
    {
        if (!ValidateConfig(_envConfig))
        {
            _logger.LogError("Invalid configuration. Please check environment variables.");
            return;
        }

        LogConfig(_envConfig);

        var loggingWindowLimiter = new LoggingWindowLimter(TimeSpan.FromMilliseconds(1000));

        var config = _envConfig.ToConsumerConfig();

        using var consumer = new ConsumerBuilder<Ignore, byte[]>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka Error: {Reason}", e.Reason))
            .Build();

        consumer.Subscribe(_envConfig.Topic);

        _logger.LogInformation("Consumption loop started.");


        var consumptionCounter = new ConsumptionCounter();

        try
        {
            while (!_cancellationToken.IsCancellationRequested)
            {
                try
            {
                await _rateLimiter.ConsumeAsync();

                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));

                    if (consumeResult != null)
                    {
                        _healthProbe.MarkReady();
                        _healthProbe.ReportMessageReceived();
                        consumptionCounter.Increment();

                        if (loggingWindowLimiter.ShouldLog(out var timePassed))
                        {
                            _logger.LogInformation(
                                "Consumed {Count} msgs in {Elapsed:F2}s. Total: {Total}",
                                consumptionCounter.CurrentWindowCount, timePassed.TotalSeconds, consumptionCounter.TotalCount);
                            consumptionCounter.ResetWindow();
                        }

                        _commitTracker.TryCommit(() => consumer.Commit());
                        _rateLogger.MessageConsumed();

                        // Log rate + estimated lag every 10s
                        long totalLag = 0;
                        foreach (var tp in consumer.Assignment)
                        {
                            try
                            {
                                var watermarks = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(2));
                                var position = consumer.Position(tp);
                                totalLag += Math.Max(0, watermarks.High.Value - position.Value);
                            }
                            catch
                            {
                                // skip partitions where lag can't be computed yet
                            }
                        }

                        _rateLogger.TryLogRateAndLag(totalLag);
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
            _commitTracker.ForceCommit(() => consumer.Commit());

            if (consumptionCounter.CurrentWindowCount > 0)
            {
                _logger.LogInformation(
                    "Final batch: {Count} msgs in {Elapsed:F2}s. Total: {Total}",
                    consumptionCounter.CurrentWindowCount, loggingWindowLimiter.LastTime.TotalSeconds, consumptionCounter.TotalCount);
            }

            _logger.LogInformation(
                "Shutdown complete — committed offsets, {Total} messages processed",
                consumptionCounter.TotalCount);

            _healthProbe.Stop();

            consumer.Close();
            _drainComplete.Set();
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