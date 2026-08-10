using MBrokerConsumer.Models;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace MBrokerConsumer.Services.Implementations;

public class CommitTracker : ICommitTracker
{
    private readonly TimeSpan _interval;
    private readonly ILogger<CommitTracker> _logger;
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

    public CommitTracker(ConsumerEnvConfig envConfig, ILogger<CommitTracker> logger)
    {
        _interval = TimeSpan.FromSeconds(envConfig.CommitIntervalSeconds);
        _logger = logger;
    }

    public bool TryCommit(Action commitAction)
    {
        if (_stopwatch.Elapsed < _interval)
            return false;

        var elapsed = _stopwatch.Elapsed;
        try
        {
            commitAction();
            _logger.LogInformation(
                "[COMMIT] Success — committed offsets after {Elapsed:F1}s (interval: {Interval:F1}s)",
                elapsed.TotalSeconds, _interval.TotalSeconds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[COMMIT] Failed: {Message}", ex.Message);
        }

        _stopwatch.Restart();
        return true;
    }

    public void ForceCommit(Action commitAction)
    {
        try
        {
            commitAction();
            _logger.LogInformation("Forced commit completed");
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Forced commit failed: {Message}", ex.Message);
        }
    }
}