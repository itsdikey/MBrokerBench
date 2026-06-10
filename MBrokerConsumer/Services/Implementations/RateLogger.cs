using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace MBrokerConsumer.Services.Implementations;

public class RateLogger : IRateLogger
{
    private readonly ILogger<RateLogger> _logger;
    private readonly TimeSpan _interval;
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    private long _periodCount;
    private long _totalCount;

    public RateLogger(ILogger<RateLogger> logger, TimeSpan? interval = null)
    {
        _logger = logger;
        _interval = interval ?? TimeSpan.FromSeconds(10);
    }

    public void MessageConsumed()
    {
        Interlocked.Increment(ref _periodCount);
        Interlocked.Increment(ref _totalCount);
    }

    public long TotalConsumed => Interlocked.Read(ref _totalCount);

    public bool TryLogRateAndLag(long totalLag)
    {
        if (!ShouldLog)
            return false;

        var elapsed = _stopwatch.Elapsed.TotalSeconds;
        var count = Interlocked.Exchange(ref _periodCount, 0);
        var rate = count / elapsed;

        _logger.LogInformation(
            "Consumer rate: {Rate:F0} msgs/s, estimated lag: {Lag:N0}",
            rate, totalLag);

        _stopwatch.Restart();
        return true;
    }

    public bool ShouldLog => _stopwatch.Elapsed >= _interval;
}