using MBrokerConsumer.Services;
using MBrokerConsumer.Services.Implementations;
using Microsoft.Extensions.Logging.Abstractions;

namespace MBrokerConsumer.Tests;

public class RateLoggerTests
{
    [Fact]
    public void MessageConsumed_IncreasesTotalConsumed()
    {
        var logger = new RateLogger(NullLogger<RateLogger>.Instance, TimeSpan.FromHours(1));
        Assert.Equal(0, logger.TotalConsumed);

        logger.MessageConsumed();
        logger.MessageConsumed();
        logger.MessageConsumed();

        Assert.Equal(3, logger.TotalConsumed);
    }

    [Fact]
    public void TryLogRateAndLag_ReturnsFalse_BeforeInterval()
    {
        var logger = new RateLogger(NullLogger<RateLogger>.Instance, TimeSpan.FromHours(1));
        var result = logger.TryLogRateAndLag(0);
        Assert.False(result);
    }

    [Fact]
    public void TryLogRateAndLag_ReturnsTrue_AfterInterval()
    {
        var logger = new RateLogger(NullLogger<RateLogger>.Instance, TimeSpan.FromMilliseconds(1));
        Thread.Sleep(5);
        logger.MessageConsumed();
        var result = logger.TryLogRateAndLag(100);
        Assert.True(result);
    }

    [Fact]
    public void TryLogRateAndLag_ResetsPeriodCount()
    {
        var logger = new RateLogger(NullLogger<RateLogger>.Instance, TimeSpan.FromMilliseconds(1));
        Thread.Sleep(5);
        logger.MessageConsumed();
        logger.TryLogRateAndLag(0);

        // After logging, period count should be 0
        // Immediate next call should return false (interval needs to elapse again + period is 0)
        var result = logger.TryLogRateAndLag(0);
        Assert.False(result);
    }

    [Fact]
    public void TotalConsumed_AccumulatesAcrossPeriods()
    {
        var logger = new RateLogger(NullLogger<RateLogger>.Instance, TimeSpan.FromMilliseconds(1));
        logger.MessageConsumed();
        logger.MessageConsumed();

        Thread.Sleep(5);
        logger.TryLogRateAndLag(0);

        logger.MessageConsumed();

        Assert.Equal(3, logger.TotalConsumed);
    }
}