using MBrokerConsumer.Services.Implementations;

namespace MBrokerConsumer.Tests;

public class LoggingWindowLimterTests
{
    [Fact]
    public void ShouldLog_ReturnsFalse_BeforeWindowElapses()
    {
        var limiter = new LoggingWindowLimter(TimeSpan.FromHours(1));
        var result = limiter.ShouldLog(out var timePassed);
        Assert.False(result);
        Assert.True(timePassed.TotalSeconds < 1);
    }

    [Fact]
    public void ShouldLog_ReturnsTrue_AfterWindowElapses()
    {
        var limiter = new LoggingWindowLimter(TimeSpan.FromMilliseconds(1));
        Thread.Sleep(5);
        var result = limiter.ShouldLog(out var timePassed);
        Assert.True(result);
        Assert.True(timePassed.TotalMilliseconds >= 1);
    }

    [Fact]
    public void LastTime_CapturesElapsedBeforeReset()
    {
        var limiter = new LoggingWindowLimter(TimeSpan.FromMilliseconds(1));
        Thread.Sleep(5);
        limiter.ShouldLog(out var firstTime);
        Assert.Equal(firstTime, limiter.LastTime);
    }

    [Fact]
    public void ShouldLog_ResetsWindow_SoNextCallReturnsFalse()
    {
        var limiter = new LoggingWindowLimter(TimeSpan.FromMilliseconds(1));
        Thread.Sleep(5);
        limiter.ShouldLog(out _);
        // immediately after reset, should return false
        var result = limiter.ShouldLog(out _);
        Assert.False(result);
    }

    [Fact]
    public void CurrentTimePassed_ReportsTimeSinceConstruction()
    {
        var limiter = new LoggingWindowLimter(TimeSpan.FromHours(1));
        Thread.Sleep(10);
        Assert.True(limiter.CurrentTimePassed.TotalMilliseconds >= 10);
    }

    [Fact]
    public void CurrentTimePassed_IncreasesOverTime()
    {
        var limiter = new LoggingWindowLimter(TimeSpan.FromHours(1));
        var t1 = limiter.CurrentTimePassed;
        Thread.Sleep(20);
        var t2 = limiter.CurrentTimePassed;
        Assert.True(t2 > t1);
    }
}