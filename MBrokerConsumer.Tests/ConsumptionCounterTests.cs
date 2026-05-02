using MBrokerConsumer.Models;

namespace MBrokerConsumer.Tests;

public class ConsumptionCounterTests
{
    [Fact]
    public void InitialState_ShouldBeZero()
    {
        var counter = new ConsumptionCounter();
        Assert.Equal(0, counter.CurrentWindowCount);
        Assert.Equal(0, counter.TotalCount);
    }

    [Fact]
    public void Increment_ShouldIncreaseBothCounters()
    {
        var counter = new ConsumptionCounter();
        counter.Increment();
        Assert.Equal(1, counter.CurrentWindowCount);
        Assert.Equal(1, counter.TotalCount);
    }

    [Fact]
    public void Increment_WithCustomCount_ShouldIncreaseBySpecifiedAmount()
    {
        var counter = new ConsumptionCounter();
        counter.Increment(5);
        Assert.Equal(5, counter.CurrentWindowCount);
        Assert.Equal(5, counter.TotalCount);
    }

    [Fact]
    public void ResetWindow_ShouldZeroCurrentWindowCountOnly()
    {
        var counter = new ConsumptionCounter();
        counter.Increment(10);
        counter.ResetWindow();
        Assert.Equal(0, counter.CurrentWindowCount);
        Assert.Equal(10, counter.TotalCount);
    }

    [Fact]
    public void MultipleIncrementAndResetCycles_ShouldAccumulateTotalCorrectly()
    {
        var counter = new ConsumptionCounter();
        counter.Increment(3);
        counter.ResetWindow();
        counter.Increment(5);
        counter.ResetWindow();
        counter.Increment(2);
        Assert.Equal(2, counter.CurrentWindowCount);
        Assert.Equal(10, counter.TotalCount);
    }
}