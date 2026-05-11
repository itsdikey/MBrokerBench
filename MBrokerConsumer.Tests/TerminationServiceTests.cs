using MBrokerConsumer.Services.Implementations;

namespace MBrokerConsumer.Tests;

public class TerminationServiceTests
{
    [Fact]
    public void RequestTermination_FiresEvent()
    {
        var service = new TerminationService();
        var fired = false;
        service.TerminationRequested += () => fired = true;
        
        service.RequestTermination();
        
        Assert.True(fired);
    }

    [Fact]
    public void RequestTermination_OnlyFiresOnce()
    {
        var service = new TerminationService();
        var callCount = 0;
        service.TerminationRequested += () => callCount++;
        
        service.RequestTermination();
        service.RequestTermination();
        
        Assert.Equal(1, callCount);
    }
}