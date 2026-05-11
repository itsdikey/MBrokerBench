namespace MBrokerConsumer.Services;

public interface IRateLogger
{
    /// <summary>Call on every consumed message to track count.</summary>
    void MessageConsumed();
    /// <summary>Every 10s, logs rate + lag and resets. Returns true if logged.</summary>
    bool TryLogRateAndLag(long totalLag);
}