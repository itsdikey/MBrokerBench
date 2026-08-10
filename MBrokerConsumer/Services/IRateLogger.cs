namespace MBrokerConsumer.Services;

public interface IRateLogger
{
    /// <summary>Call on every consumed message to track count.</summary>
    void MessageConsumed();
    /// <summary>Every 10s, logs rate + lag + assignment count + total consumed and resets. Returns true if logged.</summary>
    bool TryLogRateAndLag(long totalLag, int assignmentCount = 0);
    /// <summary>Returns true if the log interval has elapsed and a log attempt should be made.</summary>
    bool ShouldLog { get; }
}
