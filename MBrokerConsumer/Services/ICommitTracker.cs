namespace MBrokerConsumer.Services;

public interface ICommitTracker
{
    /// <summary>Commits via the provided action if the commit interval has elapsed. Returns true if commit was attempted.</summary>
    bool TryCommit(Action commitAction);
    /// <summary>Forces an immediate commit (for shutdown).</summary>
    void ForceCommit(Action commitAction);
}