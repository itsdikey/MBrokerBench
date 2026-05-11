using MBrokerConsumer.Models;
using MBrokerConsumer.Services;
using MBrokerConsumer.Services.Implementations;
using Microsoft.Extensions.Logging.Abstractions;

namespace MBrokerConsumer.Tests;

public class CommitTrackerTests
{
    [Fact]
    public void TryCommit_ReturnsFalse_BeforeIntervalElapses()
    {
        var config = new ConsumerEnvConfig { CommitIntervalSeconds = 60 };
        var tracker = new CommitTracker(config, NullLogger<CommitTracker>.Instance);
        var committed = false;

        var result = tracker.TryCommit(() => committed = true);

        Assert.False(result);
        Assert.False(committed);
    }

    [Fact]
    public void TryCommit_ReturnsTrue_AfterIntervalElapses()
    {
        var config = new ConsumerEnvConfig { CommitIntervalSeconds = 0 }; // immediate
        var tracker = new CommitTracker(config, NullLogger<CommitTracker>.Instance);
        var committed = false;

        var result = tracker.TryCommit(() => committed = true);

        Assert.True(result);
        Assert.True(committed);
    }

    [Fact]
    public void TryCommit_HandlesException()
    {
        var config = new ConsumerEnvConfig { CommitIntervalSeconds = 0 };
        var tracker = new CommitTracker(config, NullLogger<CommitTracker>.Instance);

        // Should not throw
        var result = tracker.TryCommit(() => throw new InvalidOperationException("test"));

        Assert.True(result);
    }

    [Fact]
    public void ForceCommit_ExecutesImmediately()
    {
        var config = new ConsumerEnvConfig { CommitIntervalSeconds = 60 };
        var tracker = new CommitTracker(config, NullLogger<CommitTracker>.Instance);
        var committed = false;

        tracker.ForceCommit(() => committed = true);

        Assert.True(committed);
    }

    [Fact]
    public void ForceCommit_HandlesException()
    {
        var config = new ConsumerEnvConfig { CommitIntervalSeconds = 60 };
        var tracker = new CommitTracker(config, NullLogger<CommitTracker>.Instance);

        // Should not throw
        tracker.ForceCommit(() => throw new InvalidOperationException("test"));
    }
}