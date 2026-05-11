namespace MBrokerBench.Phase2Tests;

/// <summary>Configuration for the Phase 2 end-to-end test.</summary>
public record TestConfiguration
{
    public string ClusterName { get; init; } = "mbroker-dev";
    public int ClusterAgents { get; init; } = 3;
    public string TopicName { get; init; } = "test-1";
    public int TopicPartitions { get; init; } = 32;
    public string ConsumerGroup { get; init; } = "test-group";
    public int StressMessages { get; init; } = 10_000_000;
    public int StressThroughput { get; init; } = 5000;
    public int KafkaPort { get; init; } = 9092;
    public int PrometheusPort { get; init; } = 9090;

    /// <summary>Max seconds to wait for Kafka to become ready.</summary>
    public int KafkaReadyTimeoutSec { get; init; } = 300;
    /// <summary>Max seconds to wait for a new consumer pod to appear after lag spike.</summary>
    public int ScaleUpTimeoutSec { get; init; } = 60;
    /// <summary>Max seconds to wait for lag to decrease after new pods join.</summary>
    public int LagRecoveryTimeoutSec { get; init; } = 120;
    /// <summary>How often to poll pod status / lag metrics.</summary>
    public int PollIntervalMs { get; init; } = 2000;
    /// <summary>How long to stress-test before beginning verification (seconds).</summary>
    public int StressDurationSec { get; init; } = 30;
    /// <summary>How long to monitor after starting stress (seconds).</summary>
    public int MonitorDurationSec { get; init; } = 180;
    /// <summary>Strimzi Kafka image for admin/stress pods.</summary>
    public string StrimziImage { get; init; } = "quay.io/strimzi/kafka:0.50.1-kafka-4.1.1";
}

/// <summary>Phases of the end-to-end test.</summary>
public enum TestPhase
{
    Prerequisites,
    EnvironmentSetup,
    ImageLoad,
    InfrastructureDeploy,
    Connectivity,
    WorkloadSetup,
    ControllerLaunch,
    AutoscaleVerification,
    Cleanup
}

/// <summary>Result of a single test step.</summary>
public class StepResult
{
    public string StepName { get; init; } = "";
    public bool Passed { get; set; }
    public string? Detail { get; set; }
    public TimeSpan Duration { get; set; }
    public Exception? Exception { get; set; }
}

/// <summary>An individual success criterion with pass/fail status.</summary>
public class SuccessCriterion
{
    public string Description { get; init; } = "";
    public bool Passed { get; set; }
    public string? Evidence { get; set; }
}

/// <summary>Overall test outcome.</summary>
public class TestOutcome
{
    public bool OverallPassed => Steps.All(s => s.Passed) && Criteria.All(c => c.Passed);
    public List<StepResult> Steps { get; init; } = [];
    public List<SuccessCriterion> Criteria { get; init; } = [];
    public DateTime StartTime { get; init; } = DateTime.UtcNow;
    public DateTime EndTime { get; set; }
    public TimeSpan TotalDuration => EndTime - StartTime;

    public void AddStep(StepResult step) => Steps.Add(step);
    public void AddCriterion(SuccessCriterion criterion) => Criteria.Add(criterion);

    public void SetEndTime() => EndTime = DateTime.UtcNow;
}

/// <summary>Tracks a long-running background process.</summary>
public class BackgroundProcess : IDisposable
{
    public string Label { get; init; } = "";
    public System.Diagnostics.Process Process { get; init; } = null!;
    public CancellationTokenSource Cts { get; init; } = new();
    public List<string> OutputLines { get; } = [];
    public bool IsRunning => Process is { HasExited: false };

    public void Dispose()
    {
        if (!Process.HasExited)
        {
            try { Process.Kill(entireProcessTree: true); } catch { }
            try { Process.Close(); } catch { }
        }
        Cts.Cancel();
        Cts.Dispose();
        Process.Dispose();
    }
}
