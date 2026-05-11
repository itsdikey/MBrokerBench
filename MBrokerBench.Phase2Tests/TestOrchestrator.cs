using Spectre.Console;

namespace MBrokerBench.Phase2Tests;

/// <summary>Orchestrates the Phase 2 end-to-end test, executing steps in order.</summary>
public class TestOrchestrator
{
    private readonly TestConfiguration _config;
    private readonly TestOutcome _outcome = new();
    private readonly List<BackgroundProcess> _backgroundProcesses = [];
    private CancellationTokenSource? _globalCts;

    public TestOrchestrator(TestConfiguration config)
    {
        _config = config;
    }

    public async Task<TestOutcome> RunAsync()
    {
        _globalCts = new CancellationTokenSource();
        var ct = _globalCts.Token;

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            AnsiConsole.MarkupLine("\n[yellow]Test cancelled by user. Cleaning up...[/]");
            _globalCts?.Cancel();
        };

        try
        {
            await RunPhase0Prerequisites(ct);
            if (ShouldStop) return Finalize();

            await RunPhase1EnvironmentSetup(ct);
            if (ShouldStop) return Finalize();

            await RunPhase2ImageLoad(ct);
            if (ShouldStop) return Finalize();

            await RunPhase3InfrastructureDeploy(ct);
            if (ShouldStop) return Finalize();

            await RunPhase4Connectivity(ct);
            if (ShouldStop) return Finalize();

            await RunPhase5WorkloadSetup(ct);
            if (ShouldStop) return Finalize();

            await RunPhase6ControllerLaunch(ct);
            if (ShouldStop) return Finalize();

            await RunPhase7AutoscaleVerification(ct);
        }
        finally
        {
            await CleanupAsync();
        }

        return Finalize();
    }

    private bool ShouldStop => _globalCts?.IsCancellationRequested == true;

    // ──────────────────────────────────────────────
    // Phase Implementations
    // ──────────────────────────────────────────────

    private async Task RunPhase0Prerequisites(CancellationToken ct)
    {
        Reporter.WritePhaseHeader(TestPhase.Prerequisites);

        var depResult = await Steps.CheckDependencies(ct);
        _outcome.AddStep(depResult);
        Reporter.WriteStep(depResult);
        if (!depResult.Passed) { _globalCts?.Cancel(); return; }

        var clusterResult = await Steps.CheckExistingCluster(ct);
        _outcome.AddStep(clusterResult);
        Reporter.WriteStep(clusterResult);
        // Don't stop if cluster doesn't exist — we'll create it in Phase 1
    }

    private async Task RunPhase1EnvironmentSetup(CancellationToken ct)
    {
        Reporter.WritePhaseHeader(TestPhase.EnvironmentSetup);

        // Check if cluster already exists first
        var (exitCode, clusterOut, _) = await ProcessRunner.RunCommandAsync("k3d", "cluster list -o json", ct);
        var clusterExists = exitCode == 0 && clusterOut.Contains(_config.ClusterName);

        if (!clusterExists)
        {
            var clusterResult = await Steps.ClusterUp(_config, ct);
            _outcome.AddStep(clusterResult);
            Reporter.WriteStep(clusterResult);
            if (!clusterResult.Passed) { _globalCts?.Cancel(); return; }
        }
        else
        {
            _outcome.AddStep(new StepResult
            {
                StepName = "Create k3d cluster",
                Passed = true,
                Detail = "Cluster already exists",
                Duration = TimeSpan.Zero
            });
            Reporter.WriteStep(_outcome.Steps.Last());
        }

        // Switch kubectl context to k3d cluster
        var (kCtxCode, _, kCtxErr) = await ProcessRunner.RunCommandAsync("kubectl",
            $"config use-context k3d-{_config.ClusterName}", ct);
        if (kCtxCode != 0)
        {
            _outcome.AddStep(new StepResult
            {
                StepName = "Switch kubectl context to k3d cluster",
                Passed = false,
                Detail = TrimK8sError(kCtxErr),
                Duration = TimeSpan.Zero
            });
            Reporter.WriteStep(_outcome.Steps.Last());
            _globalCts?.Cancel();
            return;
        }

        var strimziResult = await Steps.InstallStrimzi(ct);
        _outcome.AddStep(strimziResult);
        Reporter.WriteStep(strimziResult);
        if (!strimziResult.Passed) { _globalCts?.Cancel(); return; }

        var kafkaResult = await Steps.DeployKafka(_config, ct);
        _outcome.AddStep(kafkaResult);
        Reporter.WriteStep(kafkaResult);
        if (!kafkaResult.Passed) { _globalCts?.Cancel(); return; }

        var obsResult = await Steps.DeployObservability(ct);
        _outcome.AddStep(obsResult);
        Reporter.WriteStep(obsResult);
        if (!obsResult.Passed) { _globalCts?.Cancel(); return; }
    }

    private async Task RunPhase2ImageLoad(CancellationToken ct)
    {
        Reporter.WritePhaseHeader(TestPhase.ImageLoad);
        var result = await Steps.LoadConsumerImage(_config, ct);
        _outcome.AddStep(result);
        Reporter.WriteStep(result);
        if (!result.Passed) { _globalCts?.Cancel(); return; }
    }

    private async Task RunPhase3InfrastructureDeploy(CancellationToken ct)
    {
        Reporter.WritePhaseHeader(TestPhase.InfrastructureDeploy);
        var result = await Steps.DeployInfrastructure(ct);
        _outcome.AddStep(result);
        Reporter.WriteStep(result);
        if (!result.Passed) { _globalCts?.Cancel(); return; }
    }

    private async Task RunPhase4Connectivity(CancellationToken ct)
    {
        Reporter.WritePhaseHeader(TestPhase.Connectivity);

        var (kafkaResult, kafkaBg) = await Steps.StartKafkaPortForward(_config, ct);
        _outcome.AddStep(kafkaResult);
        Reporter.WriteStep(kafkaResult);
        if (kafkaBg != null) _backgroundProcesses.Add(kafkaBg);
        if (!kafkaResult.Passed) { _globalCts?.Cancel(); return; }

        var (promResult, promBg) = await Steps.StartPrometheusPortForward(_config, ct);
        _outcome.AddStep(promResult);
        Reporter.WriteStep(promResult);
        if (promBg != null) _backgroundProcesses.Add(promBg);
        if (!promResult.Passed) { _globalCts?.Cancel(); return; }

        var verifyResult = await Steps.VerifyKafkaConnectivity(_config, ct);
        _outcome.AddStep(verifyResult);
        Reporter.WriteStep(verifyResult);
        if (!verifyResult.Passed) { _globalCts?.Cancel(); return; }
    }

    private async Task RunPhase5WorkloadSetup(CancellationToken ct)
    {
        Reporter.WritePhaseHeader(TestPhase.WorkloadSetup);

        var topicResult = await Steps.CreateTopic(_config, ct);
        _outcome.AddStep(topicResult);
        Reporter.WriteStep(topicResult);
        if (!topicResult.Passed) { _globalCts?.Cancel(); return; }

        AnsiConsole.MarkupLine("  [grey]Starting stress test in background...[/]");
        var stressBg = Steps.StartStressTest(_config, _config.StrimziImage);
        _backgroundProcesses.Add(stressBg);

        _outcome.AddStep(new StepResult
        {
            StepName = $"Start stress test ({_config.StressMessages} msgs @ {_config.StressThroughput}/s)",
            Passed = true,
            Detail = $"Stress test running in background (PID: {stressBg.Process.Id})",
            Duration = TimeSpan.Zero
        });
        Reporter.WriteStep(_outcome.Steps.Last());

        // Let stress run for a bit before launching controller
        AnsiConsole.MarkupLine($"  [grey]Waiting {_config.StressDurationSec}s for stress to build lag...[/]");
        await Task.Delay(TimeSpan.FromSeconds(_config.StressDurationSec), ct);
    }

    private async Task RunPhase6ControllerLaunch(CancellationToken ct)
    {
        Reporter.WritePhaseHeader(TestPhase.ControllerLaunch);

        AnsiConsole.MarkupLine("  [grey]Starting Phase 2 controller in background...[/]");
        var controllerBg = Steps.StartPhase2Controller();
        _backgroundProcesses.Add(controllerBg);

        // Wait for initial controller output
        await Task.Delay(5000, ct);

        var hasOutput = controllerBg.OutputLines.Count > 0;
        _outcome.AddStep(new StepResult
        {
            StepName = "Launch Phase 2 Controller (Real Scaling Mode)",
            Passed = hasOutput,
            Detail = hasOutput
                ? $"Controller running (PID: {controllerBg.Process.Id})"
                : "Controller started but no output yet — may need more time",
            Duration = TimeSpan.FromSeconds(5)
        });
        Reporter.WriteStep(_outcome.Steps.Last());
    }

    private async Task RunPhase7AutoscaleVerification(CancellationToken ct)
    {
        Reporter.WritePhaseHeader(TestPhase.AutoscaleVerification);

        var stressBg = _backgroundProcesses.FirstOrDefault(b => b.Label == "stress-test");
        var controllerBg = _backgroundProcesses.FirstOrDefault(b => b.Label == "phase2-controller");

        if (stressBg == null || controllerBg == null)
        {
            AnsiConsole.MarkupLine("  [red]Cannot verify — stress test or controller not running[/]");
            _outcome.AddStep(new StepResult
            {
                StepName = "Monitor autoscale behavior",
                Passed = false,
                Detail = "Stress test or controller not available for monitoring"
            });
            _globalCts?.Cancel();
            return;
        }

        var criteria = await Steps.MonitorAndVerify(_config, stressBg, controllerBg, ct);
        foreach (var c in criteria)
        {
            _outcome.AddCriterion(c);
        }

        _outcome.AddStep(new StepResult
        {
            StepName = "Monitor autoscale behavior",
            Passed = criteria.All(c => c.Passed),
            Detail = $"Observed {criteria.Count(c => c.Passed)}/{criteria.Count} criteria passed",
            Duration = TimeSpan.Zero
        });
        Reporter.WriteStep(_outcome.Steps.Last());
    }

    // ──────────────────────────────────────────────
    // Cleanup & Finalize
    // ──────────────────────────────────────────────

    private async Task CleanupAsync()
    {
        Reporter.WritePhaseHeader(TestPhase.Cleanup);

        // Stop background processes in reverse order
        for (int i = _backgroundProcesses.Count - 1; i >= 0; i--)
        {
            var bg = _backgroundProcesses[i];
            if (bg.IsRunning)
            {
                AnsiConsole.MarkupLine($"  [grey]Stopping: {bg.Label}...[/]");
                bg.Dispose();
            }
        }
        _backgroundProcesses.Clear();

        _outcome.AddStep(new StepResult
        {
            StepName = "Cleanup background processes",
            Passed = true,
            Detail = "All background processes stopped"
        });
        Reporter.WriteStep(_outcome.Steps.Last());
    }

    private TestOutcome Finalize()
    {
        _outcome.SetEndTime();
        Reporter.WriteSummary(_outcome);
        return _outcome;
    }

    private static string TrimK8sError(string error)
    {
        if (string.IsNullOrEmpty(error)) return "";
        var lines = error.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        return string.Join("; ", lines.Take(3));
    }
}
