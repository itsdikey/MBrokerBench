using Spectre.Console;
using MBrokerBench.Phase2Tests;

AnsiConsole.Write(new FigletText("Phase 2 E2E Test").Color(Color.Blue));
AnsiConsole.MarkupLine("[grey]MBrokerBench - Real Infrastructure Autoscale Verification[/]");
AnsiConsole.WriteLine();

// ──────────────────────────────────────────────
// Parse command-line arguments
// ──────────────────────────────────────────────

var config = ParseArgs(args);

AnsiConsole.MarkupLine($"[grey]Configuration:[/]");
AnsiConsole.MarkupLine($"  Topic: [cyan]{config.TopicName}[/] ({config.TopicPartitions} partitions)");
AnsiConsole.MarkupLine($"  Stress: [cyan]{config.StressMessages:N0}[/] msgs @ [cyan]{config.StressThroughput:N0}[/]/s");
AnsiConsole.MarkupLine($"  Cluster: [cyan]{config.ClusterName}[/] ({config.ClusterAgents} agents)");
AnsiConsole.MarkupLine($"  Scale-up timeout: [cyan]{config.ScaleUpTimeoutSec}s[/], Lag recovery: [cyan]{config.LagRecoveryTimeoutSec}s[/]");
AnsiConsole.WriteLine();

// ──────────────────────────────────────────────
// Confirm or run
// ──────────────────────────────────────────────

bool autoYes = args.Contains("--yes") || args.Contains("-y") || Console.IsInputRedirected;

if (!autoYes)
{
    var confirmed = AnsiConsole.Confirm("Run the Phase 2 end-to-end test?", false);
    if (!confirmed)
    {
        AnsiConsole.MarkupLine("[yellow]Test cancelled by user.[/]");
        return 1;
    }
}
else
{
    AnsiConsole.MarkupLine("[grey]Auto-confirmed (non-interactive mode).[/]");
}

// ──────────────────────────────────────────────
// Execute
// ──────────────────────────────────────────────

var orchestrator = new TestOrchestrator(config);
var outcome = await orchestrator.RunAsync();

return outcome.OverallPassed ? 0 : 1;

// ──────────────────────────────────────────────
// Argument parsing
// ──────────────────────────────────────────────

static TestConfiguration ParseArgs(string[] args)
{
    var cfg = new TestConfiguration();

    for (int i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--topic" when i + 1 < args.Length:
                cfg = cfg with { TopicName = args[++i] };
                break;
            case "--partitions" when i + 1 < args.Length:
                cfg = cfg with { TopicPartitions = int.Parse(args[++i]) };
                break;
            case "--messages" when i + 1 < args.Length:
                cfg = cfg with { StressMessages = int.Parse(args[++i]) };
                break;
            case "--throughput" when i + 1 < args.Length:
                cfg = cfg with { StressThroughput = int.Parse(args[++i]) };
                break;
            case "--cluster-agents" when i + 1 < args.Length:
                cfg = cfg with { ClusterAgents = int.Parse(args[++i]) };
                break;
            case "--monitor-duration" when i + 1 < args.Length:
                cfg = cfg with { MonitorDurationSec = int.Parse(args[++i]) };
                break;
            case "--stress-duration" when i + 1 < args.Length:
                cfg = cfg with { StressDurationSec = int.Parse(args[++i]) };
                break;
            case "--poll-interval" when i + 1 < args.Length:
                cfg = cfg with { PollIntervalMs = int.Parse(args[++i]) };
                break;
            case "--help":
            case "-h":
                PrintHelp();
                Environment.Exit(0);
                break;
        }
    }

    return cfg;
}

static void PrintHelp()
{
    AnsiConsole.MarkupLine("[bold]Usage:[/] dotnet run --project MBrokerBench.Phase2Tests [options]");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Options:[/]");
    AnsiConsole.MarkupLine("  [cyan]--topic <name>[/]          Topic name (default: test-1)");
    AnsiConsole.MarkupLine("  [cyan]--partitions <n>[/]        Partition count (default: 32)");
    AnsiConsole.MarkupLine("  [cyan]--messages <n>[/]          Stress messages (default: 10000000)");
    AnsiConsole.MarkupLine("  [cyan]--throughput <n>[/]        Messages/second (default: 5000)");
    AnsiConsole.MarkupLine("  [cyan]--cluster-agents <n>[/]    k3d agent count (default: 3)");
    AnsiConsole.MarkupLine("  [cyan]--monitor-duration <s>[/]  Monitoring seconds (default: 180)");
    AnsiConsole.MarkupLine("  [cyan]--stress-duration <s>[/]   Wait before monitoring (default: 30)");
    AnsiConsole.MarkupLine("  [cyan]--poll-interval <ms>[/]    Polling interval (default: 2000)");
    AnsiConsole.MarkupLine("  [cyan]-y, --yes[/]               Skip confirmation prompt");
    AnsiConsole.MarkupLine("  [cyan]-h, --help[/]              Show this help");
}
