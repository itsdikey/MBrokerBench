using Spectre.Console;

namespace MBrokerBench.Phase2Tests;

/// <summary>Console reporting for the Phase 2 test harness.</summary>
public static class Reporter
{
    public static void WriteBanner()
    {
        AnsiConsole.Write(new FigletText("Phase 2 E2E Test").Color(Color.Blue));
        AnsiConsole.MarkupLine("[grey]MBrokerBench - Real Infrastructure Autoscale Verification[/]");
        AnsiConsole.WriteLine();
    }

    public static void WritePhaseHeader(TestPhase phase)
    {
        var phaseName = phase switch
        {
            TestPhase.Prerequisites => "[bold yellow]Phase 0:[/] Prerequisites Check",
            TestPhase.EnvironmentSetup => "[bold yellow]Phase 1:[/] Environment Setup (Cluster, Kafka, Observability)",
            TestPhase.ImageLoad => "[bold yellow]Phase 2:[/] Load Consumer Image",
            TestPhase.InfrastructureDeploy => "[bold yellow]Phase 3:[/] Deploy Infrastructure (RBAC, Consumers, Controller)",
            TestPhase.Connectivity => "[bold yellow]Phase 4:[/] Connectivity (Port Forwarding)",
            TestPhase.WorkloadSetup => "[bold yellow]Phase 5:[/] Workload Setup (Topic, Stress Test)",
            TestPhase.ControllerLaunch => "[bold yellow]Phase 6:[/] Launch Phase 2 Controller",
            TestPhase.AutoscaleVerification => "[bold yellow]Phase 7:[/] Monitor & Verify Autoscale",
            TestPhase.Cleanup => "[bold yellow]Phase 8:[/] Cleanup",
            _ => phase.ToString()
        };
        AnsiConsole.MarkupLine($"\n{phaseName}");
        AnsiConsole.MarkupLine(new string('-', 60));
    }

    public static void WriteStep(StepResult result)
    {
        var icon = result.Passed ? "[green]PASS[/]" : "[red]FAIL[/]";
        var duration = result.Duration.TotalSeconds < 60
            ? $"{result.Duration.TotalSeconds:F1}s"
            : $"{result.Duration.TotalMinutes:F1}m";
        AnsiConsole.MarkupLine($"  {icon} {result.StepName} [grey]({duration})[/]");

        if (!string.IsNullOrEmpty(result.Detail))
        {
            var color = result.Passed ? "grey" : "red";
            // Indent detail lines
            foreach (var line in result.Detail.Split('\n'))
                AnsiConsole.MarkupLine($"    [{color}]{line.EscapeMarkup()}[/]");
        }

        if (result.Exception != null)
        {
            AnsiConsole.MarkupLine($"    [red]Exception: {result.Exception.Message.EscapeMarkup()}[/]");
        }
    }

    public static void WriteProgress(string message)
    {
        AnsiConsole.MarkupLine($"  [grey]... {message.EscapeMarkup()}[/]");
    }

    public static void WriteSummary(TestOutcome outcome)
    {
        AnsiConsole.WriteLine();
        AnsiConsole.Write(new Rule("[bold]Test Summary[/]") { Style = Style.Parse("blue") });

        // Steps table
        var stepTable = new Table();
        stepTable.AddColumn("Step");
        stepTable.AddColumn("Result");
        stepTable.AddColumn("Duration");

        foreach (var step in outcome.Steps)
        {
            var status = step.Passed ? "[green]PASS[/]" : "[red]FAIL[/]";
            var duration = step.Duration.TotalSeconds < 60
                ? $"{step.Duration.TotalSeconds:F1}s"
                : $"{step.Duration.TotalMinutes:F1}m";
            stepTable.AddRow(step.StepName.EscapeMarkup(), status, duration);
        }
        AnsiConsole.Write(stepTable);

        // Success criteria
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Success Criteria:[/]");
        foreach (var c in outcome.Criteria)
        {
            var icon = c.Passed ? "[green]✔[/]" : "[red]✘[/]";
            var evidence = c.Passed
                ? $"[grey]({c.Evidence?.EscapeMarkup()})[/]"
                : $"[red]({c.Evidence?.EscapeMarkup()})[/]";
            AnsiConsole.MarkupLine($"  {icon} {c.Description.EscapeMarkup()} {evidence}");
        }

        // Final verdict
        AnsiConsole.WriteLine();
        var totalDuration = outcome.TotalDuration.TotalMinutes < 1
            ? $"{outcome.TotalDuration.TotalSeconds:F0}s"
            : $"{outcome.TotalDuration.TotalMinutes:F1}m";
        AnsiConsole.MarkupLine($"[grey]Total test duration: {totalDuration}[/]");

        var passedSteps = outcome.Steps.Count(s => s.Passed);
        var totalSteps = outcome.Steps.Count;
        var passedCriteria = outcome.Criteria.Count(c => c.Passed);
        var totalCriteria = outcome.Criteria.Count;

        AnsiConsole.MarkupLine($"Steps: {passedSteps}/{totalSteps} passed");
        AnsiConsole.MarkupLine($"Criteria: {passedCriteria}/{totalCriteria} passed");

        if (outcome.OverallPassed)
        {
            AnsiConsole.Write(new FigletText("ALL PASSED").Color(Color.Green));
        }
        else
        {
            AnsiConsole.Write(new FigletText("SOME FAILED").Color(Color.Red));
        }
    }
}
