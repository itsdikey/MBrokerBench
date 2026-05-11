using System.Diagnostics;
using Spectre.Console;

namespace MBrokerBench.Phase2Tests;

/// <summary>Run shell commands and manage background processes.</summary>
public class ProcessRunner
{
    /// <summary>Run a command to completion and return captured output.</summary>
    public static async Task<(int ExitCode, string StdOut, string StdErr)> RunCommandAsync(
        string fileName, string arguments, CancellationToken ct = default, string? workingDir = null)
    {
        var psi = new ProcessStartInfo
        {
            FileName = fileName,
            Arguments = arguments,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = workingDir ?? Directory.GetCurrentDirectory()
        };

        using var process = new Process { StartInfo = psi };
        var outBuilder = new System.Text.StringBuilder();
        var errBuilder = new System.Text.StringBuilder();

        process.OutputDataReceived += (_, e) => { if (e.Data != null) outBuilder.AppendLine(e.Data); };
        process.ErrorDataReceived += (_, e) => { if (e.Data != null) errBuilder.AppendLine(e.Data); };

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        await process.WaitForExitAsync(ct);

        return (process.ExitCode, outBuilder.ToString().TrimEnd(), errBuilder.ToString().TrimEnd());
    }

    /// <summary>Run a "just" command via PowerShell.</summary>
    public static async Task<(int ExitCode, string StdOut, string StdErr)> RunJustAsync(
        string target, CancellationToken ct = default)
    {
        AnsiConsole.MarkupLine($"  [grey]just {target}[/]");
        var result = await RunPowerShellAsync($"just {target}", ct);
        return result;
    }

    /// <summary>Run a PowerShell command.</summary>
    public static async Task<(int ExitCode, string StdOut, string StdErr)> RunPowerShellAsync(
        string command, CancellationToken ct = default)
    {
        return await RunCommandAsync("powershell.exe",
            $"-NoProfile -Command \"{command.Replace("\"", "\\\"")}\"", ct);
    }

    /// <summary>Run a kubectl command.</summary>
    public static async Task<(int ExitCode, string StdOut, string StdErr)> RunKubectlAsync(
        string args, CancellationToken ct = default)
    {
        return await RunCommandAsync("kubectl", args, ct);
    }

    /// <summary>Start a long-running background process.</summary>
    public static BackgroundProcess StartBackground(string fileName, string arguments,
        string label, string? workingDir = null)
    {
        var psi = new ProcessStartInfo
        {
            FileName = fileName,
            Arguments = arguments,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = workingDir ?? Directory.GetCurrentDirectory()
        };

        var process = new Process { StartInfo = psi };
        var bg = new BackgroundProcess { Label = label, Process = process };

        process.OutputDataReceived += (_, e) =>
        {
            if (e.Data != null)
            {
                lock (bg.OutputLines) bg.OutputLines.Add(e.Data);
            }
        };
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data != null)
            {
                lock (bg.OutputLines) bg.OutputLines.Add($"[ERR] {e.Data}");
            }
        };

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        return bg;
    }

    /// <summary>Start a PowerShell command in the background.</summary>
    public static BackgroundProcess StartBackgroundPowerShell(string command, string label)
    {
        return StartBackground("powershell.exe",
            $"-NoProfile -Command \"{command.Replace("\"", "\\\"")}\"", label);
    }
}
