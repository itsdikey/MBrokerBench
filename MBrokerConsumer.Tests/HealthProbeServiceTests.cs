using MBrokerConsumer.Services;
using MBrokerConsumer.Services.Implementations;
using Microsoft.Extensions.Logging.Abstractions;

namespace MBrokerConsumer.Tests;

public class HealthProbeServiceTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _readyPath;
    private readonly string _healthyPath;

    public HealthProbeServiceTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDir);
        _readyPath = Path.Combine(_testDir, "ready");
        _healthyPath = Path.Combine(_testDir, "healthy");
    }

    [Fact]
    public void MarkReady_CreatesReadinessFile()
    {
        using var service = new HealthProbeService(
            NullLogger<HealthProbeService>.Instance,
            _readyPath, _healthyPath,
            livenessInterval: TimeSpan.FromMilliseconds(50));

        service.MarkReady();

        Assert.True(File.Exists(_readyPath));
    }

    [Fact]
    public void ReportMessageReceived_DoesNotCreateFiles()
    {
        using var service = new HealthProbeService(
            NullLogger<HealthProbeService>.Instance,
            _readyPath, _healthyPath);

        service.ReportMessageReceived();

        Assert.False(File.Exists(_readyPath));
        Assert.False(File.Exists(_healthyPath));
    }

    [Fact]
    public void LivenessFile_CreatedAfterMarkReadyAndReportMessageReceived()
    {
        using var service = new HealthProbeService(
            NullLogger<HealthProbeService>.Instance,
            _readyPath, _healthyPath,
            livenessInterval: TimeSpan.FromMilliseconds(50));

        service.MarkReady();
        service.ReportMessageReceived();
        Thread.Sleep(200);

        Assert.True(File.Exists(_healthyPath));
    }

    [Fact]
    public void LivenessFile_DeletedWhenNoRecentMessages()
    {
        using var service = new HealthProbeService(
            NullLogger<HealthProbeService>.Instance,
            _readyPath, _healthyPath,
            livenessInterval: TimeSpan.FromMilliseconds(50),
            staleThreshold: TimeSpan.FromMilliseconds(1));

        service.MarkReady();
        // Don't call ReportMessageReceived — lastMessageTime stays null
        Thread.Sleep(200);

        Assert.False(File.Exists(_healthyPath));
    }

    [Fact]
    public void Stop_CleansUpProbeFiles()
    {
        var service = new HealthProbeService(
            NullLogger<HealthProbeService>.Instance,
            _readyPath, _healthyPath);
        service.MarkReady();

        service.Stop();

        Assert.False(File.Exists(_readyPath));
        Assert.False(File.Exists(_healthyPath));
        service.Dispose();
    }

    [Fact]
    public void Stop_IsIdempotent()
    {
        var service = new HealthProbeService(
            NullLogger<HealthProbeService>.Instance,
            _readyPath, _healthyPath);
        service.MarkReady();

        service.Stop();
        service.Stop(); // Should not throw

        Assert.False(File.Exists(_readyPath));
        service.Dispose();
    }

    public void Dispose()
    {
        try { Directory.Delete(_testDir, true); } catch { }
    }
}