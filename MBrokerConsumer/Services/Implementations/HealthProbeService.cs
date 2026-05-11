using Microsoft.Extensions.Logging;

namespace MBrokerConsumer.Services.Implementations;

internal class HealthProbeService : IHealthProbeService, IDisposable
{
    private readonly string _readinessPath;
    private readonly string _livenessPath;
    private readonly TimeSpan _livenessInterval;
    private readonly TimeSpan _staleThreshold;
    private readonly ILogger<HealthProbeService> _logger;
    private readonly Timer _timer;
    private DateTime? _lastMessageTime;
    private bool _readinessCreated;
    private bool _stopped;

    public HealthProbeService(
        ILogger<HealthProbeService> logger,
        string readinessPath = "/tmp/consumer-ready",
        string livenessPath = "/tmp/consumer-healthy",
        TimeSpan? livenessInterval = null,
        TimeSpan? staleThreshold = null)
    {
        _logger = logger;
        _readinessPath = readinessPath;
        _livenessPath = livenessPath;
        _livenessInterval = livenessInterval ?? TimeSpan.FromSeconds(5);
        _staleThreshold = staleThreshold ?? TimeSpan.FromSeconds(30);
        _timer = new Timer(_ => OnTimerTick(), null, Timeout.Infinite, Timeout.Infinite);
    }

    public void MarkReady()
    {
        if (_readinessCreated) return;
        _readinessCreated = true;
        File.WriteAllText(_readinessPath, DateTime.UtcNow.ToString("O"));
        _logger.LogInformation("Readiness probe created: {Path}", _readinessPath);
        _timer.Change(TimeSpan.Zero, _livenessInterval);
    }

    public void ReportMessageReceived()
    {
        _lastMessageTime = DateTime.UtcNow;
    }

    public void Stop()
    {
        if (_stopped) return;
        _stopped = true;
        _timer.Change(Timeout.Infinite, Timeout.Infinite);
        try { File.Delete(_readinessPath); } catch { }
        try { File.Delete(_livenessPath); } catch { }
    }

    private void OnTimerTick()
    {
        if (_stopped) return;

        if (_lastMessageTime.HasValue &&
            (DateTime.UtcNow - _lastMessageTime.Value) < _staleThreshold)
        {
            File.WriteAllText(_livenessPath, DateTime.UtcNow.ToString("O"));
        }
        else
        {
            File.Delete(_livenessPath);
            _logger.LogWarning(
                "No messages for {Threshold}s — liveness probe failing",
                _staleThreshold.TotalSeconds);
        }
    }

    public void Dispose()
    {
        _timer?.Dispose();
    }
}