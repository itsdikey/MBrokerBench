using Confluent.Kafka;
using MBrokerConsumer.Models;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.Threading;

namespace MBrokerConsumer.Services.Implementations;

internal sealed class MainProgram : IMainProgram
{
    private enum ManualAssignmentLoadStatus
    {
        MissingOrUnreadable,
        Present
    }

    private sealed class ManualAssignmentLoadResult
    {
        public ManualAssignmentLoadStatus Status { get; init; }
        public List<TopicPartition> Partitions { get; init; } = new();
    }

    private readonly ILogger<Program> _logger;
    private readonly ConsumerEnvConfig _envConfig;
    private readonly TokenBucketLimiter _rateLimiter;
    private readonly IHealthProbeService _healthProbe;
    private readonly ICommitTracker _commitTracker;
    private readonly IRateLogger _rateLogger;

    private readonly CancellationTokenSource _cancellationToken = new();
    private readonly ManualResetEventSlim _drainComplete = new(false);

    // Manual assignment state — only used when ManualPartitionAssignmentEnabled = true
    private List<TopicPartition>? _currentAssignment;
    private DateTime _lastAssignmentReload = DateTime.MinValue;
    private readonly object _assignmentLock = new();

    public MainProgram(
        ILogger<Program> logger,
        ConsumerEnvConfig envConfig,
        TokenBucketLimiter rateLimiter,
        ITerminationService terminationService,
        IHealthProbeService healthProbe,
        ICommitTracker commitTracker,
        IRateLogger rateLogger
        )
    {
        _logger = logger;
        _envConfig = envConfig;
        _rateLimiter = rateLimiter;
        _healthProbe = healthProbe;
        _commitTracker = commitTracker;
        _rateLogger = rateLogger;
        terminationService.TerminationRequested += () =>
        {
            _logger.LogInformation("Termination requested. Initiating shutdown...");
            OnTerminate();
        };
    }

    private void OnTerminate()
    {
        _cancellationToken.Cancel();
        _logger.LogInformation("Draining in-flight messages for up to {Seconds}s...", _envConfig.DrainTimeoutSeconds);

        if (!_drainComplete.Wait(TimeSpan.FromSeconds(_envConfig.DrainTimeoutSeconds)))
        {
            _logger.LogWarning("Drain timeout exceeded ({Seconds}s). Forcing exit.", _envConfig.DrainTimeoutSeconds);
            Environment.Exit(1);
        }
    }

    public async Task Run()
    {
        if (!ValidateConfig(_envConfig))
        {
            _logger.LogError("Invalid configuration. Please check environment variables.");
            return;
        }

        LogConfig(_envConfig);

        var loggingWindowLimiter = new LoggingWindowLimter(TimeSpan.FromMilliseconds(1000));

        var config = _envConfig.ToConsumerConfig();

        int consecutiveNullPolls = 0;
        var memberId = string.Empty;

        using var consumer = new ConsumerBuilder<Ignore, byte[]>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka Error: {Reason}", e.Reason))
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogInformation(
                    "[ASSIGN] Partitions assigned: {Partitions} (member-id: {MemberId})",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")),
                    c.MemberId);
                memberId = c.MemberId ?? string.Empty;
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                _logger.LogInformation(
                    "[REVOKE] Partitions revoked: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .SetPartitionsLostHandler((c, partitions) =>
            {
                _logger.LogWarning(
                    "[LOST] Partitions lost: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .Build();

        // ----------------------------------------------------------------
        // Manual or group-based partition initialization
        // ----------------------------------------------------------------
        if (_envConfig.ManualPartitionAssignmentEnabled)
        {
            // Manual mode: validate PodName, wait for assignment entry, use Assign()
            if (string.IsNullOrWhiteSpace(_envConfig.PodName))
            {
                _logger.LogError(
                    "[MANUAL] PodName is required when MANUAL_PARTITION_ASSIGNMENT_ENABLED=true. " +
                    "Set the POD_NAME environment variable.");
                Environment.Exit(1);
            }

            _logger.LogInformation(
                "[MANUAL] Manual partition assignment enabled. ConfigMap path: {Path}, Poll interval: {Interval}s, Startup timeout: {Timeout}s, PodName: {Pod}",
                _envConfig.AssignmentConfigMapPath,
                _envConfig.AssignmentPollIntervalSeconds,
                _envConfig.AssignmentStartupTimeoutSeconds,
                _envConfig.PodName);

            // Bounded wait at startup — keep retrying until the pod appears in the ConfigMap
            // or the timeout expires. This prevents crash-loops when the controller hasn't
            // published assignments yet (pods may still be Pending).
            ManualAssignmentLoadResult? initialAssignment = null;
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(_envConfig.AssignmentStartupTimeoutSeconds);

            while (DateTime.UtcNow < deadline)
            {
                initialAssignment = LoadAndApplyManualAssignment(consumer, _envConfig.PodName, _envConfig.AssignmentConfigMapPath);
                if (initialAssignment.Status == ManualAssignmentLoadStatus.Present)
                    break;

                _logger.LogInformation(
                    "[MANUAL] Pod '{Pod}' not yet in ConfigMap (still waiting {Remaining}s)...",
                    _envConfig.PodName,
                    (deadline - DateTime.UtcNow).TotalSeconds);
                Thread.Sleep(TimeSpan.FromSeconds(2));
            }

            if (initialAssignment == null || initialAssignment.Status != ManualAssignmentLoadStatus.Present)
            {
                _logger.LogError(
                    "[MANUAL] Startup timeout ({Timeout}s) expired: pod '{Pod}' never appeared in ConfigMap '{Path}'. Exiting.",
                    _envConfig.AssignmentStartupTimeoutSeconds,
                    _envConfig.PodName,
                    _envConfig.AssignmentConfigMapPath);
                Environment.Exit(1);
            }

            _logger.LogInformation(
                "[MANUAL] Startup assignment acquired for pod '{Pod}': {Partitions}",
                _envConfig.PodName,
                initialAssignment.Partitions.Any()
                    ? string.Join(", ", initialAssignment.Partitions.Select(tp => tp.Partition.Value))
                    : "<none>");
        }
        else
        {
            // Normal group-based Subscribe path
            consumer.Subscribe(_envConfig.Topic);
            _logger.LogInformation("Subscription to topic '{Topic}' succeeded.", _envConfig.Topic);
        }

        var consumptionCounter = new ConsumptionCounter();

        try
        {
            while (!_cancellationToken.IsCancellationRequested)
            {
                try
            {
                await _rateLimiter.ConsumeAsync();

                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));

                    if (consumeResult != null)
                    {
                        consecutiveNullPolls = 0;
                        _healthProbe.MarkReady();
                        _healthProbe.ReportMessageReceived();
                        consumptionCounter.Increment();

                        _logger.LogDebug(
                            "[CONSUME] topic={Topic} partition={Partition} offset={Offset} timestamp={Timestamp} assign-count={AssignCount}",
                            consumeResult.Topic,
                            consumeResult.Partition.Value,
                            consumeResult.Offset.Value,
                            consumeResult.Message.Timestamp.UnixTimestampMs,
                            consumer.Assignment.Count);

                        if (loggingWindowLimiter.ShouldLog(out var timePassed))
                        {
                            _logger.LogInformation(
                                "Consumed {Count} msgs in {Elapsed:F2}s. Total: {Total}",
                                consumptionCounter.CurrentWindowCount, timePassed.TotalSeconds, consumptionCounter.TotalCount);
                            consumptionCounter.ResetWindow();
                        }

                        _commitTracker.TryCommit(() => consumer.Commit());
                        _rateLogger.MessageConsumed();

                        // Log rate + estimated lag every 10s
                        if (_rateLogger.ShouldLog)
                        {
                            long totalLag = 0;
                            foreach (var tp in consumer.Assignment)
                            {
                                try
                                {
                                    var watermarks = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(2));
                                    var position = consumer.Position(tp);
                                    totalLag += Math.Max(0, watermarks.High.Value - position.Value);
                                }
                                catch
                                {
                                    // skip partitions where lag can't be computed yet
                                }
                            }

                            _rateLogger.TryLogRateAndLag(totalLag, consumer.Assignment.Count);
                        }
                    }
                    else
                    {
                        consecutiveNullPolls++;
                        // Warn every ~100 null polls (~10s at 100ms poll interval) if nothing received
                        if (consecutiveNullPolls >= 100)
                        {
                            _logger.LogWarning(
                                "[NULLPOLL] No messages fetched for {Polls} polls (~{Seconds}s). assign-count={AssignCount} member-id={MemberId}",
                                consecutiveNullPolls,
                                consecutiveNullPolls / 10,
                                consumer.Assignment.Count,
                                memberId);
                            consecutiveNullPolls = 0;
                        }
                    }

                    // ----------------------------------------------------------------
                    // Periodic manual assignment reload (manual mode only)
                    // ----------------------------------------------------------------
                    if (_envConfig.ManualPartitionAssignmentEnabled)
                    {
                        var pollInterval = TimeSpan.FromSeconds(_envConfig.AssignmentPollIntervalSeconds);
                        if (DateTime.UtcNow - _lastAssignmentReload >= pollInterval)
                        {
                            _lastAssignmentReload = DateTime.UtcNow;
                            var refreshed = LoadAndApplyManualAssignment(consumer, _envConfig.PodName, _envConfig.AssignmentConfigMapPath);
                            if (refreshed.Status == ManualAssignmentLoadStatus.MissingOrUnreadable)
                            {
                                _logger.LogWarning(
                                    "[MANUAL] Could not reload assignment file '{Path}' for pod '{Pod}'. Keeping current assignment.",
                                    _envConfig.AssignmentConfigMapPath,
                                    _envConfig.PodName);
                            }
                        }
                    }
                }
                catch (ConsumeException e)
                {
                    _logger.LogError("Error occurred: {Reason}", e.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Closing consumer...");
        }
        finally
        {
            _commitTracker.ForceCommit(() => consumer.Commit());

            if (consumptionCounter.CurrentWindowCount > 0)
            {
                _logger.LogInformation(
                    "Final batch: {Count} msgs in {Elapsed:F2}s. Total: {Total}",
                    consumptionCounter.CurrentWindowCount, loggingWindowLimiter.LastTime.TotalSeconds, consumptionCounter.TotalCount);
            }

            _logger.LogInformation(
                "Shutdown complete — committed offsets, {Total} messages processed",
                consumptionCounter.TotalCount);

            _healthProbe.Stop();

            consumer.Close();
            _drainComplete.Set();
        }
    }

    private void LogConfig(ConsumerEnvConfig envConfig)
    {
        _logger.Log(envConfig.GetLogLevel(), "MBrokerConsumer starting...");
        _logger.Log(envConfig.GetLogLevel(), "Bootstrap Servers: {Servers}", envConfig.BootstrapServers);
        _logger.Log(envConfig.GetLogLevel(), "Topic: {Topic}", envConfig.Topic);
        _logger.Log(envConfig.GetLogLevel(), "Group ID: {GroupId}", envConfig.GroupId);
        _logger.Log(envConfig.GetLogLevel(), "Max Rate Limit: {Rate} msgs/s", envConfig.MaxRateLimit);
        _logger.Log(envConfig.GetLogLevel(), "Consumer Profile: {Profile}", envConfig.ConsumerProfile);
        _logger.Log(envConfig.GetLogLevel(), "Log Level: {Level}", envConfig.LogLevel);
        _logger.Log(envConfig.GetLogLevel(), "Drain Timeout Seconds: {Seconds}", envConfig.DrainTimeoutSeconds);
        _logger.Log(envConfig.GetLogLevel(), "Commit Interval Seconds: {Seconds}", envConfig.CommitIntervalSeconds);
        if (envConfig.ManualPartitionAssignmentEnabled)
        {
            _logger.Log(envConfig.GetLogLevel(), "Manual Partition Assignment: ENABLED");
            _logger.Log(envConfig.GetLogLevel(), "  ConfigMap path: {Path}", envConfig.AssignmentConfigMapPath);
            _logger.Log(envConfig.GetLogLevel(), "  Poll interval: {Interval}s", envConfig.AssignmentPollIntervalSeconds);
            _logger.Log(envConfig.GetLogLevel(), "  Pod name: {Pod}", envConfig.PodName);
        }
    }

    private bool ValidateConfig(ConsumerEnvConfig? envConfig)
    {
        if (envConfig == null)
            return false;
        if (string.IsNullOrEmpty(envConfig.BootstrapServers))
        {
            _logger.LogError("BootstrapServers is required.");
            return false;
        }
        if (string.IsNullOrEmpty(envConfig.Topic))
        {
            _logger.LogError("Topic is required.");
            return false;
        }
        if (string.IsNullOrEmpty(envConfig.GroupId))
        {
            _logger.LogError("GroupId is required.");
            return false;
        }
        if (envConfig.MaxRateLimit <= 0)
        {
            _logger.LogError("MaxRateLimit must be greater than 0.");
            return false;
        }
        return true;
    }

    /// <summary>
    /// Reads the JSON assignment file from the ConfigMap mount path, looks up the
    /// partition list for the given pod, and if it differs from the consumer's
    /// current assignment, commits offsets and reassigns.
    /// Returns whether the pod entry was present, and the pod's partition list.
    /// Missing/unreadable assignment data is reported as MissingOrUnreadable.
    /// </summary>
    private ManualAssignmentLoadResult LoadAndApplyManualAssignment(
        IConsumer<Ignore, byte[]> consumer,
        string podName,
        string configMapPath)
    {
        if (!File.Exists(configMapPath))
        {
            return new ManualAssignmentLoadResult { Status = ManualAssignmentLoadStatus.MissingOrUnreadable };
        }

        string json;
        try
        {
            json = File.ReadAllText(configMapPath);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[MANUAL] Failed to read assignment file '{Path}': {Msg}", configMapPath, ex.Message);
            return new ManualAssignmentLoadResult { Status = ManualAssignmentLoadStatus.MissingOrUnreadable };
        }

        Dictionary<string, int[]>? assignmentMap;
        try
        {
            assignmentMap = JsonSerializer.Deserialize<Dictionary<string, int[]>>(json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[MANUAL] Failed to parse assignment JSON from '{Path}': {Msg}", configMapPath, ex.Message);
            return new ManualAssignmentLoadResult { Status = ManualAssignmentLoadStatus.MissingOrUnreadable };
        }

        if (assignmentMap == null || !assignmentMap.TryGetValue(podName, out var partitionIds))
        {
            return new ManualAssignmentLoadResult { Status = ManualAssignmentLoadStatus.MissingOrUnreadable };
        }

        var newAssignment = partitionIds
            .Distinct()
            .Select(pid => new TopicPartition(_envConfig.Topic, new Partition(pid)))
            .OrderBy(tp => tp.Partition.Value)
            .ToList();

        lock (_assignmentLock)
        {
            var current = consumer.Assignment;
            var currentSet = current.ToHashSet();
            var newSet = newAssignment.ToHashSet();

            if (currentSet.SetEquals(newSet))
            {
                // No change
                return new ManualAssignmentLoadResult
                {
                    Status = ManualAssignmentLoadStatus.Present,
                    Partitions = newAssignment
                };
            }

            _logger.LogInformation(
                "[MANUAL] Assignment changed for pod '{Pod}': was {Old}, now {New}. Committing and reassigning.",
                podName,
                string.Join(",", current.OrderBy(tp => tp.Partition.Value).Select(tp => tp.Partition.Value)),
                string.Join(",", newAssignment.Select(tp => tp.Partition.Value)));

            try
            {
                consumer.Commit();
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[MANUAL] Commit during reassignment failed: {Msg}", ex.Message);
            }

            if (newAssignment.Count > 0)
            {
                consumer.Assign(newAssignment);
            }
            else
            {
                consumer.Unassign();
            }

            _logger.LogInformation(
                "[MANUAL] Assigned partitions: {Partitions}",
                newAssignment.Any()
                    ? string.Join(", ", newAssignment.Select(tp => tp.Partition.Value))
                    : "<none>");

            _currentAssignment = newAssignment;
            return new ManualAssignmentLoadResult
            {
                Status = ManualAssignmentLoadStatus.Present,
                Partitions = newAssignment
            };
        }
    }
}
