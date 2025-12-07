using Prometheus;
using System.Threading.Tasks;

namespace MBrokerBench
{
    public static class MetricsExporter
    {
        private static readonly object _lock = new object();
        private static KestrelMetricServer? _server;
        private static MetricPusher? _metricPusher;

        // Default grouping labels for experiments
        private static string _strategy = "unknown";
        private static string _runId = "unknown";

        // Core metrics
        private static Gauge? ConsumersGauge;
        private static Gauge? TotalLagGauge;
        private static Counter? ScaleUpCounter;
        private static Counter? ScaleDownCounter;

        // Partition metrics (label: partition)
        private static Gauge? PartitionLagGauge;
        private static Gauge? PartitionRateGauge;

        // Total production rate metric
        private static Gauge? TotalProductionRateGauge;

        // Partition reassignment metrics
        private static Counter? PartitionReassignmentsCounter; // labeled by strategy/run
        private static Counter? PartitionReassignmentsPerPartitionCounter; // label: partition, strategy, run

        // Partition -> assigned consumer mapping to clear previous label metric
        private static readonly Dictionary<string, string?> _partitionAssignedConsumer = new();
        private static Gauge? PartitionAssignedGauge; // labels: partition, consumer, strategy, run

        // Consumer metrics (label: consumer)
        private static Gauge? ConsumerUtilizationGauge;
        private static Gauge? ConsumerAssignedCountGauge;

        public static void Init(int port = 1234, string? strategy = null, string? runId = null)
        {
            try
            {
                // Determine default labels
                _strategy = strategy ?? Environment.GetEnvironmentVariable("STRATEGY") ?? "unknown";
                _runId = runId ?? Environment.GetEnvironmentVariable("RUN_ID") ?? Guid.NewGuid().ToString();

                ConsumersGauge = Metrics.CreateGauge("consumers_total", "Number of active consumers", new GaugeConfiguration { LabelNames = new[] { "strategy", "run" } });
                TotalLagGauge = Metrics.CreateGauge("total_system_lag_messages", "Total system lag in messages", new GaugeConfiguration { LabelNames = new[] { "strategy", "run" } });
                ScaleUpCounter = Metrics.CreateCounter("scale_up_total", "Total number of scale-up events", new CounterConfiguration { LabelNames = new[] { "strategy", "run" } });
                ScaleDownCounter = Metrics.CreateCounter("scale_down_total", "Total number of scale-down events", new CounterConfiguration { LabelNames = new[] { "strategy", "run" } });

                PartitionLagGauge = Metrics.CreateGauge("partition_lag_messages", "Partition lag in messages", new GaugeConfiguration { LabelNames = new[] { "partition", "strategy", "run" } });
                PartitionRateGauge = Metrics.CreateGauge("partition_production_rate_msgs_per_sec", "Partition production rate (msgs/sec)", new GaugeConfiguration { LabelNames = new[] { "partition", "strategy", "run" } });
                PartitionAssignedGauge = Metrics.CreateGauge("partition_assigned_consumer", "Partition assigned consumer (1 if assigned)", new GaugeConfiguration { LabelNames = new[] { "partition", "consumer", "strategy", "run" } });

                // Total production rate
                TotalProductionRateGauge = Metrics.CreateGauge("total_system_production_rate_msgs_per_sec", "Total system production rate (msgs/sec)", new GaugeConfiguration { LabelNames = new[] { "strategy", "run" } });

                // Create reassignment counters
                PartitionReassignmentsCounter = Metrics.CreateCounter("partition_reassignments_total", "Total number of partition reassignments to a different consumer", new CounterConfiguration { LabelNames = new[] { "strategy", "run" } });
                PartitionReassignmentsPerPartitionCounter = Metrics.CreateCounter("partition_reassignments", "Partition reassignments by partition", new CounterConfiguration { LabelNames = new[] { "partition", "strategy", "run" } });

                ConsumerUtilizationGauge = Metrics.CreateGauge("consumer_utilization_percent", "Per-consumer utilization in percent", new GaugeConfiguration { LabelNames = new[] { "consumer", "strategy", "run" } });
                ConsumerAssignedCountGauge = Metrics.CreateGauge("consumer_assigned_partitions_count", "Number of partitions assigned to a consumer", new GaugeConfiguration { LabelNames = new[] { "consumer", "strategy", "run" } });
                _server = new KestrelMetricServer(hostname: "0.0.0.0", port: port);
                _server.Start();
                Console.WriteLine($"Prometheus metric server started on http://0.0.0.0:{port}/metrics (strategy={_strategy} run={_runId})");

                var pushUrl = Environment.GetEnvironmentVariable("PUSHGATEWAY_URL");
                if (!string.IsNullOrEmpty(pushUrl))
                {
                    try
                    {
                        var pusher = new MetricPusher(new MetricPusherOptions
                        {
                            Endpoint = pushUrl,
                            Job = "mbroker"
                        });

                        pusher.Start();
                        _metricPusher = pusher;
                        Console.WriteLine($"MetricPushServer started to {pushUrl}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to start MetricPushServer: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to start prometheus-net metrics server: {ex.Message}");
            }
        }

        public static async Task Stop()
        {
            try
            {
                _metricPusher?.Stop();
            }
            catch { }
            _metricPusher = null;
            _server?.Stop();
            _server = null;
        }

        public static void SetConsumers(int count)
        {
            ConsumersGauge?.WithLabels(_strategy, _runId).Set(count);
        }

        public static void SetTotalLag(long lag)
        {
            TotalLagGauge?.WithLabels(_strategy, _runId).Set(lag);
        }

        public static void SetTotalProductionRate(double rate)
        {
            TotalProductionRateGauge?.WithLabels(_strategy, _runId).Set(rate);
        }

        public static void SetPartition(string id, long lag, double rate)
        {
            PartitionLagGauge?.WithLabels(id, _strategy, _runId).Set(lag);
            PartitionRateGauge?.WithLabels(id, _strategy, _runId).Set(rate);
            lock (_lock)
            {
                if (!_partitionAssignedConsumer.ContainsKey(id))
                    _partitionAssignedConsumer[id] = null;
            }
        }

        public static void SetPartitionAssignment(string partitionId, string? consumerId)
        {
            lock (_lock)
            {
                // Clear previous assignment gauge if different
                if (_partitionAssignedConsumer.TryGetValue(partitionId, out var prev) && !string.IsNullOrEmpty(prev) && prev != consumerId)
                {
                    PartitionAssignedGauge?.WithLabels(partitionId, prev, _strategy, _runId).Set(0);

                    // Increment reassignment counters only when switching from one consumer to a different non-null consumer
                    if (!string.IsNullOrEmpty(consumerId))
                    {
                        PartitionReassignmentsCounter?.WithLabels(_strategy, _runId).Inc();
                        PartitionReassignmentsPerPartitionCounter?.WithLabels(partitionId, _strategy, _runId).Inc();
                    }
                }

                if (!string.IsNullOrEmpty(consumerId))
                {
                    PartitionAssignedGauge?.WithLabels(partitionId, consumerId, _strategy, _runId).Set(1);
                    _partitionAssignedConsumer[partitionId] = consumerId;
                }
                else
                {
                    _partitionAssignedConsumer[partitionId] = null;
                }
            }
        }

        public static void SetConsumerMetrics(string consumerId, double utilizationPercent, int assignedCount)
        {
            ConsumerUtilizationGauge?.WithLabels(consumerId, _strategy, _runId).Set(utilizationPercent);
            ConsumerAssignedCountGauge?.WithLabels(consumerId, _strategy, _runId).Set(assignedCount);
        }

        public static void IncScaleUp()
        {
            ScaleUpCounter?.WithLabels(_strategy, _runId).Inc();
        }

        public static void IncScaleDown()
        {
            ScaleDownCounter?.WithLabels(_strategy, _runId).Inc();
        }

        public static async Task Finalizer()
        {
            await PrometheusExporter.ExportAllMetricsAsync(
                prometheusUrl: Environment.GetEnvironmentVariable("PROMETHEUS_URL") ?? "http://localhost:9090",
                strategy: _strategy,
                runId: _runId,
                startUtc: DateTime.UtcNow.AddHours(-1),
                endUtc: DateTime.UtcNow,
                step: "1s");
        }
    }
}
