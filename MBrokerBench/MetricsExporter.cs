using Prometheus;

namespace MBrokerBench
{
    public static class MetricsExporter
    {
        private static readonly object _lock = new object();
        private static KestrelMetricServer? _server;
        private static MetricPusher? _metricPusher;
        // Core metrics
        private static Gauge? ConsumersGauge;
        private static Gauge? TotalLagGauge;
        private static Counter? ScaleUpCounter;
        private static Counter? ScaleDownCounter;

        // Partition metrics (label: partition)
        private static Gauge? PartitionLagGauge;
        private static Gauge? PartitionRateGauge;

        // Partition -> assigned consumer mapping to clear previous label metric
        private static readonly Dictionary<string, string?> _partitionAssignedConsumer = new();
        private static Gauge? PartitionAssignedGauge; // labels: partition, consumer

        // Consumer metrics (label: consumer)
        private static Gauge? ConsumerUtilizationGauge;
        private static Gauge? ConsumerAssignedCountGauge;

        public static void Init(int port = 1234)
        {
            try
            {
                ConsumersGauge = Metrics.CreateGauge("consumers_total", "Number of active consumers");
                TotalLagGauge = Metrics.CreateGauge("total_system_lag_messages", "Total system lag in messages");
                ScaleUpCounter = Metrics.CreateCounter("scale_up_total", "Total number of scale-up events");
                ScaleDownCounter = Metrics.CreateCounter("scale_down_total", "Total number of scale-down events");

                PartitionLagGauge = Metrics.CreateGauge("partition_lag_messages", "Partition lag in messages", new GaugeConfiguration { LabelNames = new[] { "partition" } });
                PartitionRateGauge = Metrics.CreateGauge("partition_production_rate_msgs_per_sec", "Partition production rate (msgs/sec)", new GaugeConfiguration { LabelNames = new[] { "partition" } });
                PartitionAssignedGauge = Metrics.CreateGauge("partition_assigned_consumer", "Partition assigned consumer (1 if assigned)", new GaugeConfiguration { LabelNames = new[] { "partition", "consumer" } });

                ConsumerUtilizationGauge = Metrics.CreateGauge("consumer_utilization_percent", "Per-consumer utilization in percent", new GaugeConfiguration { LabelNames = new[] { "consumer" } });
                ConsumerAssignedCountGauge = Metrics.CreateGauge("consumer_assigned_partitions_count", "Number of partitions assigned to a consumer", new GaugeConfiguration { LabelNames = new[] { "consumer" } });
                _server = new KestrelMetricServer(hostname: "0.0.0.0", port: port);
                _server.Start();
                Console.WriteLine($"Prometheus metric server started on http://0.0.0.0:{port}/metrics (accessible from host at http://localhost:{port}/metrics)");

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
            ConsumersGauge?.Set(count);
        }

        public static void SetTotalLag(long lag)
        {
            TotalLagGauge?.Set(lag);
        }

        public static void SetPartition(string id, long lag, double rate)
        {
            PartitionLagGauge?.WithLabels(id).Set(lag);
            PartitionRateGauge?.WithLabels(id).Set(rate);
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
                    PartitionAssignedGauge?.WithLabels(partitionId, prev).Set(0);
                }

                if (!string.IsNullOrEmpty(consumerId))
                {
                    PartitionAssignedGauge?.WithLabels(partitionId, consumerId).Set(1);
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
            ConsumerUtilizationGauge?.WithLabels(consumerId).Set(utilizationPercent);
            ConsumerAssignedCountGauge?.WithLabels(consumerId).Set(assignedCount);
        }

        public static void IncScaleUp()
        {
            ScaleUpCounter?.Inc();
        }

        public static void IncScaleDown()
        {
            ScaleDownCounter?.Inc();
        }
    }
}
