using Confluent.Kafka;
using Confluent.Kafka.Admin;
using MBrokerBench.Components;
using System.Text.Json;

namespace MBrokerBench.DataProviders
{
    public sealed class KafkaDataProvider : IDataProvider, IDisposable
    {
        private readonly string _bootstrapServers;
        private readonly string _prometheusUrl;
        private readonly string _topic;
        private readonly string _consumerGroup;
        private readonly HttpClient _httpClient = new();
        private readonly IConsumer<Ignore, Ignore> _pollConsumer;

        public int MaxRuntimeSteps { get; } = int.MaxValue; // Run until stopped

        private readonly Dictionary<int, long> _previousOffsets = new();
        private DateTime _lastUpdateTime = DateTime.MinValue;

        // Committed-offset tracking for observed real consumption rate
        private readonly Dictionary<int, long> _previousCommittedOffsets = new();
        private long _lastTotalCommittedOffset = 0;
        private DateTime _lastCommittedOffsetSampleTime = DateTime.MinValue;

        /// <summary>
        /// Controller-observed real consumption rate in msgs/s, derived from committed-offset
        /// deltas between Process() calls. Returns 0 until a valid second sample exists.
        /// </summary>
        public double ObservedConsumptionRate { get; private set; }

        private readonly IAdminClient _adminClient;

        public KafkaDataProvider(string bootstrapServers, string prometheusUrl, string topic, string consumerGroup)
        {
            _bootstrapServers = bootstrapServers;
            _prometheusUrl = prometheusUrl;
            _topic = topic;
            _consumerGroup = consumerGroup;

            var config = new ConsumerConfig 
            { 
                BootstrapServers = _bootstrapServers,
                GroupId = "mbroker-bench-poll-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
            _pollConsumer = new ConsumerBuilder<Ignore, Ignore>(config).Build();

            var adminConfig = new AdminClientConfig { BootstrapServers = _bootstrapServers };
            _adminClient = new AdminClientBuilder(adminConfig).Build();
        }

        public List<Components.Partition> InitializePartitions()
        {
            try
            {
                var metadata = _adminClient.GetMetadata(_topic, TimeSpan.FromSeconds(10));
                var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == _topic);

                if (topicMetadata == null)
                {
                    throw new Exception($"Topic '{_topic}' not found in Kafka.");
                }

                var partitions = topicMetadata.Partitions
                    .Select(p => new Components.Partition(p.PartitionId.ToString()))
                    .ToList();

                // Initialize offsets
                foreach (var p in partitions)
                {
                    int pid = int.Parse(p.Id);
                    try {
                        var watermark = _pollConsumer.QueryWatermarkOffsets(new TopicPartition(_topic, pid), TimeSpan.FromSeconds(5));
                        _previousOffsets[pid] = watermark.High.Value;
                    } catch { _previousOffsets[pid] = 0; }

                    // Seed committed-offset tracking so the first Process() call has a baseline
                    _previousCommittedOffsets[pid] = 0;
                }

                return partitions;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error initializing Kafka partitions: {ex.Message}");
                return Enumerable.Range(0, 6).Select(i => new Components.Partition(i.ToString())).ToList();
            }
        }

        public List<Components.Partition> Process(List<Components.Partition> partitions, int timeStep)
        {
            var now = DateTime.UtcNow;
            if ((now - _lastUpdateTime).TotalSeconds < 1.0)
            {
                return partitions;
            }

            _lastUpdateTime = now;

            try
            {
                // Fetch all committed offsets for the target group in one go
                var groupOffsets = _adminClient.ListConsumerGroupOffsetsAsync(new List<ConsumerGroupTopicPartitions>
                {
                    new ConsumerGroupTopicPartitions(_consumerGroup, null)
                }).Result;

                var offsetMap = groupOffsets.First().Partitions.ToDictionary(p => p.Partition.Value, p => p.Offset.Value);

                // Accumulate committed offset deltas for ObservedConsumptionRate
                long totalCurrentCommitted = 0;

                foreach (var partition in partitions)
                {
                    int partitionId = int.Parse(partition.Id);

                    try
                    {
                        // Query Kafka directly for the Log End Offset (High Watermark)
                        var topicPartition = new TopicPartition(_topic, new Confluent.Kafka.Partition(partitionId));
                        var watermark = _pollConsumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(2));
                        long currentRealOffset = watermark.High.Value;

                        // Calculate Lag: difference between high watermark and committed offset.
                        // If the consumer group has no committed offset for this partition (e.g.,
                        // brand new group that hasn't consumed anything), ALL messages are unread.
                        if (offsetMap.TryGetValue(partitionId, out long committedOffset))
                        {
                            if (committedOffset < 0) committedOffset = 0;
                            partition.CurrentLag = Math.Max(0, currentRealOffset - committedOffset);
                            totalCurrentCommitted += committedOffset;
                        }
                        else if (currentRealOffset > 0)
                        {
                            // No committed offset means consumer group has never consumed from
                            // this partition — the entire backlog is unread.
                            partition.CurrentLag = currentRealOffset;
                        }

                        // Calculate Production Rate: change in high watermark since last tick.
                        // In a pre-loaded scenario (all messages produced before controller starts),
                        // this will be 0 because the watermark doesn't change between ticks.
                        // That's correct — no NEW messages are arriving.
                        if (_previousOffsets.TryGetValue(partitionId, out long lastRealOffset))
                        {
                            partition.ProductionRate = Math.Max(0, currentRealOffset - lastRealOffset);
                        }
                        _previousOffsets[partitionId] = currentRealOffset;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Kafka] Error polling partition {partitionId}: {ex.Message}");
                    }
                }

                // ------------------------------------------------------------
                // ObservedConsumptionRate: committed-offset delta / elapsed seconds
                // Skip the very first sample to avoid a spike from zero.
                // ------------------------------------------------------------
                if (_lastCommittedOffsetSampleTime != DateTime.MinValue)
                {
                    var elapsed = (now - _lastCommittedOffsetSampleTime).TotalSeconds;
                    if (elapsed > 0)
                    {
                        long delta = totalCurrentCommitted - _lastTotalCommittedOffset;
                        if (delta > 0)
                        {
                            ObservedConsumptionRate = delta / elapsed;
                        }
                        // else: no forward progress — keep previous rate (avoids flapping to 0 on momentary stalls)
                    }
                }

                _lastTotalCommittedOffset = totalCurrentCommitted;
                _lastCommittedOffsetSampleTime = now;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Kafka] Error fetching group offsets: {ex.Message}");
            }

            return partitions;
        }

        public void Dispose()
        {
            _pollConsumer.Close();
            _pollConsumer.Dispose();
            _adminClient.Dispose();
            _httpClient.Dispose();
        }
    }
}
