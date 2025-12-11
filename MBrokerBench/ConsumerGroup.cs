using MBrokerBench.Models;

namespace MBrokerBench
{
    public class ConsumerGroup
    {
        public string GroupId { get; }
        public List<Partition> AllPartitions { get; }
        public List<Consumer> Consumers { get; private set; } = new List<Consumer>();

        public double TotalCostPerSecond => Consumers.Sum(x => x.ConsumerProfile.CostPerSecond);

        public IReadOnlyList<ConsumerProfile> ConsumerProfiles { get; private set; }
        public ConsumerProfile DefaultProfile { get; private set; }
        public double ConsumerCapacity => _consumerCapacity;

        private readonly IPartitionAssignmentStrategy _assignmentStrategy;
        
        private readonly double _consumerCapacity;


        public double RebalanceTimeSeconds { get; } = 5.0; // rebalance blocking time

        public double LatencySLASeconds { get; } = 10.0;   // SLA window


        public ConsumerGroup(
            string groupId,
            List<Partition> partitions,
            List<ConsumerProfile> consumerProfiles, 
            ConsumerProfile defaultProfile,
            IPartitionAssignmentStrategy assignmentStrategy
            )
        {
            GroupId = groupId;
            AllPartitions = partitions;
            ConsumerProfiles = consumerProfiles;
            DefaultProfile = defaultProfile;

            _consumerCapacity = defaultProfile.MaxCapacity;

            _assignmentStrategy = assignmentStrategy;
            _assignmentStrategy.RebalanceTimeSeconds = RebalanceTimeSeconds;
            _assignmentStrategy.ConsumerGroup = this;
        }

        public Consumer AddConsumer(string? profileName = null)
        {
            var profile = ConsumerProfiles.FirstOrDefault(p => p.Name == profileName) ?? DefaultProfile;
            var newConsumer = new Consumer($"C-{Guid.NewGuid()}", profile);
            Consumers.Add(newConsumer);
            MetricsExporter.SetConsumers(Consumers.Count);
            MetricsExporter.IncScaleUp();
            Console.WriteLine($"[SCALED UP] New Consumer {newConsumer.Id} added. Total: {Consumers.Count}");
            return newConsumer;
        }

        public void RemoveConsumer(Consumer consumer)
        {
            Console.WriteLine($"[SCALED DOWN] Removing Consumer {consumer.Id}...");

            // Unassign partitions so the next rebalance will reassign them.
            foreach (var partition in consumer.AssignedPartitions.ToList())
            {
                partition.AssignedConsumer = null;
                consumer.AssignedPartitions.Remove(partition);
            }

            Consumers.Remove(consumer);
            MetricsExporter.SetConsumers(Consumers.Count);
            MetricsExporter.IncScaleDown();
            Console.WriteLine($"[SCALED DOWN] Consumer {consumer.Id} removed. Total: {Consumers.Count}");
        }

        // Rebalance all partitions using the configured strategy.
        public void Rebalance()
        {
            Console.WriteLine($"--- REBALANCING (Blocking for {RebalanceTimeSeconds}s) ---");

            //We are emulating rebalance time by blocking partition production for that duration if reassigned.
            //This will cause backlog to appear on reassigned partitions.
            var partitionConsumerMap = new Dictionary<string, string?>();

            foreach (var consumer in Consumers) 
            {
                foreach (var partition in consumer.AssignedPartitions) 
                {
                    partitionConsumerMap[partition.Id] = consumer.Id;
                }
            }

            _assignmentStrategy.Assign(AllPartitions, Consumers);

            foreach(var partition in AllPartitions)
            {
                if (partitionConsumerMap.TryGetValue(partition.Id, out var previousConsumerId))
                {
                    if (previousConsumerId != partition.AssignedConsumer?.Id)
                    {
                        partition.Produce(RebalanceTimeSeconds);
                    }
                }
            }

            // Update partition metrics labels after rebalance
            foreach (var p in AllPartitions)
            {
                MetricsExporter.SetPartition(p.Id, p.CurrentLag, p.ProductionRate);
                MetricsExporter.SetPartitionAssignment(p.Id, p.AssignedConsumer?.Id);
            }

            // Update consumer metrics after rebalance
            foreach (var c in Consumers)
            {
                double util = (c.GetCurrentWorkloadRate() / c.MaxCapacity) * 100.0;
                MetricsExporter.SetConsumerMetrics(c.Id, util, c.AssignedPartitions.Count);
            }
        }

        public void Autoscale()
        {
            _assignmentStrategy.AutoScale();
        }
    }
}
