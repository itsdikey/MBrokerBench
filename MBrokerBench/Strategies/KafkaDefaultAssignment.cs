using MBrokerBench.Components;
using MBrokerBench.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MBrokerBench.Strategies
{
    public enum KafkaAssignorType
    {
        Range,
        RoundRobin,
        Sticky
    }

    public class KafkaDefaultAssignment : IPartitionAssignmentStrategy
    {
        public double RebalanceTimeSeconds { get; set; }
        public ConsumerGroup? ConsumerGroup { get; set; }

        public KafkaAssignorType AssignorType { get; set; } = KafkaAssignorType.Sticky;

        // Standard HPA parameters
        public double TargetUtilization { get; set; } = 0.80; // Target utilization (80%)
        public double TargetLatencySeconds { get; set; } = 5.0; // Target latency threshold (seconds)
        public bool AllowScaleToZero { get; set; } = true;    // KEDA style scale to zero

        public KafkaDefaultAssignment()
        {
            var typeEnv = System.Environment.GetEnvironmentVariable("KAFKA_ASSIGNOR") ?? "Sticky";
            if (Enum.TryParse<KafkaAssignorType>(typeEnv, true, out var parsedType))
            {
                AssignorType = parsedType;
            }
            
            var utilEnv = System.Environment.GetEnvironmentVariable("HPA_TARGET_UTILIZATION");
            if (double.TryParse(utilEnv, out double util))
            {
                TargetUtilization = util;
            }

            var latEnv = System.Environment.GetEnvironmentVariable("HPA_TARGET_LATENCY");
            if (double.TryParse(latEnv, out double lat))
            {
                TargetLatencySeconds = lat;
            }
        }

        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            if (ConsumerGroup == null || consumers == null || !consumers.Any()) return;

            // 1. Clear Assignments
            foreach (var c in consumers) c.AssignedPartitions.Clear();
            foreach (var p in partitions) p.AssignedConsumer = null;

            var sortedConsumers = consumers.OrderBy(c => c.Id).ToList();
            var sortedPartitions = partitions.OrderBy(p => p.Id).ToList();

            switch (AssignorType)
            {
                case KafkaAssignorType.Range:
                    AssignRange(sortedPartitions, sortedConsumers);
                    break;
                case KafkaAssignorType.RoundRobin:
                    AssignRoundRobin(sortedPartitions, sortedConsumers);
                    break;
                case KafkaAssignorType.Sticky:
                    AssignSticky(sortedPartitions, sortedConsumers);
                    break;
            }

            Logger.Log($"[KafkaAssignor] Rebalance completed using {AssignorType}. Fleet Size={consumers.Count}, Idle={consumers.Count(c => !c.AssignedPartitions.Any())}");
        }

        private void AssignRange(List<Partition> partitions, List<Consumer> consumers)
        {
            int n = partitions.Count;
            int c = consumers.Count;
            int numPartitionsPerConsumer = n / c;
            int consumersWithExtra = n % c;

            for (int i = 0; i < c; i++)
            {
                int count = numPartitionsPerConsumer + (i < consumersWithExtra ? 1 : 0);
                int start = i * numPartitionsPerConsumer + Math.Min(i, consumersWithExtra);

                for (int j = 0; j < count; j++)
                {
                    var partition = partitions[start + j];
                    consumers[i].AssignedPartitions.Add(partition);
                    partition.AssignedConsumer = consumers[i];
                }
            }
        }

        private void AssignRoundRobin(List<Partition> partitions, List<Consumer> consumers)
        {
            int c = consumers.Count;
            for (int j = 0; j < partitions.Count; j++)
            {
                var consumer = consumers[j % c];
                consumer.AssignedPartitions.Add(partitions[j]);
                partitions[j].AssignedConsumer = consumer;
            }
        }

        private void AssignSticky(List<Partition> partitions, List<Consumer> consumers)
        {
            int n = partitions.Count;
            int c = consumers.Count;
            int minPartitionsPerConsumer = n / c;
            int consumersWithExtra = n % c;

            var targetCounts = consumers.ToDictionary(
                cons => cons.Id,
                cons => {
                    int index = consumers.IndexOf(cons);
                    return minPartitionsPerConsumer + (index < consumersWithExtra ? 1 : 0);
                }
            );

            var consumerPartitionsCount = consumers.ToDictionary(cons => cons.Id, cons => 0);
            var unassignedPartitions = new List<Partition>();

            // Phase 1: Keep previous assignments if the consumer is still active and has target room
            foreach (var p in partitions)
            {
                var prevConsumer = consumers.FirstOrDefault(cons => p.AssignedConsumer?.Id == cons.Id);
                if (prevConsumer != null && consumerPartitionsCount[prevConsumer.Id] < targetCounts[prevConsumer.Id])
                {
                    prevConsumer.AssignedPartitions.Add(p);
                    p.AssignedConsumer = prevConsumer;
                    consumerPartitionsCount[prevConsumer.Id]++;
                }
                else
                {
                    unassignedPartitions.Add(p);
                }
            }

            // Phase 2: Distribute remaining partitions to under-assigned consumers
            foreach (var p in unassignedPartitions)
            {
                var targetConsumer = consumers
                    .Where(cons => consumerPartitionsCount[cons.Id] < targetCounts[cons.Id])
                    .OrderBy(cons => consumerPartitionsCount[cons.Id])
                    .FirstOrDefault();

                if (targetConsumer != null)
                {
                    targetConsumer.AssignedPartitions.Add(p);
                    p.AssignedConsumer = targetConsumer;
                    consumerPartitionsCount[targetConsumer.Id]++;
                }
            }
        }

        public Task AutoScale()
        {
            if (ConsumerGroup == null) return Task.CompletedTask;

            var consumers = ConsumerGroup.Consumers;
            var partitions = ConsumerGroup.AllPartitions;

            // In standard Kafka HPA/KEDA, we scale a homogeneous fleet using the default profile capacity
            double capacityPerConsumer = ConsumerGroup.ConsumerCapacity;
            long totalLag = partitions.Sum(p => p.CurrentLag);
            double totalArrivalRate = partitions.Sum(p => p.ProductionRate);

            // HPA target lag calculation based on latency SLA
            double targetLagPerConsumer = capacityPerConsumer * TargetLatencySeconds * TargetUtilization;
            if (targetLagPerConsumer <= 0) targetLagPerConsumer = 100;

            int desiredCount = 1;
            if (totalLag > 0)
            {
                desiredCount = (int)Math.Ceiling((double)totalLag / targetLagPerConsumer);
            }
            else if (totalArrivalRate > 0)
            {
                desiredCount = (int)Math.Ceiling(totalArrivalRate / (capacityPerConsumer * TargetUtilization));
            }
            else if (AllowScaleToZero)
            {
                desiredCount = 0;
            }

            // Kafka constraint: Replicas capped at partition count
            int maxReplicas = partitions.Count;
            desiredCount = Math.Min(desiredCount, maxReplicas);

            if (desiredCount < 1 && totalArrivalRate > 0 && maxReplicas > 0)
            {
                desiredCount = 1;
            }

            int currentCount = consumers.Count;

            if (desiredCount > currentCount)
            {
                int toAdd = desiredCount - currentCount;
                Logger.Log($"[KAFKA-HPA] Scaling UP: {currentCount} -> {desiredCount} (Lag={totalLag}, Rate={totalArrivalRate:F1})");
                for (int i = 0; i < toAdd; i++)
                {
                    // Always add the default profile (homogeneous scaling)
                    ConsumerGroup.AddConsumer(ConsumerGroup.DefaultProfile.Name);
                }
                ConsumerGroup.Rebalance();
            }
            else if (desiredCount < currentCount)
            {
                int toRemove = currentCount - desiredCount;
                Logger.Log($"[KAFKA-HPA] Scaling DOWN: {currentCount} -> {desiredCount} (Lag={totalLag}, Rate={totalArrivalRate:F1})");
                for (int i = 0; i < toRemove; i++)
                {
                    // Remove the default profile consumers
                    var removable = ConsumerGroup.Consumers.FirstOrDefault(c => c.ConsumerProfile.Name == ConsumerGroup.DefaultProfile.Name)
                                    ?? ConsumerGroup.Consumers.Last();
                    ConsumerGroup.RemoveConsumer(removable);
                }
                ConsumerGroup.Rebalance();
            }

            return Task.CompletedTask;
        }
    }
}
