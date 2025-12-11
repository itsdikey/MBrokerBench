namespace MBrokerBench
{
    // Lag-aware worst-fit assignment (place largest total-lag partitions on least-lagged consumer).
    public class LagAwareWorstFitAssignment : IPartitionAssignmentStrategy
    {
        public double RebalanceTimeSeconds { get; set; }
        public ConsumerGroup? ConsumerGroup { get; set; }

        public float _rebalanceTimeSeconds = 5.0f; // rebalance blocking time
        public float _latencySLASeconds = 30.0f;   // SLA window
        public float _scaleDownUtilizationThreshold = 0.20f; // consider removing consumer if <20% utilized
        public float _scaleUpHysteresis = 1.05f; // require 5% headroom before scaling up

        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            if (!consumers.Any()) return;

            // Clear consumers' partitions BEFORE assigning to avoid duplicates.
            foreach (var c in consumers)
                c.AssignedPartitions.Clear();

            foreach (var p in partitions)
                p.AssignedConsumer = null;

            var sortedPartitions = partitions.OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds)).ToList();

            foreach (var partition in sortedPartitions)
            {
                Consumer targetConsumer = consumers
                    .OrderBy(c => c.GetCurrentTotalLag(RebalanceTimeSeconds))
                    .First();

                targetConsumer.AssignedPartitions.Add(partition);
                partition.AssignedConsumer = targetConsumer;
            }

            Console.WriteLine($"[Assignment] Used LagAwareWorstFit strategy (Rebalance Time: {RebalanceTimeSeconds}s). Consumers={consumers.Count}");
        }

        public Task AutoScale()
        {
            if (ConsumerGroup == null)
            {
                return Task.CompletedTask;
            }
            
            // Conservative required capacity: rate + lag / SLA


            var consumers = ConsumerGroup.Consumers;

            double totalRequiredCapacitySLA = ConsumerGroup.AllPartitions
                .Sum(p => p.ProductionRate + (double)p.GetTotalLag(_rebalanceTimeSeconds) / _latencySLASeconds);

            int requiredConsumersSLA = (int)Math.Ceiling(totalRequiredCapacitySLA / ConsumerGroup.ConsumerCapacity);

            // Apply hysteresis for scaling up
            int targetConsumers = consumers.Count;
            if (requiredConsumersSLA > consumers.Count)
            {
                // only scale up if required > current * hysteresis
                if (requiredConsumersSLA >= Math.Ceiling(consumers.Count * _scaleUpHysteresis))
                    targetConsumers = requiredConsumersSLA;
            }
            else if (requiredConsumersSLA < consumers.Count)
            {
                // scale down only if there exists at least one under-utilized consumer
                var underUtilized = consumers
                    .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * _scaleDownUtilizationThreshold)
                    .OrderBy(c => c.GetCurrentWorkloadRate())
                    .FirstOrDefault();

                if (underUtilized != null)
                    targetConsumers = consumers.Count - 1;
            }

            if (targetConsumers > consumers.Count)
            {
                int toAdd = targetConsumers - consumers.Count;
                for (int i = 0; i < toAdd; i++) ConsumerGroup.AddConsumer();
                ConsumerGroup.Rebalance();
            }
            else if (targetConsumers < consumers.Count)
            {
                // Remove one under-utilized consumer (safe downscale), then rebalance.
                var removable = consumers
                    .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * _scaleDownUtilizationThreshold)
                    .OrderBy(c => c.GetCurrentWorkloadRate())
                    .FirstOrDefault();

                if (removable != null)
                {
                    ConsumerGroup.RemoveConsumer(removable);
                    ConsumerGroup.Rebalance();
                }
            }

            return Task.CompletedTask;
        }
    }
}
