using MBrokerBench.Models;

namespace MBrokerBench.Components
{
    public class Consumer
    {
        public string Id { get; }
        public double MaxCapacity { get; }
        public List<Partition> AssignedPartitions { get; } = new List<Partition>();
        public ConsumerProfile ConsumerProfile { get; init; }


        public Consumer(string id, ConsumerProfile consumerProfile)
        {
            Id = id;
            MaxCapacity = consumerProfile.MaxCapacity;
            ConsumerProfile = consumerProfile;
        }

        // Sum of production rates from assigned partitions.
        public double GetCurrentWorkloadRate()
        {
            return AssignedPartitions.Sum(p => p.ProductionRate);
        }

        public double GetCurrentAndFutureWorkloadRate(double sla)
        {
            return AssignedPartitions.Sum(p => p.GetRequiredThroughput(sla));
        }

        // Sum of total lag for assigned partitions (used by lag-aware strategies).
        public long GetCurrentTotalLag(double rebalanceTimeSeconds)
        {
            return AssignedPartitions.Sum(p => p.GetTotalLag(rebalanceTimeSeconds));
        }

        public double RemainingCapacity => MaxCapacity - GetCurrentWorkloadRate();

        // Consume messages for a given time step, reducing lag.
        // Consume from highest-lag partitions first (fair and reduces SLA violations).
        // Returns the number of messages consumed during this timestep.
        public long Consume(double timeStepSeconds)
        {
            double availableWork = MaxCapacity * timeStepSeconds;
            long totalConsumed = 0;

            foreach (var partition in AssignedPartitions.OrderByDescending(p => p.CurrentLag))
            {
                if (availableWork <= 0) break;

                long consumed = Math.Min(partition.CurrentLag, (long)Math.Floor(availableWork));

                partition.CurrentLag -= consumed;
                totalConsumed += consumed;
                availableWork -= consumed;
            }

            return totalConsumed;
        }

        internal double GetRebalanceCost(double rebalanceTimeSeconds)
        {
            return AssignedPartitions.Sum(p => p.ProductionRate * rebalanceTimeSeconds);
        }
    }
}
