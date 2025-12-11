using MBrokerBench.Models;

namespace MBrokerBench
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

        // Sum of total lag for assigned partitions (used by lag-aware strategies).
        public long GetCurrentTotalLag(double rebalanceTimeSeconds)
        {
            return AssignedPartitions.Sum(p => p.GetTotalLag(rebalanceTimeSeconds));
        }

        public double RemainingCapacity => MaxCapacity - GetCurrentWorkloadRate();

        // Consume messages for a given time step, reducing lag.
        // Consume from highest-lag partitions first (fair and reduces SLA violations).
        public void Consume(double timeStepSeconds)
        {
            double availableWork = MaxCapacity * timeStepSeconds;

            foreach (var partition in AssignedPartitions.OrderByDescending(p => p.CurrentLag))
            {
                if (availableWork <= 0) break;

                long consumed = (long)Math.Min(partition.CurrentLag, (long)Math.Floor(availableWork));

                partition.CurrentLag -= consumed;
                availableWork -= consumed;
            }
        }
    }
}
