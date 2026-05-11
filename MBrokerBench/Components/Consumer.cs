using MBrokerBench.Models;

namespace MBrokerBench.Components
{
    public enum ConsumerState
    {
        Booting,       // Container starting, process initializing (Cost: YES, Capacity: 0)
        Syncing,       // Joined group, fetching assignments/state (Cost: YES, Capacity: 0)
        Running,       // Fully operational (Cost: YES, Capacity: 100%)
        Terminating    // Shutting down (Cost: YES, Capacity: 0)
    }

    public class Consumer
    {
        public string Id { get; }
        public double MaxCapacity { get; }
        public List<Partition> AssignedPartitions { get; } = new List<Partition>();
        public ConsumerProfile ConsumerProfile { get; init; }
        public double StartupTimeRemaining { get; set; }
        public double Efficiency { get; internal set; } = 0.5;
        public ConsumerState State { get; set; } = ConsumerState.Booting;

        public Consumer(string id, ConsumerProfile consumerProfile)
        {
            Id = id;
            MaxCapacity = consumerProfile.MaxCapacity;
            ConsumerProfile = consumerProfile;
            StartupTimeRemaining =  (1 - Random.Shared.NextDouble() / 4) * consumerProfile.StartupTime; // +/- 25% variance
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

        public double CurrentEffectiveCapacity
        {
            get
            {
                if (State != ConsumerState.Running) return 0;
                return MaxCapacity * Efficiency;
            }
        }

        public double RemainingCapacityWithEfficiency
        {
            get
            {
                double limit = CurrentEffectiveCapacity;
                double currentLoad = GetCurrentWorkloadRate();

                return Math.Max(0, limit - currentLoad);
            }
        }

        public void Tick(double timeStepSeconds)
        {
            if (State == ConsumerState.Booting)
            {
                StartupTimeRemaining -= timeStepSeconds;
                if (StartupTimeRemaining <= 0)
                {
                    State = ConsumerState.Running;
                    Efficiency = 0.5; // Reset efficiency to cold-start level upon running
                }
            }
            else if (State == ConsumerState.Running)
            {
                if (Efficiency < 1.0)
                {
                    Efficiency += (0.5 / 20.0) * timeStepSeconds;
                    if (Efficiency > 1.0) Efficiency = 1.0;
                }
            }
        }

        public double RemainingCapacity => MaxCapacity - GetCurrentWorkloadRate();

        // Consume messages for a given time step, reducing lag.
        // Consume from highest-lag partitions first (fair and reduces SLA violations).
        // Returns the number of messages consumed during this timestep.
        public long Consume(double timeStepSeconds)
        {
            double currentCap = CurrentEffectiveCapacity;

            // If booting, we consume 0.
            if (currentCap <= 0) return 0;

            double availableWork = currentCap * timeStepSeconds;
            long totalConsumed = 0;

            var pausedPartitions = AssignedPartitions.Where(p => p.RebalancePenaltyRemaining > 0).ToList();
            var activePartitions = AssignedPartitions.Except(pausedPartitions).ToList();

            foreach(var paused in pausedPartitions)
            {
                paused.RebalancePenaltyRemaining -= timeStepSeconds;
                if (paused.RebalancePenaltyRemaining < 0)
                {
                    paused.RebalancePenaltyRemaining = 0;
                }
            }

            foreach (var partition in activePartitions.OrderByDescending(p => p.CurrentLag))
            {
                if (availableWork <= 0) break;

                long consumed = Math.Min(partition.CurrentLag, (long)Math.Floor(availableWork));

                partition.Consume(consumed);
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
