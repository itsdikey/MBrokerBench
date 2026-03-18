namespace MBrokerBench.Components
{
    // Simulation runtime classes (models moved to MBrokerBench/Models/SimulationModels.cs)

    public class Partition : IPartition
    {
        public string Id { get; }
        public long CurrentLag { get; private set; } // Messages or bytes pending consumption
        public double ProductionRate { get; set; } // Messages(or bytes)/sec

        // Consumer currently assigned to this partition.
        public Consumer? AssignedConsumer { get; set; }
        public double RebalancePenaltyRemaining { get; internal set; }

        public Partition(string id)
        {
            Id = id;
        }

        // Simulate new messages arriving over a time step.
        public void Produce(double timeStepSeconds)
        {
            int count = (int)Math.Floor(ProductionRate * timeStepSeconds);
            CurrentLag += count;
        }

        // Total lag including projected messages during a rebalance window.
        public long GetTotalLag(double rebalanceTimeSeconds)
        {
            return CurrentLag + (long)Math.Ceiling(ProductionRate * rebalanceTimeSeconds);
        }

        public double GetRequiredThroughput(double sla)
        {
            double catchupRate = CurrentLag / sla;

            return ProductionRate + catchupRate;
        }

        public void Consume(long amount)
        {
            CurrentLag -= amount;
        }
    }
}
