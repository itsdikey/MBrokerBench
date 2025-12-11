namespace MBrokerBench
{
    // Simulation runtime classes (models moved to MBrokerBench/Models/SimulationModels.cs)

    public class Partition
    {
        public string Id { get; }
        public long CurrentLag { get; set; } // Messages or bytes pending consumption
        public double ProductionRate { get; set; } // Messages(or bytes)/sec

        // Consumer currently assigned to this partition.
        public Consumer? AssignedConsumer { get; set; }

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
    }
}
