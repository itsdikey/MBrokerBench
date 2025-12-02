namespace MBrokerBench
{
    // Assignment strategy interface.
    public interface IPartitionAssignmentStrategy
    {
        double RebalanceTimeSeconds { get; set; }
        ConsumerGroup? ConsumerGroup { get; set; }

        void Assign(List<Partition> partitions, List<Consumer> consumers);
        Task AutoScale();
    }
}
