namespace MBrokerBench.Components
{
    public interface IPartition
    {
        long CurrentLag { get; }
        Consumer? AssignedConsumer { get; set; }
        double ProductionRate { get; set; }
        void Consume(long amount);
        double GetRequiredThroughput(double sla);
        long GetTotalLag(double rebalanceTimeSeconds);
        void Produce(double timeStepSeconds);
    }
}