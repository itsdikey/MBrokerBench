namespace MBrokerBench.Components
{
    public interface IPartition
    {
        long CurrentLag { get; set; }
        Consumer? AssignedConsumer { get; set; }
        double ProductionRate { get; set; }
    }
}