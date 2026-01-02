using MBrokerBench.Components;

namespace MBrokerBench.DataProviders
{
    public interface IDataProvider
    {
        public int MaxRuntimeSteps { get; }
        List<Partition> InitializePartitions();
        List<Partition> Process(List<Partition> partitions, int timeStep);
    }
}
