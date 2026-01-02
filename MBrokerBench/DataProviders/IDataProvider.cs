using MBrokerBench.Components;

namespace MBrokerBench.DataProviders
{
    public interface IDataProvider
    {
        List<Partition> InitializePartitions();
        List<Partition> Process(List<Partition> partitions, int timeStep);
    }
}
