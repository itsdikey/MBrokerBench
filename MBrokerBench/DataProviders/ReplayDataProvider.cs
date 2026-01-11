using MBrokerBench.Components;
using System.Text.Json;

namespace MBrokerBench.DataProviders
{
    internal sealed class ReplayDataProvider : IDataProvider
    {
        private readonly ReplayRecording _rec;

        public int MaxRuntimeSteps { get; private set; }

        public ReplayDataProvider(string path)
        {
            var json = System.IO.File.ReadAllText(path);
            _rec = JsonSerializer.Deserialize<ReplayRecording>(json) ?? new ReplayRecording();
            MaxRuntimeSteps = _rec.MaxRuntimeSteps;
        }

        public List<Partition> InitializePartitions()
        {
            return _rec.InitialPartitions.Select(p => new Partition(p.Id) { ProductionRate = p.ProductionRate, CurrentLag = p.CurrentLag }).ToList();
        }

        public List<Partition> Process(List<Partition> partitions, int timeStep)
        {
            var step = _rec.Steps.FirstOrDefault(s => s.TimeStep == timeStep);
            if (step != null)
            {
                // Apply changes: update existing partitions, add/remove as necessary
                // Update rates and lag
                foreach (var pr in step.Partitions)
                {
                    var p = partitions.FirstOrDefault(x => x.Id == pr.Id);
                    if (p != null)
                    {
                        p.ProductionRate = pr.ProductionRate;
                        p.CurrentLag = pr.CurrentLag;
                    }
                    else
                    {
                        var np = new Partition(pr.Id) { ProductionRate = pr.ProductionRate, CurrentLag = pr.CurrentLag };
                        partitions.Add(np);
                    }
                }

                // Remove partitions not present in recorded step
                var ids = step.Partitions.Select(x => x.Id).ToHashSet();
                partitions.RemoveAll(p => !ids.Contains(p.Id));
            }

            return partitions;
        }

        private class ReplayRecording
        {
            public int MaxRuntimeSteps { get; set; }
            public List<PartitionRecord> InitialPartitions { get; set; } = new();
            public List<StepRecord> Steps { get; set; } = new();
        }

        private class StepRecord
        {
            public int TimeStep { get; set; }
            public List<PartitionRecord> Partitions { get; set; } = new();
        }

        private class PartitionRecord
        {
            public string Id { get; set; } = string.Empty;
            public double ProductionRate { get; set; }
            public long CurrentLag { get; set; }
        }
    }
}
