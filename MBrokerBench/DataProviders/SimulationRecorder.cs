using MBrokerBench.Components;
using System.Text.Json;

namespace MBrokerBench.DataProviders
{
    internal sealed class SimulationRecorder : IDataProvider
    {
        private readonly IDataProvider _inner;
        public string Path { get; }

        private readonly SimulationRecording _rec = new();

        public int MaxRuntimeSteps => _inner.MaxRuntimeSteps;

        public SimulationRecorder(string path, IDataProvider inner)
        {
            Path = path;
            _inner = inner;
            _rec.MaxRuntimeSteps = inner.MaxRuntimeSteps;
        }

        public List<Partition> InitializePartitions()
        {
            var parts = _inner.InitializePartitions();
            _rec.InitialPartitions = parts.Select(p => new PartitionRecord { Id = p.Id, ProductionRate = p.ProductionRate, CurrentLag = p.CurrentLag }).ToList();
            return parts;
        }

        public List<Partition> Process(List<Partition> partitions, int timeStep)
        {
            var result = _inner.Process(partitions, timeStep);

            var snapshot = new StepRecord
            {
                TimeStep = timeStep,
                Partitions = result.Select(p => new PartitionRecord { Id = p.Id, ProductionRate = p.ProductionRate, CurrentLag = p.CurrentLag }).ToList()
            };

            _rec.Steps.Add(snapshot);

            return result;
        }

        public void Save()
        {
            var options = new JsonSerializerOptions { WriteIndented = true };
            var json = JsonSerializer.Serialize(_rec, options);
            var dir = System.IO.Path.GetDirectoryName(Path);
            if (!string.IsNullOrEmpty(dir)) System.IO.Directory.CreateDirectory(dir);
            System.IO.File.WriteAllText(Path, json);
        }

        private class SimulationRecording
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
