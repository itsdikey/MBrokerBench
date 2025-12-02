using System.Collections.Generic;

namespace MBrokerBench
{
    // Models for simulation config (moved out of Program.cs)

    public record Message(long Timestamp, string Key, int PayloadSize);

    public class SimulationConfig
    {
        public List<PartitionConfig> InitialPartitions { get; set; } = new List<PartitionConfig>();
        public List<SimulationEvent> Events { get; set; } = new List<SimulationEvent>();
        public int MaxRuntimeSteps { get; set; } = 600;
    }

    public class PartitionConfig
    {
        public string Id { get; set; } = string.Empty;
        public double ProductionRate { get; set; }
        public long CurrentLag { get; set; }
    }

    public class SimulationEvent
    {
        // Time step when the event occurs (1-based)
        public int TimeStep { get; set; }
        // "add_partition", "remove_partition", "change_rate"
        public string Type { get; set; } = string.Empty;
        public string PartitionId { get; set; } = string.Empty;
        public double? ProductionRate { get; set; }
        public long? CurrentLag { get; set; }
    }

    // Models for the new multi-config file format
    public class ConfigFile
    {
        public string? ActiveTest { get; set; }
        public List<ConfigEntry> Configs { get; set; } = new List<ConfigEntry>();
    }

    public class ConfigEntry
    {
        public string Name { get; set; } = string.Empty;
        public List<PartitionConfig>? InitialPartitions { get; set; }
        public InitialPartitionsConfig? InitialPartitionsConfig { get; set; }
        public List<SimulationEvent>? Events { get; set; }
        public int? MaxRuntimeSteps { get; set; }
    }

    public class InitialPartitionsConfig
    {
        public int Count { get; set; }
        public RangeDouble ProductionRateRange { get; set; } = new RangeDouble();
        public RangeLong InitialLagRange { get; set; } = new RangeLong();
        public double? RateVariability { get; set; }
        public int? RateRefreshIntervalSteps { get; set; }
    }

    public class RangeDouble
    {
        public double Min { get; set; }
        public double Max { get; set; }
    }

    public class RangeLong
    {
        public long Min { get; set; }
        public long Max { get; set; }
    }
}
