using MBrokerBench.Components;
using System.Text.Json;

namespace MBrokerBench.DataProviders
{
    internal sealed class JSONConfigDataProvider : IDataProvider
    {
        private readonly string _configPath;
        private JsonDocument? _doc;
        private JsonElement? _selectedConfig;
        private double _rateVariability = 0.0;
        private int _rateRefreshInterval = 0;
        private double _maxRate = double.MaxValue;
        private Dictionary<string, double> _baseRates = new();

        public int MaxRuntimeSteps { get; private set; } = 600;

        public JSONConfigDataProvider(string? configPath = null)
        {
            _configPath = configPath ?? Path.Combine(AppContext.BaseDirectory, "simulation_config.json");

            if (File.Exists(_configPath))
            {
                try
                {
                    var json = File.ReadAllText(_configPath);
                    _doc = JsonDocument.Parse(json);

                    var root = _doc.RootElement;
                    if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("Configs", out var configsElem))
                    {
                        string? activeTest = null;
                        if (root.TryGetProperty("ActiveTest", out var at))
                            activeTest = at.GetString();

                        foreach (var c in configsElem.EnumerateArray())
                        {
                            if (_selectedConfig == null && (!string.IsNullOrEmpty(activeTest) && c.GetProperty("Name").GetString() == activeTest))
                            {
                                _selectedConfig = c;
                            }
                            else if (_selectedConfig == null && string.IsNullOrEmpty(activeTest))
                            {
                                _selectedConfig = c;
                            }
                        }

                        if (_selectedConfig == null)
                            _selectedConfig = configsElem.EnumerateArray().FirstOrDefault();

                        if (_selectedConfig != null)
                        {
                            var sel = _selectedConfig.Value;
                            if (sel.TryGetProperty("InitialPartitionsConfig", out var ipc))
                            {
                                if (ipc.TryGetProperty("RateVariability", out var rv))
                                    _rateVariability = rv.GetDouble();
                                if (ipc.TryGetProperty("RateRefreshIntervalSteps", out var rri))
                                    _rateRefreshInterval = rri.GetInt32();
                                if (ipc.TryGetProperty("MaxRate", out var mr))
                                    _maxRate = mr.GetDouble();
                            }

                            if (sel.TryGetProperty("MaxRuntimeSteps", out var mrs))
                            {
                                MaxRuntimeSteps = mrs.GetInt32();
                            }
                        }
                    }
                    else
                    {
                        // legacy single-config format
                        try
                        {
                            var cfg = JsonSerializer.Deserialize<SimulationConfig>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                            if (cfg != null)
                            {
                                MaxRuntimeSteps = cfg.MaxRuntimeSteps;
                            }
                        }
                        catch { }
                    }
                }
                catch { /* ignore parse errors for now */ }
            }
        }

        public List<Partition> InitializePartitions()
        {
            var list = new List<Partition>();
            if (_selectedConfig == null && _doc != null)
            {
                try
                {
                    var root = _doc.RootElement;
                    // legacy single-config format
                    var cfg = JsonSerializer.Deserialize<SimulationConfig>(root.GetRawText(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                    if (cfg != null)
                    {
                        foreach (var pc in cfg.InitialPartitions)
                        {
                            var p = new Partition(pc.Id) { ProductionRate = pc.ProductionRate };
                            list.Add(p);
                            _baseRates[p.Id] = p.ProductionRate;
                        }
                        return list;
                    }
                }
                catch { }
            }

            if (_selectedConfig != null)
            {
                var sel = _selectedConfig.Value;
                if (sel.TryGetProperty("InitialPartitions", out var ip))
                {
                    foreach (var pc in ip.EnumerateArray())
                    {
                        var id = pc.GetProperty("Id").GetString() ?? string.Empty;
                        var rate = pc.GetProperty("ProductionRate").GetDouble();
                        var lag = pc.GetProperty("CurrentLag").GetInt64();
                        var p = new Partition(id) { ProductionRate = rate };
                        list.Add(p);
                        _baseRates[p.Id] = p.ProductionRate;
                    }
                }
                else if (sel.TryGetProperty("InitialPartitionsConfig", out var ipc))
                {
                    int count = ipc.GetProperty("Count").GetInt32();
                    var pr = ipc.GetProperty("ProductionRateRange");
                    double prMin = pr.GetProperty("Min").GetDouble();
                    double prMax = pr.GetProperty("Max").GetDouble();
                    var ir = ipc.GetProperty("InitialLagRange");
                    long lagMin = ir.GetProperty("Min").GetInt64();
                    long lagMax = ir.GetProperty("Max").GetInt64();

                    var rnd = new Random();
                    for (int i = 0; i < count; i++)
                    {
                        double rate = prMin + rnd.NextDouble() * (prMax - prMin);
                        long lag = rnd.NextInt64(lagMin, lagMax + 1);
                        var p = new Partition($"P-{i + 1}") { ProductionRate = rate};
                        list.Add(p);
                        _baseRates[p.Id] = p.ProductionRate;
                    }
                }
            }

            return list;
        }

        public List<Partition> Process(List<Partition> partitions, int timeStep)
        {
            // Handle periodic production rate refresh
            if (_rateRefreshInterval > 0 && timeStep > 0 && timeStep % _rateRefreshInterval == 0)
            {
                var rnd = new Random();
                foreach (var p in partitions)
                {
                    if (!_baseRates.TryGetValue(p.Id, out var baseRate))
                        baseRate = p.ProductionRate;

                    var delta = (rnd.NextDouble() * 2.0) - 1.0; // -1..1
                    var newRate = baseRate * (1.0 + delta * _rateVariability);
                    if (newRate < 0) newRate = 0;
                    if (newRate > _maxRate) newRate = _maxRate;
                    p.ProductionRate = newRate;
                }
            }

            // Apply events at this timestep (if present in selected config)
            if (_selectedConfig != null)
            {
                if (_selectedConfig.Value.TryGetProperty("Events", out var ev))
                {
                    foreach (var e in ev.EnumerateArray())
                    {
                        var t = e.GetProperty("TimeStep").GetInt32();
                        if (t != timeStep) continue;

                        var type = e.GetProperty("Type").GetString() ?? string.Empty;
                        var pid = e.GetProperty("PartitionId").GetString() ?? string.Empty;

                        if (string.Equals(type, "add_partition", StringComparison.OrdinalIgnoreCase))
                        {
                            if (!partitions.Any(x => x.Id == pid))
                            {
                                var newP = new Partition(pid) { ProductionRate = e.GetProperty("ProductionRate").GetDouble()};
                                partitions.Add(newP);
                                _baseRates[newP.Id] = newP.ProductionRate;
                            }
                        }
                        else if (string.Equals(type, "remove_partition", StringComparison.OrdinalIgnoreCase))
                        {
                            var toRemove = partitions.FirstOrDefault(x => x.Id == pid);
                            if (toRemove != null)
                                partitions.Remove(toRemove);
                        }
                        else if (string.Equals(type, "change_rate", StringComparison.OrdinalIgnoreCase))
                        {
                            var p = partitions.FirstOrDefault(x => x.Id == pid);
                            if (p != null)
                            {
                                var newRate = e.GetProperty("ProductionRate").GetDouble();
                                p.ProductionRate = newRate;
                                _baseRates[p.Id] = newRate;
                            }
                        }
                    }
                }
            }

            // Advance production (caller will call Produce on partitions)
            return partitions;
        }
    }
}
