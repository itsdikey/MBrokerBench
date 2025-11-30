using System.Text.Json;

namespace MBrokerBench
{
    // Core data structure for a message (kept for future per-message simulation).
    public record Message(long Timestamp, string Key, int PayloadSize);

    // Config DTOs for simulation file
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

    public class Partition
    {
        public string Id { get; }
        public long CurrentLag { get; set; }
        public double ProductionRate { get; set; } // Messages/sec

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

    public class Consumer
    {
        public string Id { get; }
        public double MaxCapacity { get; }
        public List<Partition> AssignedPartitions { get; } = new List<Partition>();

        public Consumer(string id, double maxCapacity)
        {
            Id = id;
            MaxCapacity = maxCapacity;
        }

        // Sum of production rates from assigned partitions.
        public double GetCurrentWorkloadRate()
        {
            return AssignedPartitions.Sum(p => p.ProductionRate);
        }

        // Sum of total lag for assigned partitions (used by lag-aware strategies).
        public long GetCurrentTotalLag(double rebalanceTimeSeconds)
        {
            return AssignedPartitions.Sum(p => p.GetTotalLag(rebalanceTimeSeconds));
        }

        // Consume messages for a given time step, reducing lag.
        // Consume from highest-lag partitions first (fair and reduces SLA violations).
        public void Consume(double timeStepSeconds)
        {
            double availableWork = MaxCapacity * timeStepSeconds;

            foreach (var partition in AssignedPartitions.OrderByDescending(p => p.CurrentLag))
            {
                if (availableWork <= 0) break;

                long consumed = (long)Math.Min(partition.CurrentLag, (long)Math.Floor(availableWork));

                partition.CurrentLag -= consumed;
                availableWork -= consumed;
            }
        }
    }

    // Assignment strategy interface.
    public interface IPartitionAssignmentStrategy
    {
        double RebalanceTimeSeconds { get; set; }
        void Assign(List<Partition> partitions, List<Consumer> consumers);
    }

    // Lag-aware worst-fit assignment (place largest total-lag partitions on least-lagged consumer).
    public class LagAwareWorstFitAssignment : IPartitionAssignmentStrategy
    {
        public double RebalanceTimeSeconds { get; set; }

        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            if (!consumers.Any()) return;

            // Clear consumers' partitions BEFORE assigning to avoid duplicates.
            foreach (var c in consumers)
                c.AssignedPartitions.Clear();

            foreach (var p in partitions)
                p.AssignedConsumer = null;

            var sortedPartitions = partitions.OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds)).ToList();

            foreach (var partition in sortedPartitions)
            {
                Consumer targetConsumer = consumers
                    .OrderBy(c => c.GetCurrentTotalLag(RebalanceTimeSeconds))
                    .First();

                targetConsumer.AssignedPartitions.Add(partition);
                partition.AssignedConsumer = targetConsumer;
            }

            Console.WriteLine($"[Assignment] Used LagAwareWorstFit strategy (Rebalance Time: {RebalanceTimeSeconds}s). Consumers={consumers.Count}");
        }
    }

    public class ConsumerGroup
    {
        public string GroupId { get; }
        public List<Partition> AllPartitions { get; }
        public List<Consumer> Consumers { get; private set; } = new List<Consumer>();

        private readonly IPartitionAssignmentStrategy _assignmentStrategy;
        private readonly double _consumerCapacity;
        public double _rebalanceTimeSeconds { get; } = 5.0; // rebalance blocking time
        public double _latencySLASeconds { get; } = 30.0;   // SLA window
        private readonly double _scaleUpHysteresis = 1.05; // require 5% headroom before scaling up
        private readonly double _scaleDownUtilizationThreshold = 0.20; // consider removing consumer if <20% utilized

        public ConsumerGroup(
            string groupId,
            List<Partition> partitions,
            double consumerCapacity,
            IPartitionAssignmentStrategy assignmentStrategy)
        {
            GroupId = groupId;
            AllPartitions = partitions;
            _consumerCapacity = consumerCapacity;
            _assignmentStrategy = assignmentStrategy;

            _assignmentStrategy.RebalanceTimeSeconds = _rebalanceTimeSeconds;
        }

        public Consumer AddConsumer()
        {
            var newConsumer = new Consumer($"C-{Consumers.Count + 1}", _consumerCapacity);
            Consumers.Add(newConsumer);
            MetricsExporter.SetConsumers(Consumers.Count);
            MetricsExporter.IncScaleUp();
            Console.WriteLine($"[SCALED UP] New Consumer {newConsumer.Id} added. Total: {Consumers.Count}");
            return newConsumer;
        }

        private void RemoveConsumer(Consumer consumer)
        {
            Console.WriteLine($"[SCALED DOWN] Removing Consumer {consumer.Id}...");

            // Unassign partitions so the next rebalance will reassign them.
            foreach (var partition in consumer.AssignedPartitions.ToList())
            {
                partition.AssignedConsumer = null;
                consumer.AssignedPartitions.Remove(partition);
            }

            Consumers.Remove(consumer);
            MetricsExporter.SetConsumers(Consumers.Count);
            MetricsExporter.IncScaleDown();
            Console.WriteLine($"[SCALED DOWN] Consumer {consumer.Id} removed. Total: {Consumers.Count}");
        }

        // Rebalance all partitions using the configured strategy.
        public void Rebalance()
        {
            Console.WriteLine($"--- REBALANCING (Blocking for {_rebalanceTimeSeconds}s) ---");
            _assignmentStrategy.Assign(AllPartitions, Consumers);

            // Update partition metrics labels after rebalance
            foreach (var p in AllPartitions)
            {
                MetricsExporter.SetPartition(p.Id, p.CurrentLag, p.ProductionRate);
                MetricsExporter.SetPartitionAssignment(p.Id, p.AssignedConsumer?.Id);
            }

            // Update consumer metrics after rebalance
            foreach (var c in Consumers)
            {
                double util = (c.GetCurrentWorkloadRate() / c.MaxCapacity) * 100.0;
                MetricsExporter.SetConsumerMetrics(c.Id, util, c.AssignedPartitions.Count);
            }
        }

        // Autoscaling based on ScaleWithLag (SLA-focused), with hysteresis and safe downscale.
        public void Autoscale()
        {
            // Conservative required capacity: rate + lag / SLA
            double totalRequiredCapacitySLA = AllPartitions
                .Sum(p => p.ProductionRate + (double)p.GetTotalLag(_rebalanceTimeSeconds) / _latencySLASeconds);

            int requiredConsumersSLA = (int)Math.Ceiling(totalRequiredCapacitySLA / _consumerCapacity);

            // Apply hysteresis for scaling up
            int targetConsumers = Consumers.Count;
            if (requiredConsumersSLA > Consumers.Count)
            {
                // only scale up if required > current * hysteresis
                if (requiredConsumersSLA >= Math.Ceiling(Consumers.Count * _scaleUpHysteresis))
                    targetConsumers = requiredConsumersSLA;
            }
            else if (requiredConsumersSLA < Consumers.Count)
            {
                // scale down only if there exists at least one under-utilized consumer
                var underUtilized = Consumers
                    .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * _scaleDownUtilizationThreshold)
                    .OrderBy(c => c.GetCurrentWorkloadRate())
                    .FirstOrDefault();

                if (underUtilized != null)
                    targetConsumers = Consumers.Count - 1;
            }

            if (targetConsumers > Consumers.Count)
            {
                int toAdd = targetConsumers - Consumers.Count;
                for (int i = 0; i < toAdd; i++) AddConsumer();
                Rebalance();
            }
            else if (targetConsumers < Consumers.Count)
            {
                // Remove one under-utilized consumer (safe downscale), then rebalance.
                var removable = Consumers
                    .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * _scaleDownUtilizationThreshold)
                    .OrderBy(c => c.GetCurrentWorkloadRate())
                    .FirstOrDefault();

                if (removable != null)
                {
                    RemoveConsumer(removable);
                    Rebalance();
                }
            }
        }
    }

    public class BrokerSimulator
    {
        private const double TimeStepSeconds = 1.0;
        private const double ConsumerBaseCapacity = 1000.0; // Msgs/sec per consumer

        public static void Main()
        {
            Console.WriteLine("Starting Kafka Autoscaling Simulation (Config-Driven)...");

            // Start metrics endpoint
            MetricsExporter.Init(1234);

            // Load config
            var configPath = Path.Combine(AppContext.BaseDirectory, "simulation_config.json");
            SimulationConfig? config = null;

            if (File.Exists(configPath))
            {
                try
                {
                    var json = File.ReadAllText(configPath);
                    config = JsonSerializer.Deserialize<SimulationConfig>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                    Console.WriteLine($"Loaded simulation config from {configPath}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to read config: {ex.Message}");
                }
            }

            // Fallback default config if none provided
            if (config == null)
            {
                config = new SimulationConfig
                {
                    InitialPartitions = new List<PartitionConfig>
                    {
                        new PartitionConfig { Id = "P-A", ProductionRate = 500, CurrentLag = 5000 },
                        new PartitionConfig { Id = "P-B", ProductionRate = 300, CurrentLag = 1000 },
                        new PartitionConfig { Id = "P-C", ProductionRate = 700, CurrentLag = 7000 },
                        new PartitionConfig { Id = "P-D", ProductionRate = 100, CurrentLag = 0 },
                    },
                    Events = new List<SimulationEvent>
                    {
                        new SimulationEvent { TimeStep = 20, Type = "change_rate", PartitionId = "P-C", ProductionRate = 1400 },
                        new SimulationEvent { TimeStep = 40, Type = "add_partition", PartitionId = "P-E", ProductionRate = 200, CurrentLag = 0 },
                        new SimulationEvent { TimeStep = 120, Type = "remove_partition", PartitionId = "P-B" }
                    },
                    MaxRuntimeSteps = 600
                };

                Console.WriteLine("Using built-in default simulation config.");
            }

            // Initialize partitions from config
            var partitions = config.InitialPartitions
                .Select(pc => new Partition(pc.Id) { ProductionRate = pc.ProductionRate, CurrentLag = pc.CurrentLag })
                .ToList();

            IPartitionAssignmentStrategy assignmentStrategy = new LagAwareWorstFitAssignment();

            var group = new ConsumerGroup("MyGroup", partitions, ConsumerBaseCapacity, assignmentStrategy);

            // Start with 1 consumer
            group.AddConsumer();
            group.Rebalance();

            // Index events by timestep for fast lookup
            var eventsByStep = config.Events.GroupBy(e => e.TimeStep).ToDictionary(g => g.Key, g => g.ToList());

            for (int step = 1; step <= config.MaxRuntimeSteps; step++)
            {
                Console.WriteLine($"\n--- SIMULATION STEP {step} ---");

                // Apply scheduled events at the start of the step
                if (eventsByStep.TryGetValue(step, out var scheduled))
                {
                    foreach (var ev in scheduled)
                    {
                        Console.WriteLine($"[EVENT] Step {step}: {ev.Type} {ev.PartitionId}");

                        if (string.Equals(ev.Type, "add_partition", StringComparison.OrdinalIgnoreCase))
                        {
                            if (!group.AllPartitions.Any(p => p.Id == ev.PartitionId))
                            {
                                var newP = new Partition(ev.PartitionId) { ProductionRate = ev.ProductionRate ?? 0, CurrentLag = ev.CurrentLag ?? 0 };
                                group.AllPartitions.Add(newP);
                                Console.WriteLine($"[EVENT] Added partition {newP.Id} rate={newP.ProductionRate} lag={newP.CurrentLag}");
                            }
                        }
                        else if (string.Equals(ev.Type, "remove_partition", StringComparison.OrdinalIgnoreCase))
                        {
                            var toRemove = group.AllPartitions.FirstOrDefault(p => p.Id == ev.PartitionId);
                            if (toRemove != null)
                            {
                                // Unassign from consumer if needed
                                if (toRemove.AssignedConsumer != null)
                                {
                                    toRemove.AssignedConsumer.AssignedPartitions.Remove(toRemove);
                                    toRemove.AssignedConsumer = null;
                                }
                                group.AllPartitions.Remove(toRemove);
                                Console.WriteLine($"[EVENT] Removed partition {ev.PartitionId}");
                            }
                        }
                        else if (string.Equals(ev.Type, "change_rate", StringComparison.OrdinalIgnoreCase))
                        {
                            var p = group.AllPartitions.FirstOrDefault(x => x.Id == ev.PartitionId);
                            if (p != null && ev.ProductionRate.HasValue)
                            {
                                Console.WriteLine($"[EVENT] Changing rate {p.Id}: {p.ProductionRate} -> {ev.ProductionRate.Value}");
                                p.ProductionRate = ev.ProductionRate.Value;
                            }
                        }
                    }

                    // After structural changes, re-run assignment to ensure new partitions are placed.
                    group.Rebalance();
                }

                // Production
                group.AllPartitions.ForEach(p => p.Produce(TimeStepSeconds));

                // Consumption
                group.Consumers.ForEach(c => c.Consume(TimeStepSeconds));

                // Autoscale check every 5 seconds
                if (step % 5 == 0)
                {
                    group.Autoscale();
                }

                // Reporting
                Console.WriteLine($"Current Consumers: {group.Consumers.Count}");
                long totalLag = group.AllPartitions.Sum(p => p.CurrentLag);

                double maxLagTime = group.AllPartitions
                    .Where(p => p.CurrentLag > 0 && p.AssignedConsumer != null)
                    .DefaultIfEmpty()
                    .Max(p => p == null ? 0 : p.CurrentLag / (p.AssignedConsumer?.MaxCapacity ?? ConsumerBaseCapacity));

                Console.WriteLine($"Total System Lag: {totalLag} messages");
                Console.WriteLine($"Max Estimated Latency (Worst-Case): {maxLagTime:F2} seconds (Target: {group._latencySLASeconds}s)");

                // Update metrics
                MetricsExporter.SetTotalLag(totalLag);
                MetricsExporter.SetConsumers(group.Consumers.Count);
                foreach (var p in group.AllPartitions)
                {
                    MetricsExporter.SetPartition(p.Id, p.CurrentLag, p.ProductionRate);
                    MetricsExporter.SetPartitionAssignment(p.Id, p.AssignedConsumer?.Id);
                }
                foreach (var consumer in group.Consumers)
                {
                    double util = (consumer.GetCurrentWorkloadRate() / consumer.MaxCapacity) * 100.0;
                    MetricsExporter.SetConsumerMetrics(consumer.Id, util, consumer.AssignedPartitions.Count);
                }

                foreach (var consumer in group.Consumers)
                {
                    double utilization = (consumer.GetCurrentWorkloadRate() / consumer.MaxCapacity) * 100;

                    Console.WriteLine($"  {consumer.Id}: Rate={consumer.GetCurrentWorkloadRate():F0} msgs/s. Util={utilization:F1}%. Partitions: {string.Join(", ", consumer.AssignedPartitions.Select(p => p.Id))}");
                }

                if (totalLag == 0 && step > 10)
                {
                    Console.WriteLine("\nAll partitions cleared lag. Simulation Complete.");
                    break;
                }
            }
            Console.ReadLine();

            while (true)
            {
                System.Threading.Thread.Sleep(10000);
            }

            // Allow metrics server to be stopped gracefully
            MetricsExporter.Stop().Wait();

            Console.WriteLine("Simulation finished.");
        }
    }
}
