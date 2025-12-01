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
        ConsumerGroup? ConsumerGroup { get; set; }

        void Assign(List<Partition> partitions, List<Consumer> consumers);
        Task AutoScale();
    }

    // Lag-aware worst-fit assignment (place largest total-lag partitions on least-lagged consumer).
    public class LagAwareWorstFitAssignment : IPartitionAssignmentStrategy
    {
        public double RebalanceTimeSeconds { get; set; }
        public ConsumerGroup? ConsumerGroup { get; set; }

        public float _rebalanceTimeSeconds = 5.0f; // rebalance blocking time
        public float _latencySLASeconds = 30.0f;   // SLA window
        public float _scaleDownUtilizationThreshold = 0.20f; // consider removing consumer if <20% utilized
        public float _scaleUpHysteresis = 1.05f; // require 5% headroom before scaling up

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

        public Task AutoScale()
        {
            if (ConsumerGroup == null)
            {
                return Task.CompletedTask;
            }
            
            // Conservative required capacity: rate + lag / SLA


            var consumers = ConsumerGroup.Consumers;

            double totalRequiredCapacitySLA = ConsumerGroup.AllPartitions
                .Sum(p => p.ProductionRate + (double)p.GetTotalLag(_rebalanceTimeSeconds) / _latencySLASeconds);

            int requiredConsumersSLA = (int)Math.Ceiling(totalRequiredCapacitySLA / ConsumerGroup.ConsumerCapacity);

            // Apply hysteresis for scaling up
            int targetConsumers = consumers.Count;
            if (requiredConsumersSLA > consumers.Count)
            {
                // only scale up if required > current * hysteresis
                if (requiredConsumersSLA >= Math.Ceiling(consumers.Count * _scaleUpHysteresis))
                    targetConsumers = requiredConsumersSLA;
            }
            else if (requiredConsumersSLA < consumers.Count)
            {
                // scale down only if there exists at least one under-utilized consumer
                var underUtilized = consumers
                    .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * _scaleDownUtilizationThreshold)
                    .OrderBy(c => c.GetCurrentWorkloadRate())
                    .FirstOrDefault();

                if (underUtilized != null)
                    targetConsumers = consumers.Count - 1;
            }

            if (targetConsumers > consumers.Count)
            {
                int toAdd = targetConsumers - consumers.Count;
                for (int i = 0; i < toAdd; i++) ConsumerGroup.AddConsumer();
                ConsumerGroup.Rebalance();
            }
            else if (targetConsumers < consumers.Count)
            {
                // Remove one under-utilized consumer (safe downscale), then rebalance.
                var removable = consumers
                    .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * _scaleDownUtilizationThreshold)
                    .OrderBy(c => c.GetCurrentWorkloadRate())
                    .FirstOrDefault();

                if (removable != null)
                {
                    ConsumerGroup.RemoveConsumer(removable);
                    ConsumerGroup.Rebalance();
                }
            }

            return Task.CompletedTask;
        }
    }

    public class ModifiedWorstFitAssignment : IPartitionAssignmentStrategy
    {
        public double RebalanceTimeSeconds { get; set; }
        public ConsumerGroup? ConsumerGroup { get; set; }

        private const double ScaleDownUtilizationThreshold = 0.20; // Cost-aware threshold
        private const double ScaleUpHysteresis = 1.05;           // Cost-aware hysteresis

        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            if (!consumers.Any()) return;


            foreach (var c in consumers)
            {
                c.AssignedPartitions.Clear();
            }

            var unassignedPartitions = new List<Partition>();

            var sortedConsumers = consumers
                .OrderBy(c => c.GetCurrentTotalLag(RebalanceTimeSeconds))
                .ToList();

            foreach (var currentConsumer in sortedConsumers)
            {
                // Line 4: pset ← partitions assigned to c
                // Get partitions that were assigned to this consumer in the *previous* state.
                var pset = partitions
                    .Where(p => p.AssignedConsumer?.Id == currentConsumer.Id)
                    .ToList();

                // Line 5: pset ← sort pset in decreasing order (by lag/size)
                // We sort ascending to match the backward iteration logic (smallest item first)
                var sortedPSet = pset.OrderBy(p => p.GetTotalLag(RebalanceTimeSeconds)).ToList();

                int successfulPreservationCount = 0;

                // Line 6-13: Iterate over pset from smallest to largest partition to preserve existing assignments
                for (int i = 0; i < sortedPSet.Count; i++)
                {
                    var p = sortedPSet[i];

                    double currentWorkloadRate = currentConsumer.GetCurrentWorkloadRate();

                    // Line 8: result ← N.assignOpenBin(p). Capacity Constraint Check.
                    if (currentWorkloadRate + p.ProductionRate <= currentConsumer.MaxCapacity)
                    {
                        // Line 8 (result true): Keep the partition on this consumer.
                        currentConsumer.AssignedPartitions.Add(p);
                        p.AssignedConsumer = currentConsumer; // Update partition reference
                        successfulPreservationCount++;
                    }
                    else
                    {
                        // Line 9: if result = false then (Capacity constraint violated)
                        // Stop assigning remaining (larger) partitions to this consumer.
                        break; // Line 10: break
                    }
                }

                // Line 25: U.extend(pset) (Add remaining, non-preserved partitions to the global unassigned list U)
                var remainingPSet = sortedPSet.Skip(successfulPreservationCount).ToList();
                unassignedPartitions.AddRange(remainingPSet);

            }

            var newlyUnassigned = partitions.Where(p => p.AssignedConsumer == null).ToList();
            var finalU = unassignedPartitions.Union(newlyUnassigned).ToList();

            // Line 27: U ← sort U decreasing order (by lag/size)
            var sortedFinalU = finalU.OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds)).ToList();

            // Line 28: for p ∈ U do
            foreach (var partition in sortedFinalU)
            {
                // 1. Find "safe" consumers (must satisfy capacity constraint).
                var safeConsumers = consumers
                    .Where(c => c.GetCurrentWorkloadRate() + partition.ProductionRate <= c.MaxCapacity)
                    .ToList();

                Consumer targetConsumer;

                if (safeConsumers.Any())
                {
                    // Line 29: N.assignBin(p) (MWF: pick consumer that minimizes lag/is the least-loaded bin)
                    targetConsumer = safeConsumers
                        .OrderBy(c => c.GetCurrentTotalLag(RebalanceTimeSeconds))
                        .First();
                }
                else
                {
                    // Fallback for capacity violation
                    targetConsumer = consumers
                        .OrderBy(c => c.GetCurrentWorkloadRate())
                        .First();
                }

                // Assign the partition
                targetConsumer.AssignedPartitions.Add(partition);
                partition.AssignedConsumer = targetConsumer;
            } // Line 30: end for

            Console.WriteLine($"[Assignment] Used ModifiedWorstFit (MWF) strategy (Paper-exact logic). Consumers={consumers.Count}. Partitions to reassign: {sortedFinalU.Count}");
        }

        public Task AutoScale()
        {
            if(ConsumerGroup == null)
                return Task.CompletedTask;

            var consumers = ConsumerGroup.Consumers;
            var partitions = ConsumerGroup.AllPartitions;

            if (!partitions.Any())
            {
                // Simple check: if there's nothing to process, we aim for 0 consumers.
                // Downscale logic below will handle the safe removal of one consumer at a time.
                if (consumers.Count > 0)
                {

                }
                // downscale everything
            }

            double totalRateCapacity = partitions.Sum(p => p.ProductionRate);
            long totalProjectedLag = partitions.Sum(p => p.GetTotalLag(ConsumerGroup.RebalanceTimeSeconds));

            double totalRequiredCapacity = totalRateCapacity + (double)totalProjectedLag / ConsumerGroup.LatencySLASeconds;
            int requiredConsumers = (int)Math.Ceiling(totalRequiredCapacity / ConsumerGroup.ConsumerCapacity);

            // Handle case where we need at least one consumer if partitions exist
            if (partitions.Any() && requiredConsumers < 1) requiredConsumers = 1;

            // --- 2. Cost-Aware Hysteresis and Safe Downscale Logic ---

            int targetConsumers = consumers.Count;

            // Scale Up Logic (with Hysteresis)
            if (requiredConsumers > consumers.Count)
            {
                // Only scale up if required capacity exceeds current capacity by the hysteresis factor.
                if (requiredConsumers >= Math.Ceiling(consumers.Count * ScaleUpHysteresis))
                    targetConsumers = requiredConsumers;
            }
            else if (requiredConsumers < consumers.Count)
            {
                // Scale Down Logic (Safe Downscale Check)
                // Check if we can safely remove one consumer (i.e., if one is sufficiently under-utilized).
                var underUtilized = consumers
                    .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * ScaleDownUtilizationThreshold)
                    .OrderBy(c => c.GetCurrentWorkloadRate())
                    .FirstOrDefault();

                if (underUtilized != null)
                    targetConsumers = consumers.Count - 1;
            }

            if (targetConsumers > consumers.Count)
            {
                int toAdd = targetConsumers - consumers.Count;
                Console.WriteLine($"[AUTOSCALE] Scaling UP by {toAdd} consumers. Required={requiredConsumers}.");
                for (int i = 0; i < toAdd; i++) ConsumerGroup.AddConsumer();
                ConsumerGroup.Rebalance();
            }
            else if (targetConsumers < consumers.Count)
            {
                // Use the same underutilized consumer identified above for removal
                var removable = consumers
                    .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * ScaleDownUtilizationThreshold)
                    .OrderBy(c => c.GetCurrentWorkloadRate())
                    .FirstOrDefault();

                if (removable != null)
                {
                    Console.WriteLine($"[AUTOSCALE] Scaling DOWN by 1 consumer ({removable.Id}). Required={requiredConsumers}.");
                    ConsumerGroup.RemoveConsumer(removable);
                    ConsumerGroup.Rebalance();
                }
            }

            return Task.CompletedTask;
        }
    }

    public class ConsumerGroup
    {
        public string GroupId { get; }
        public List<Partition> AllPartitions { get; }
        public List<Consumer> Consumers { get; private set; } = new List<Consumer>();

        public double ConsumerCapacity => _consumerCapacity;

        private readonly IPartitionAssignmentStrategy _assignmentStrategy;
        private readonly double _consumerCapacity;

        public double RebalanceTimeSeconds { get; } = 5.0; // rebalance blocking time

        public double LatencySLASeconds { get; } = 30.0;   // SLA window

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
            _assignmentStrategy.RebalanceTimeSeconds = RebalanceTimeSeconds;
            _assignmentStrategy.ConsumerGroup = this;
        }

        public Consumer AddConsumer()
        {
            var newConsumer = new Consumer($"C-{Guid.NewGuid()}", _consumerCapacity);
            Consumers.Add(newConsumer);
            MetricsExporter.SetConsumers(Consumers.Count);
            MetricsExporter.IncScaleUp();
            Console.WriteLine($"[SCALED UP] New Consumer {newConsumer.Id} added. Total: {Consumers.Count}");
            return newConsumer;
        }

        public void RemoveConsumer(Consumer consumer)
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
            Console.WriteLine($"--- REBALANCING (Blocking for {RebalanceTimeSeconds}s) ---");
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
            _assignmentStrategy.AutoScale();
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

            IPartitionAssignmentStrategy assignmentStrategy = new ModifiedWorstFitAssignment();

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
                Console.WriteLine($"Max Estimated Latency (Worst-Case): {maxLagTime:F2} seconds (Target: {group.LatencySLASeconds}s)");

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
