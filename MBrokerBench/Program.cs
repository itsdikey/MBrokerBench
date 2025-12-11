using MBrokerBench.Models;
using System.Text.Json;
using System.Threading.Tasks;

namespace MBrokerBench
{

    public class ModifiedWorstFitAssignment : IPartitionAssignmentStrategy
    {
        public double RebalanceTimeSeconds { get; set; }
        public ConsumerGroup? ConsumerGroup { get; set; }

        private const double ScaleDownUtilizationThreshold = 0.20; // Cost-aware threshold

        private const double CapacityExcessFactor = 5.0 / 6.0; //  C = 5/6 * C

        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            if (ConsumerGroup == null)
            {
                return;
            }

            if (!consumers.Any()) return;


            foreach (var c in consumers)
            {
                c.AssignedPartitions.Clear();
            }

            var unassignedPartitions = new List<Partition>();

            var sortedConsumers = consumers
                .OrderBy(c => c.GetCurrentTotalLag(RebalanceTimeSeconds)) // cumulative partition sort
                .ToList();

            foreach (var currentConsumer in sortedConsumers)
            {
                // Line 4: pset ← partitions assigned to c
                // Get partitions that were assigned to this consumer in the *previous* state.
                var pset = new HashSet<Partition>(partitions
                    .Where(p => p.AssignedConsumer?.Id == currentConsumer.Id));

                // Line 5: pset ← sort pset in decreasing order (by lag/size)
                // We sort ascending to match the backward iteration logic (smallest item first)
                var sortedPSet = pset.OrderBy(p => p.GetTotalLag(RebalanceTimeSeconds)).ToList();

                int successfulPreservationCount = 0;

                // Line 6-13: Iterate over pset from smallest to largest partition to preserve existing assignments
                for (int i = sortedPSet.Count-1; i>=0; i--) 
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

                    pset.Remove(p); // Line 12: pset.remove(p)
                }

                if(pset.Count == 0) // Line 14: if pset.size() = 0 then
                {
                    // All partitions preserved for this consumer
                    continue;
                }


                //Line 17-24: Reassign remaining partitions in pset
                var remainingPSet = sortedPSet.Where(x => pset.Contains(x)).ToList();
                sortedPSet = remainingPSet.OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds)).ToList();

                var remainingPSetHash = new HashSet<Partition>(sortedPSet);

                var newConsumer = ConsumerGroup.AddConsumer(); // Line 17 createConsumer();

                foreach (var p in sortedPSet) // Line 18: for p ∈ pset do
                {
                    double currentWorkloadRate = newConsumer.GetCurrentWorkloadRate();

                    // Line 19: result ← N.assign(c, p). Capacity Constraint Check.
                    if (currentWorkloadRate + p.ProductionRate <= newConsumer.MaxCapacity)
                    {
                        // Line 20 (result true): Keep the partition on this consumer.
                        newConsumer.AssignedPartitions.Add(p);
                        p.AssignedConsumer = newConsumer; // Update partition reference
                        successfulPreservationCount++;

                    }
                    else
                    {
                        // Line 9: if result = false then (Capacity constraint violated)
                        // Stop assigning remaining (larger) partitions to this consumer.
                        break; // Line 10: break
                    }

                    remainingPSetHash.Remove(p); // Line 23: pset.remove(p)
                }

                // Line 25: U.extend(pset) (add unassigned partitions to U)
                unassignedPartitions.AddRange(remainingPSetHash);
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
                    // Line 29: N.assignBin(p) (MWF: pick consumer that minimizes is)
                    targetConsumer = safeConsumers
                        .OrderByDescending(c => c.RemainingCapacity)
                        .First();
                }
                else
                {
                    var created = ConsumerGroup.AddConsumer();

                    // Refresh the local consumer list reference
                    consumers = ConsumerGroup.Consumers;

                    // If the new consumer can take the partition, use it
                    if (created.GetCurrentWorkloadRate() + partition.ProductionRate <= created.MaxCapacity)
                    {
                        targetConsumer = created;
                    }
                    else
                    {
                        // Partition too large for a single consumer or other unexpected condition — pick worst fit available consumer
                        targetConsumer = consumers
                            .OrderByDescending(c => c.RemainingCapacity)
                            .First();
                    }
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
                if (consumers.Count > 0)
                {
                    var removable = consumers
                    .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * ScaleDownUtilizationThreshold)
                    .OrderBy(c => c.GetCurrentWorkloadRate())
                    .FirstOrDefault();

                    if (removable != null)
                    {
                        Console.WriteLine($"[AUTOSCALE] Scaling DOWN by 1 consumer ({removable.Id}).");
                        ConsumerGroup.RemoveConsumer(removable);
                        ConsumerGroup.Rebalance();
                    }
                }
            }

            double totalRateCapacity = partitions.Sum(p => p.ProductionRate);
            long totalProjectedLag = partitions.Sum(p => p.GetTotalLag(ConsumerGroup.RebalanceTimeSeconds));

            double totalRequiredCapacity = totalRateCapacity + totalProjectedLag / ConsumerGroup.LatencySLASeconds;
            int requiredConsumers = (int)Math.Ceiling(totalRequiredCapacity / (CapacityExcessFactor * ConsumerGroup.ConsumerCapacity));

            // Handle case where we need at least one consumer if partitions exist
            if (partitions.Any() && requiredConsumers < 1) requiredConsumers = 1;

            // --- 2. Cost-Aware Hysteresis and Safe Downscale Logic ---

            int targetConsumers = consumers.Count;

            if (requiredConsumers < consumers.Count)
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

    public class CostCentricModifiedWorstFitAssignment : IPartitionAssignmentStrategy
    {
        public double RebalanceTimeSeconds { get; set; }
        public ConsumerGroup? ConsumerGroup { get; set; }

        private const double ScaleDownUtilizationThreshold = 0.20; // Cost-aware threshold

        private const double CapacityExcessFactor = 1;// 5.0 / 6.0; //  C = 5/6 * C

        private ConsumerProfile GetOptimalProfile(double requiredCapacity)
        {
            if (ConsumerGroup == null) return ConsumerProfiles.Medium;

            // 1. Filter profiles that are large enough to hold the load (with safety margin)
            var candidates = ConsumerGroup.ConsumerProfiles
                .Where(p => p.MaxCapacity * CapacityExcessFactor >= requiredCapacity)
                .OrderBy(p => p.CostPerSecond) // Cheapest first
                .ToList();

            // 2. If valid candidates exist, pick the cheapest.
            if (candidates.Any())
            {
                return candidates.First();
            }

            // 3. If NONE fit (load is huge), pick the largest available profile 
            // (we will likely need multiple, but start with largest).
            return ConsumerGroup.ConsumerProfiles.OrderByDescending(p => p.MaxCapacity).First();
        }

        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            if (ConsumerGroup == null || !consumers.Any()) return;

            // Clear current mappings
            foreach (var c in consumers) c.AssignedPartitions.Clear();

            var unassignedPartitions = new List<Partition>();

            // Sort consumers by total lag (heuristic to process heavy consumers first)
            var sortedConsumers = consumers
                .OrderByDescending(c => c.GetCurrentTotalLag(RebalanceTimeSeconds))
                .ToList();

            // --- PHASE 1: Preserve existing assignments where possible ---
            foreach (var currentConsumer in sortedConsumers)
            {
                // Get partitions previously assigned to this specific consumer ID
                var pset = partitions
                    .Where(p => p.AssignedConsumer?.Id == currentConsumer.Id)
                    .OrderBy(p => p.GetTotalLag(RebalanceTimeSeconds)) // Smallest first
                    .ToList();

                var keptPartitions = new List<Partition>();
                var ejectedPartitions = new List<Partition>();

                double currentLoad = 0;

                // Greedy packing: Keep as many as fit
                foreach (var p in pset)
                {
                    if (currentLoad + p.ProductionRate <= currentConsumer.MaxCapacity * CapacityExcessFactor)
                    {
                        currentConsumer.AssignedPartitions.Add(p);
                        p.AssignedConsumer = currentConsumer;
                        currentLoad += p.ProductionRate;
                        keptPartitions.Add(p);
                    }
                    else
                    {
                        ejectedPartitions.Add(p);
                    }
                }

                // If partitions were ejected, we need a new home for them.
                // In MWF, we often create a new consumer immediately for ejected sets to maintain locality.
                if (ejectedPartitions.Any())
                {
                    // HETEROGENEOUS LOGIC: 
                    // Calculate exactly how much capacity we need for the ejected items
                    double requiredCap = ejectedPartitions.Sum(p => p.ProductionRate);

                    // Pick the best profile (Small/Medium/Large) for this specific overflow
                    var bestProfile = GetOptimalProfile(requiredCap);

                    var newConsumer = ConsumerGroup.AddConsumer(bestProfile.Name);

                    // Assign ejected partitions to the new consumer
                    foreach (var ep in ejectedPartitions)
                    {
                        // Note: If the new consumer is still too small (Corner case), 
                        // they go to unassigned.
                        if (newConsumer.GetCurrentWorkloadRate() + ep.ProductionRate <= newConsumer.MaxCapacity)
                        {
                            newConsumer.AssignedPartitions.Add(ep);
                            ep.AssignedConsumer = newConsumer;
                        }
                        else
                        {
                            unassignedPartitions.Add(ep);
                        }
                    }
                }
            }

            // --- PHASE 2: Handle Completely Unassigned / Orphaned Partitions ---
            var newlyUnassigned = partitions.Where(p => p.AssignedConsumer == null).ToList();
            var finalU = unassignedPartitions.Union(newlyUnassigned)
                                             .OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds))
                                             .ToList();

            foreach (var partition in finalU)
            {
                // Try to fit into ANY existing consumer (Modified Best Fit)
                // We prioritize consumers that have space, sorting by "Cost Efficiency" implies 
                // filling the expensive ones first to get our money's worth.
                var candidate = consumers
                    .Where(c => c.RemainingCapacity >= partition.ProductionRate)
                    .OrderBy(c => c.RemainingCapacity) // Worst Fit (leaves large gaps for others) or Best Fit?
                    .FirstOrDefault();

                if (candidate != null)
                {
                    candidate.AssignedPartitions.Add(partition);
                    partition.AssignedConsumer = candidate;
                }
                else
                {
                    // No one has space. Spin up a new Consumer just for this partition 
                    // (and potentially subsequent ones).
                    // We look ahead slightly? No, keeping it simple: match this partition size.
                    var bestProfile = GetOptimalProfile(partition.ProductionRate);
                    var newConsumer = ConsumerGroup.AddConsumer(bestProfile.Name);

                    newConsumer.AssignedPartitions.Add(partition);
                    partition.AssignedConsumer = newConsumer;

                    // Update our local list so subsequent iterations see this new consumer
                    consumers = ConsumerGroup.Consumers;
                }
            }

            Console.WriteLine($"[Assignment] Heterogeneous Assignment Complete. Consumers: {consumers.Count}. Total Cost: ${ConsumerGroup.TotalCostPerSecond:F2}/s");
        }

        public Task AutoScale()
        {
            if (ConsumerGroup == null) return Task.CompletedTask;

            var consumers = ConsumerGroup.Consumers;
            var partitions = ConsumerGroup.AllPartitions;

            // --- HETEROGENEOUS SCALE DOWN LOGIC ---

            // We want to find a consumer that we can delete. 
            // Criteria:
            // 1. Its partitions MUST fit into the remaining consumers.
            // 2. We want to delete the one with the Worst "Value" (High Cost, Low Utilization).

            var candidatesForRemoval = consumers
                .Select(c => new
                {
                    Consumer = c,
                    // Metric: Cost per unit of work currently being done. Higher is worse (more wasteful).
                    // If utilization is 0, score is infinite (remove immediately).
                    InefficiencyScore = c.GetCurrentWorkloadRate() == 0 ? double.MaxValue : c.ConsumerProfile.CostPerSecond / c.GetCurrentWorkloadRate()
                })
                .OrderByDescending(x => x.InefficiencyScore) // Look at most inefficient first
                .ToList();

            foreach (var item in candidatesForRemoval)
            {
                var candidate = item.Consumer;
                double loadToRelocate = candidate.GetCurrentWorkloadRate();

                // Calculate total Slack (free space) in the rest of the fleet
                double totalSlackInOthers = consumers
                    .Where(c => c.Id != candidate.Id)
                    .Sum(c => c.RemainingCapacity);

                // Simple check: Is there enough math space globally?
                // (Constraint: We ideally want to check if individual partitions fit, 
                // but global slack is a good enough proxy for a simulation heuristic).
                if (totalSlackInOthers > loadToRelocate * 1.1) // 10% buffer
                {
                    Console.WriteLine($"[AUTOSCALE] Efficiency Check: Removing {candidate.Id} ({candidate.ConsumerProfile.Name}). Saving ${candidate.ConsumerProfile.CostPerSecond}/s");
                    ConsumerGroup.RemoveConsumer(candidate);
                    ConsumerGroup.Rebalance();
                    return Task.CompletedTask; // Only remove one per interval to be safe
                }
            }

            // --- SCALE UP IS HANDLED BY ASSIGNMENT ---
            // In MWF, if load exists that doesn't fit, 'Assign' creates consumers. 
            // However, we can do a proactive check here if we want to ensure headroom.

            return Task.CompletedTask;
        }
    }

    //not working as intended yet
    public class ConsolidatingCostCentricModifiedWorstFitAssignment : IPartitionAssignmentStrategy
    {
        public double RebalanceTimeSeconds { get; set; }
        public ConsumerGroup? ConsumerGroup { get; set; }

        private const double ScaleDownUtilizationThreshold = 0.20; // Cost-aware threshold

        private const double CapacityExcessFactor = 1;// 5.0 / 6.0; //  C = 5/6 * C

        private ConsumerProfile GetOptimalProfile(double requiredCapacity)
        {
            if (ConsumerGroup == null) return ConsumerProfiles.Medium;

            // 1. Filter profiles that are large enough to hold the load (with safety margin)
            var candidates = ConsumerGroup.ConsumerProfiles
                .Where(p => p.MaxCapacity * CapacityExcessFactor >= requiredCapacity)
                .OrderBy(p => p.CostPerSecond) // Cheapest first
                .ToList();

            // 2. If valid candidates exist, pick the cheapest.
            if (candidates.Any())
            {
                return candidates.First();
            }

            // 3. If NONE fit (load is huge), pick the largest available profile 
            // (we will likely need multiple, but start with largest).
            return ConsumerGroup.ConsumerProfiles.OrderByDescending(p => p.MaxCapacity).First();
        }

        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            if (ConsumerGroup == null || !consumers.Any()) return;

            // Clear current mappings
            foreach (var c in consumers) c.AssignedPartitions.Clear();

            var unassignedPartitions = new List<Partition>();

            // Sort consumers by total lag (heuristic to process heavy consumers first)
            var sortedConsumers = consumers
                .OrderByDescending(c => c.GetCurrentTotalLag(RebalanceTimeSeconds))
                .ToList();

            // --- PHASE 1: Preserve existing assignments where possible ---
            foreach (var currentConsumer in sortedConsumers)
            {
                // Get partitions previously assigned to this specific consumer ID
                var pset = partitions
                    .Where(p => p.AssignedConsumer?.Id == currentConsumer.Id)
                    .OrderBy(p => p.GetTotalLag(RebalanceTimeSeconds)) // Smallest first
                    .ToList();

                var keptPartitions = new List<Partition>();
                var ejectedPartitions = new List<Partition>();

                double currentLoad = 0;

                // Greedy packing: Keep as many as fit
                foreach (var p in pset)
                {
                    if (currentLoad + p.ProductionRate <= currentConsumer.MaxCapacity * CapacityExcessFactor)
                    {
                        currentConsumer.AssignedPartitions.Add(p);
                        p.AssignedConsumer = currentConsumer;
                        currentLoad += p.ProductionRate;
                        keptPartitions.Add(p);
                    }
                    else
                    {
                        ejectedPartitions.Add(p);
                    }
                }

                // If partitions were ejected, we need a new home for them.
                // In MWF, we often create a new consumer immediately for ejected sets to maintain locality.
                if (ejectedPartitions.Any())
                {
                    // HETEROGENEOUS LOGIC: 
                    // Calculate exactly how much capacity we need for the ejected items
                    double requiredCap = ejectedPartitions.Sum(p => p.ProductionRate);

                    // Pick the best profile (Small/Medium/Large) for this specific overflow
                    var bestProfile = GetOptimalProfile(requiredCap);

                    var newConsumer = ConsumerGroup.AddConsumer(bestProfile.Name);

                    // Assign ejected partitions to the new consumer
                    foreach (var ep in ejectedPartitions)
                    {
                        // Note: If the new consumer is still too small (Corner case), 
                        // they go to unassigned.
                        if (newConsumer.GetCurrentWorkloadRate() + ep.ProductionRate <= newConsumer.MaxCapacity)
                        {
                            newConsumer.AssignedPartitions.Add(ep);
                            ep.AssignedConsumer = newConsumer;
                        }
                        else
                        {
                            unassignedPartitions.Add(ep);
                        }
                    }
                }
            }

            // --- PHASE 2: Handle Completely Unassigned / Orphaned Partitions ---
            var newlyUnassigned = partitions.Where(p => p.AssignedConsumer == null).ToList();
            var finalU = unassignedPartitions.Union(newlyUnassigned)
                                             .OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds))
                                             .ToList();

            foreach (var partition in finalU)
            {
                // Try to fit into ANY existing consumer (Modified Best Fit)
                // We prioritize consumers that have space, sorting by "Cost Efficiency" implies 
                // filling the expensive ones first to get our money's worth.
                var candidate = consumers
                    .Where(c => c.RemainingCapacity >= partition.ProductionRate)
                    .OrderBy(c => c.RemainingCapacity) // Worst Fit (leaves large gaps for others) or Best Fit?
                    .FirstOrDefault();

                if (candidate != null)
                {
                    candidate.AssignedPartitions.Add(partition);
                    partition.AssignedConsumer = candidate;
                }
                else
                {
                    // No one has space. Spin up a new Consumer just for this partition 
                    // (and potentially subsequent ones).
                    // We look ahead slightly? No, keeping it simple: match this partition size.
                    var bestProfile = GetOptimalProfile(partition.ProductionRate);
                    var newConsumer = ConsumerGroup.AddConsumer(bestProfile.Name);

                    newConsumer.AssignedPartitions.Add(partition);
                    partition.AssignedConsumer = newConsumer;

                    // Update our local list so subsequent iterations see this new consumer
                    consumers = ConsumerGroup.Consumers;
                }
            }

            Console.WriteLine($"[Assignment] Heterogeneous Assignment Complete. Consumers: {consumers.Count}. Total Cost: ${ConsumerGroup.TotalCostPerSecond:F2}/s");
        }

        public Task AutoScale()
        {
            if (ConsumerGroup == null) return Task.CompletedTask;

            // 1. Try Standard Scale Down (Remove empty/inefficient nodes into existing slack)
            bool scaledDown = TryStandardScaleDown();
            if (scaledDown) return Task.CompletedTask;

            // 2. NEW: Try Consolidation (Merge multiple small nodes into one optimized node)
            // Only run this if we didn't just scale down (to avoid too much churn at once)
            TryConsolidateConsumers();

            return Task.CompletedTask;
        }

        private bool TryStandardScaleDown()
        {
            var consumers = ConsumerGroup.Consumers;

            // Find the most inefficient consumer (Cost / Load)
            var candidates = consumers
                .OrderByDescending(c => c.GetCurrentWorkloadRate() == 0 ? double.MaxValue : c.ConsumerProfile.CostPerSecond / c.GetCurrentWorkloadRate())
                .ToList();

            double totalGlobalSlack = consumers.Sum(c => c.RemainingCapacity);

            foreach (var candidate in candidates)
            {
                double load = candidate.GetCurrentWorkloadRate();
                double slackWithoutCandidate = totalGlobalSlack - candidate.RemainingCapacity;

                // If the rest of the fleet can absorb this load
                if (slackWithoutCandidate > load * 1.1)
                {
                    Console.WriteLine($"[AUTOSCALE] Standard Scale Down: Removing {candidate.Id}");
                    ConsumerGroup.RemoveConsumer(candidate);
                    ConsumerGroup.Rebalance();
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Looks for opportunities to replace multiple small consumers with one larger, cheaper consumer.
        /// </summary>
        private void TryConsolidateConsumers()
        {
            var consumers = ConsumerGroup.Consumers;
            if (consumers.Count < 2) return;

            // Sort by load (ascending) to try merging smallest consumers first
            var sortedConsumers = consumers.OrderBy(c => c.GetCurrentWorkloadRate()).ToList();

            // Simple Heuristic: Sliding Window of 2 consumers
            // (You can expand this to window size 3 or 4 if needed)
            for (int i = 0; i < sortedConsumers.Count - 1; i++)
            {
                var c1 = sortedConsumers[i];
                var c2 = sortedConsumers[i + 1];

                // 1. Calculate Combined State
                double combinedLoad = c1.GetCurrentWorkloadRate() + c2.GetCurrentWorkloadRate();
                double combinedCost = c1.ConsumerProfile.CostPerSecond + c2.ConsumerProfile.CostPerSecond;

                // 2. Find the optimal single profile for this combined load
                // (We reuse the helper from the previous step)
                var bestProfile = GetOptimalProfile(combinedLoad);

                // 3. The "Upgrade" Condition:
                // Is the new single profile CHEAPER than the two existing ones?
                // AND does it actually fit the load?
                bool isCheaper = bestProfile.CostPerSecond < combinedCost;
                bool fits = bestProfile.MaxCapacity >= combinedLoad * 1.05; // 5% safety buffer

                if (isCheaper && fits)
                {
                    Console.WriteLine($"[AUTOSCALE] CONSOLIDATION FOUND!");
                    Console.WriteLine($"   Merging {c1.ConsumerProfile.Name} (${c1.ConsumerProfile.CostPerSecond}) + {c2.ConsumerProfile.Name} (${c2.ConsumerProfile.CostPerSecond})");
                    Console.WriteLine($"   Into -> {bestProfile.Name} (${bestProfile.CostPerSecond})");
                    Console.WriteLine($"   Saving: ${combinedCost - bestProfile.CostPerSecond:F2}/sec");

                    // Execute the swap
                    // 1. Add the new efficient consumer
                    var newConsumer = ConsumerGroup.AddConsumer(bestProfile.Name);

                    // 2. Remove the old ones (Rebalance will move partitions to the new guy automatically)
                    ConsumerGroup.RemoveConsumer(c1);
                    ConsumerGroup.RemoveConsumer(c2);

                    ConsumerGroup.Rebalance();
                    return; // Only one merge per interval to maintain stability
                }
            }
        }
    }

    public class BrokerSimulator
    {
        private const double TimeStepSeconds = 1.0;

        public static async Task Main()
        {
            Console.WriteLine("Starting Kafka Autoscaling Simulation (Config-Driven)...");

            IPartitionAssignmentStrategy assignmentStrategy = new CostCentricModifiedWorstFitAssignment();

            // Start metrics endpoint with strategy/run labels (from environment)
            var strategyEnv = assignmentStrategy.GetType().Name ?? System.Environment.GetEnvironmentVariable("STRATEGY");
            var runIdEnv = System.Environment.GetEnvironmentVariable("RUN_ID") ?? DateTimeOffset.UtcNow.Subtract(DateTimeOffset.UnixEpoch).TotalSeconds.ToString();//current unix epoch 
            MetricsExporter.Init(1234, strategyEnv, runIdEnv);

            // Load config
            var configPath = Path.Combine(AppContext.BaseDirectory, "simulation_config.json");
            SimulationConfig? config = null;

            if (File.Exists(configPath))
            {
                try
                {
                    var json = File.ReadAllText(configPath);
                    var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

                    using (var doc = JsonDocument.Parse(json))
                    {
                        var root = doc.RootElement;

                        if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("Configs", out var configsElem))
                        {
                            string? activeTest = null;
                            if (root.TryGetProperty("ActiveTest", out var at))
                                activeTest = at.GetString();

                            JsonElement? selected = null;

                            foreach (var c in configsElem.EnumerateArray())
                            {
                                if (selected == null && (!string.IsNullOrEmpty(activeTest) && c.GetProperty("Name").GetString() == activeTest))
                                {
                                    selected = c;
                                }
                                else if (selected == null && string.IsNullOrEmpty(activeTest))
                                {
                                    // pick first if no active specified
                                    selected = c;
                                }
                            }

                            if (selected == null)
                                selected = configsElem.EnumerateArray().FirstOrDefault();

                            if (selected != null)
                            {
                                var sel = selected.Value;
                                config = new SimulationConfig();

                                // InitialPartitions (explicit list)
                                if (sel.TryGetProperty("InitialPartitions", out var ip))
                                {
                                    config.InitialPartitions = JsonSerializer.Deserialize<List<PartitionConfig>>(ip.GetRawText(), options) ?? new List<PartitionConfig>();
                                }
                                // InitialPartitionsConfig (generation spec)
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
                                    var list = new List<PartitionConfig>();
                                    for (int i = 0; i < count; i++)
                                    {
                                        double rate = prMin + rnd.NextDouble() * (prMax - prMin);
                                        long lag = rnd.NextInt64(lagMin, lagMax + 1);
                                        list.Add(new PartitionConfig { Id = $"P-{i + 1}", ProductionRate = rate, CurrentLag = lag });
                                    }

                                    config.InitialPartitions = list;
                                }

                                // Events
                                if (sel.TryGetProperty("Events", out var ev))
                                {
                                    config.Events = JsonSerializer.Deserialize<List<SimulationEvent>>(ev.GetRawText(), options) ?? new List<SimulationEvent>();
                                }

                                // MaxRuntimeSteps
                                if (sel.TryGetProperty("MaxRuntimeSteps", out var mrs))
                                {
                                    config.MaxRuntimeSteps = mrs.GetInt32();
                                }
                            }
                        }
                        else
                        {
                            // Legacy single-config file
                            try
                            {
                                config = JsonSerializer.Deserialize<SimulationConfig>(json, options);
                                Console.WriteLine($"Loaded simulation config from {configPath}");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Failed to parse legacy config: {ex.Message}");
                            }
                        }
                    }
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

            // Support for periodic refresh of production rates (from InitialPartitionsConfig)
            double rateVariability = 0.0;
            int rateRefreshIntervalSteps = 0;
            var baseProductionRates = new Dictionary<string, double>();

            // If the config file used the generation spec, we may have embedded variability values; try to read them
            try
            {
                using var doc = JsonDocument.Parse(File.ReadAllText(configPath));
                var root = doc.RootElement;
                JsonElement? selected = null;
                if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("Configs", out var configsElem))
                {
                    string? activeTest = null;
                    if (root.TryGetProperty("ActiveTest", out var at))
                        activeTest = at.GetString();

                    foreach (var c in configsElem.EnumerateArray())
                    {
                        if (selected == null && (!string.IsNullOrEmpty(activeTest) && c.GetProperty("Name").GetString() == activeTest))
                        {
                            selected = c;
                        }
                        else if (selected == null && string.IsNullOrEmpty(activeTest))
                        {
                            selected = c;
                        }
                    }

                    if (selected == null)
                        selected = configsElem.EnumerateArray().FirstOrDefault();

                    if (selected != null)
                    {
                        var sel = selected.Value;
                        if (sel.TryGetProperty("InitialPartitionsConfig", out var ipc))
                        {
                            if (ipc.TryGetProperty("RateVariability", out var rv))
                                rateVariability = rv.GetDouble();
                            if (ipc.TryGetProperty("RateRefreshIntervalSteps", out var rri))
                                rateRefreshIntervalSteps = rri.GetInt32();
                        }
                    }
                }
            }
            catch { /* ignore parsing errors here - defaults will be used */ }

            // record base rates for periodic refresh
            foreach (var p in partitions)
                baseProductionRates[p.Id] = p.ProductionRate;

            var group = new ConsumerGroup("MyGroup", partitions, ConsumerProfiles.AllProfiles, ConsumerProfiles.Medium, assignmentStrategy);

            // Start with 1 consumer
            group.AddConsumer();
            group.Rebalance();

            // Index events by timestep for fast lookup
            var eventsByStep = config.Events.GroupBy(e => e.TimeStep).ToDictionary(g => g.Key, g => g.ToList());

            var rndRate = new Random();

            for (int step = 1; step <= config.MaxRuntimeSteps; step++)
            {
                Console.WriteLine($"\n--- SIMULATION STEP {step} ---");

                // Periodic production rate refresh
                if (rateRefreshIntervalSteps > 0 && step > 0 && step % rateRefreshIntervalSteps == 0)
                {
                    Console.WriteLine($"[RATE REFRESH] Updating production rates with variability={rateVariability:P0} at step {step}");
                    foreach (var p in group.AllPartitions)
                    {
                        if (!baseProductionRates.TryGetValue(p.Id, out var baseRate))
                            baseRate = p.ProductionRate;

                        // newRate = base * (1 +/- variability)
                        var delta = (rndRate.NextDouble() * 2.0) - 1.0; // -1..1
                        var newRate = baseRate * (1.0 + delta * rateVariability);
                        if (newRate < 0) newRate = 0;
                        p.ProductionRate = newRate;

                        Console.WriteLine($"  {p.Id}: rate {baseRate:F1} -> {p.ProductionRate:F1}");

                        // update base map if you want variability around updated base instead of initial base
                        // baseProductionRates[p.Id] = p.ProductionRate;
                    }

                    // After changing rates, update assignment if desired
                    group.Rebalance();
                }

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
                    .Max(p => p == null ? 0 : p.CurrentLag / (p.AssignedConsumer?.MaxCapacity ?? 1000));

                var totalProductionRate = group.AllPartitions.Sum(p => p.ProductionRate);
                var averageProductionRate = group.AllPartitions.Count > 0 ? totalProductionRate / group.AllPartitions.Count : 0.0;


                Console.WriteLine($"Total System Lag: {totalLag} messages. Total Production Rate: {totalProductionRate:F1} msgs/s. Average Production Rate: {averageProductionRate:F1} msgs/s");
                Console.WriteLine($"Max Estimated Latency (Worst-Case): {maxLagTime:F2} seconds (Target: {group.LatencySLASeconds}s)");
                Console.WriteLine($"Total System Cost: {group.TotalCostPerSecond}");

                // Update metrics
                MetricsExporter.SetTotalLag(totalLag);
                MetricsExporter.SetConsumers(group.Consumers.Count);
                foreach (var p in group.AllPartitions)
                {
                    MetricsExporter.SetPartition(p.Id, p.CurrentLag, p.ProductionRate);
                    MetricsExporter.SetPartitionAssignment(p.Id, p.AssignedConsumer?.Id);
                }

                MetricsExporter.SetTotalProductionRate(totalProductionRate);

                foreach (var consumer in group.Consumers)
                {
                    double util = (consumer.GetCurrentWorkloadRate() / consumer.MaxCapacity) * 100.0;
                    MetricsExporter.SetConsumerMetrics(consumer.Id, util, consumer.AssignedPartitions.Count);
                }

                foreach (var consumer in group.Consumers)
                {
                    double utilization = (consumer.GetCurrentWorkloadRate() / consumer.MaxCapacity) * 100;

                    double consumerLag = consumer.GetCurrentTotalLag(0);

                    Console.WriteLine($"  {consumer.Id}: Profile={consumer.ConsumerProfile.ShortCode} Messages={consumerLag:F0} msg Rate={consumer.GetCurrentWorkloadRate():F0} msgs/s. Util={utilization:F1}%. Partitions: {string.Join(", ", consumer.AssignedPartitions.Select(p => p.Id))}");
                }

                Thread.Sleep(200);

                //if (totalLag == 0 && step > 10)
                //{
                //    Console.WriteLine("\nAll partitions cleared lag. Simulation Complete.");
                //    break;
                //}
            }

            await MetricsExporter.Finalizer();

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
