using MBrokerBench.Models;
using System.Text.Json;
using System.Threading.Tasks;
using System.Text;
using System.IO;
using MBrokerBench.Components;

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


        private const double CapacityExcessFactor = 5/ (double) 6;// 5.0 / 6.0; //  C = 5/6 * C

        public double SLA => ConsumerGroup?.LatencySLASeconds??10;

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
                    .OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds)) // Largenst first
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
                // Try to fit into ANY existing consumer (Modified Worst Fit)
                // We prioritize consumers that have space, sorting by "Cost Efficiency" implies 
                // filling the expensive ones first to get our money's worth.
                var candidate = consumers
                    .Where(c => c.RemainingCapacity >= partition.GetRequiredThroughput(SLA))
                    .OrderBy(c => c.RemainingCapacity) // Worst Fit (leaves large gaps for others)
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
                    var bestProfile = GetOptimalProfile(partition.GetRequiredThroughput(SLA));
                    var newConsumer = ConsumerGroup.AddConsumer(bestProfile.Name);

                    newConsumer.AssignedPartitions.Add(partition);
                    partition.AssignedConsumer = newConsumer;

                    // Update our local list so subsequent iterations see this new consumer
                    consumers = ConsumerGroup.Consumers;
                }
            }

            var extraConsumers = consumers.Where(x => !x.AssignedPartitions.Any()).ToList();

            foreach (var consumer in extraConsumers)
            {
                ConsumerGroup.RemoveConsumer(consumer);
            }

            Console.WriteLine($"[Assignment] Heterogeneous Assignment Complete. Consumers: {consumers.Count}. Total Cost: ${ConsumerGroup.TotalCostPerSecond:F2}/s");
        }

        private double _previousTotalLag = double.MaxValue;

        public Task AutoScale()
        {
            if (ConsumerGroup == null) return Task.CompletedTask;

            var consumers = ConsumerGroup.Consumers;
            var partitions = ConsumerGroup.AllPartitions;

            double totalLag = partitions.Sum(p => p.CurrentLag);

            if (totalLag > _previousTotalLag * 1.05)
            {
                Console.WriteLine("[AUTOSCALE] Scaling down ABORTED: System lag is increasing.");
                _previousTotalLag = totalLag;
                return Task.CompletedTask;
            }

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
                    InefficiencyScore = c.GetCurrentAndFutureWorkloadRate(SLA) == 0 ? double.MaxValue : c.ConsumerProfile.CostPerSecond / c.GetCurrentAndFutureWorkloadRate(SLA)
                })
                .OrderByDescending(x => x.InefficiencyScore) // Look at most inefficient first
                .ToList();

            var removals = new List<Consumer>();

            foreach (var item in candidatesForRemoval)
            {
                var candidate = item.Consumer;

                var survivors = consumers.Where(c => c.Id != candidate.Id).ToList();

                var loadToMove = candidate.AssignedPartitions.ToList();

                if (CanRelocateLoad(loadToMove, survivors))
                {
                    Console.WriteLine($"[AUTOSCALE] Efficiency Check: Removing {candidate.Id}. " +
                                      $"SLA Guaranteed via Bin Packing simulation.");

                    ConsumerGroup.RemoveConsumer(candidate);
                    ConsumerGroup.Rebalance();
                    return Task.CompletedTask; // Remove only one at a time for stability
                }

                continue;

                double loadToRelocate = candidate.GetCurrentAndFutureWorkloadRate(SLA) + candidate.GetRebalanceCost(RebalanceTimeSeconds) / SLA;

                // Calculate total Slack (free space) in the rest of the fleet
                double maxSlackLeft = consumers
                    .Where(c => c.Id != candidate.Id && !removals.Contains(c))
                    .Max(c => c.RemainingCapacity);

                // Simple check: Is there enough math space in any partition
                if (maxSlackLeft * CapacityExcessFactor > loadToRelocate) // 10% buffer
                {
                    Console.WriteLine($"[AUTOSCALE] Efficiency Check: Removing {candidate.Id} ({candidate.ConsumerProfile.Name}). Saving ${candidate.ConsumerProfile.CostPerSecond}/s");
                    removals.Add(candidate);
                }
            }

            Console.ReadLine();

            foreach (var candidate in removals)
            {
                ConsumerGroup.RemoveConsumer(candidate);
               // break; // One at a time for stability
            }
            
            ConsumerGroup.Rebalance();


            // --- SCALE UP IS HANDLED BY ASSIGNMENT ---
            // In MWF, if load exists that doesn't fit, 'Assign' creates consumers. 
            // However, we can do a proactive check here if we want to ensure headroom.

            return Task.CompletedTask;
        }

        private bool CanRelocateLoad(List<Partition> partitionsToMove, List<Consumer> survivors)
        {
            // 1. Calculate the *Real* Available Space on each survivor.
            // We must respect the SLA on the survivors too! Their current load is NOT just production rate,
            // but the rate required to keep their own current lag in check.
            var survivorCapacities = survivors.Select(c => new
            {
                Id = c.Id,
                // Max Cap (with safety margin) - Current Required Throughput (SLA based)
                AvailableSpace = (c.MaxCapacity * CapacityExcessFactor) -
                                 c.AssignedPartitions.Sum(p => p.GetRequiredThroughput(SLA))
            }).ToList();

            // 2. We use a dictionary or list to track the simulation state so we don't modify the actual objects
            // Map: ConsumerIndex -> Remaining Capacity
            var simulationSpace = survivorCapacities.Select(x => x.AvailableSpace).ToList();

            // 3. Sort partitions to move: Largest (Hardest to fit) First
            var sortedPartitions = partitionsToMove
                .Select(p => p.GetRequiredThroughput(SLA))
                .OrderByDescending(load => load)
                .ToList();

            // 4. Try to pack every partition
            foreach (var partitionLoad in sortedPartitions)
            {
                bool placed = false;

                // Try to find a survivor with enough space (Best Fit or First Fit)
                // First Fit is usually fine for a safety check.
                for (int i = 0; i < simulationSpace.Count; i++)
                {
                    if (simulationSpace[i] >= partitionLoad)
                    {
                        // Place it here (virtually)
                        simulationSpace[i] -= partitionLoad;
                        placed = true;
                        break;
                    }
                }

                // If we couldn't place this single partition anywhere, the whole plan fails.
                if (!placed)
                {
                    return false;
                }
            }

            // If we get here, everything fits!
            return true;
        }
    }
    public class SmartModifiedWorstFitStrategy : IPartitionAssignmentStrategy
    {
        public double RebalanceTimeSeconds { get; set; }
        public ConsumerGroup? ConsumerGroup { get; set; }

        private const double CapacitySafetyMargin = 5/(double)6;

        // The "Virtual Cost" of moving a partition. 
        // We only merge if (Savings/sec * StabilityWindow) > (NumPartitionsMoved * ReassignmentPenalty)
        // High penalty = Fewer Reassignments (Higher Stability). Low penalty = Aggressive Cost Optimization.
        private const double ReassignmentPenaltyCost = 0.50;

        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            if (ConsumerGroup == null || !consumers.Any()) return;

            // 1. Clear current mappings internally (MWF relies on 'AssignedConsumer' prop)
            foreach (var c in consumers) c.AssignedPartitions.Clear();

            // 2. Identify Assignments to KEEP vs EJECT
            // We use standard MWF logic here: iterate consumers, keep largest partitions that fit.
            var unassigned = new List<Partition>();
            var sortedConsumers = consumers.OrderByDescending(c => c.GetCurrentTotalLag(RebalanceTimeSeconds)).ToList();

            foreach (var consumer in sortedConsumers)
            {
                // Get partitions previously owned by this consumer
                var prevOwned = partitions
                    .Where(p => p.AssignedConsumer?.Id == consumer.Id)
                    .OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds)) // keep large ones
                    .ToList();

                foreach (var p in prevOwned)
                {
                    // Check if it still fits
                    if (consumer.GetCurrentWorkloadRate() + p.ProductionRate <= consumer.MaxCapacity * CapacitySafetyMargin)
                    {
                        consumer.AssignedPartitions.Add(p);
                        p.AssignedConsumer = consumer; // Re-link
                    }
                    else
                    {
                        unassigned.Add(p); // Overflow
                    }
                }
            }

            // 3. Add any completely new partitions to unassigned list
            var newPartitions = partitions.Where(p => p.AssignedConsumer == null && !unassigned.Contains(p));
            unassigned.AddRange(newPartitions);

            // 4. Assign Unassigned Partitions
            // Sort Descending (Bin Packing Best Practice)
            unassigned = unassigned.OrderByDescending(p => p.ProductionRate).ToList();

            foreach (var p in unassigned)
            {
                // Try to fit into ANY existing consumer first (Best Fit to minimize waste)
                var candidate = consumers
                    .Where(c => c.RemainingCapacity >= p.ProductionRate)
                    .OrderBy(c => c.RemainingCapacity)
                    .FirstOrDefault();

                if (candidate != null)
                {
                    candidate.AssignedPartitions.Add(p);
                    p.AssignedConsumer = candidate;
                }
                else
                {
                    // 5. SMART NEW BIN CREATION
                    // If we need a new consumer, look ahead at *all* remaining unassigned items 
                    // to guess the best size. We don't want to create a Small for a Large item.

                    double neededCapacity = p.ProductionRate;

                    // Simple lookahead: Grab the next few items too if they are waiting
                    double bufferLoad = unassigned.Where(u => u != p && u.AssignedConsumer == null).Sum(u => u.ProductionRate);

                    // Let's size for this item + safety.

                    var bestProfile = GetCheapestProfileForLoad(neededCapacity);

                    var newConsumer = ConsumerGroup.AddConsumer(bestProfile.Name);

                    // Update our local list reference
                    consumers = ConsumerGroup.Consumers;

                    newConsumer.AssignedPartitions.Add(p);
                    p.AssignedConsumer = newConsumer;
                }
            }
        }

        public Task AutoScale()
        {
            if (ConsumerGroup == null) return Task.CompletedTask;

            // 1. Try Standard Scale Down (Remove empty/inefficient nodes into existing slack)
            // This is the "Free" optimization (no new costs, just savings).
            bool scaledDown = TryStandardScaleDown();
            if (scaledDown) return Task.CompletedTask;

            // 2. Try Financial Merging 
            TryFinancialMerge();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Scans for subsets of consumers that can be merged into a cheaper profile.
        /// Considers the "Cost of Reassignment" vs "Savings".
        /// </summary>
        private void TryFinancialMerge()
        {
            var consumers = ConsumerGroup!.Consumers;
            if (consumers.Count < 2) return;

            // Sort by Inefficiency (Cost / Load) to target the worst offenders first
            // Avoid division by zero
            var sortedCandidates = consumers
                .OrderByDescending(c => c.GetCurrentWorkloadRate() <= 0 ? 0 : c.ConsumerProfile.CostPerSecond / c.GetCurrentWorkloadRate())
                .ToList();

            // We will look at pairs (and potentially triplets) of consumers.
            // O(N^2) complexity roughly, but N is usually small (<100).

            for (int i = 0; i < sortedCandidates.Count; i++)
            {
                for (int j = i + 1; j < sortedCandidates.Count; j++)
                {
                    var c1 = sortedCandidates[i];
                    var c2 = sortedCandidates[j];

                    // If they are already the most efficient type and full, skip
                    if (c1.GetCurrentWorkloadRate() > c1.MaxCapacity * 0.9 && c2.GetCurrentWorkloadRate() > c2.MaxCapacity * 0.9) continue;

                    // 1. What if we merged them?
                    double combinedLoad = c1.GetCurrentWorkloadRate() + c2.GetCurrentWorkloadRate();
                    double currentCost = c1.ConsumerProfile.CostPerSecond + c2.ConsumerProfile.CostPerSecond;

                    // 2. Find the singular profile that fits their combined load
                    var bestProfile = GetCheapestProfileForLoad(combinedLoad * 1.05); // +5% buffer

                    // 3. Calculate Financials
                    double newCost = bestProfile.CostPerSecond;
                    double savingsPerSec = currentCost - newCost;

                    if (savingsPerSec > 0)
                    {
                        // 4. Calculate "Reassignment Pain"
                        // How many partitions are moving? All of them.
                        int partitionsMoving = c1.AssignedPartitions.Count + c2.AssignedPartitions.Count;
                        double penalty = partitionsMoving * ReassignmentPenaltyCost;

                        // 5. The Decision: Is it worth it?
                        // We assume we want the savings to pay off the penalty within X seconds (e.g. 10 seconds)
                        double payoffTime = 10.0;

                        if ((savingsPerSec * payoffTime) > penalty)
                        {
                            Console.WriteLine($"[AUTOSCALE] MERGE DETECTED (Financial Sense):");
                            Console.WriteLine($"   {c1.Id} ({c1.ConsumerProfile.Name}) + {c2.Id} ({c2.ConsumerProfile.Name}) -> {bestProfile.Name}");
                            Console.WriteLine($"   Load: {combinedLoad:F1}. Old Cost: ${currentCost:F2} -> New Cost: ${newCost:F2}");
                            Console.WriteLine($"   Savings: ${savingsPerSec:F2}/s. Penalty: ${penalty:F2} (Moves: {partitionsMoving})");

                            // Execute Merge
                            var newConsumer = ConsumerGroup.AddConsumer(bestProfile.Name);
                            ConsumerGroup.RemoveConsumer(c1);
                            ConsumerGroup.RemoveConsumer(c2);
                            ConsumerGroup.Rebalance();

                            return; // One merge per tick for stability
                        }
                    }
                }
            }
        }

        private double _previousTotalLag = double.MaxValue;
        private bool TryStandardScaleDown()
        {
            if (ConsumerGroup == null) return false;

            var consumers = ConsumerGroup.Consumers;
            var partitions = ConsumerGroup.AllPartitions;

            double totalLag = partitions.Sum(p => p.CurrentLag);

            if (totalLag > _previousTotalLag * 1.05)
            {
                Console.WriteLine("[AUTOSCALE] Scaling down ABORTED: System lag is increasing.");
                _previousTotalLag = totalLag;
                return false;
            }

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
                double maxSlackLeft = consumers
                    .Where(c => c.Id != candidate.Id)
                    .Max(c => c.RemainingCapacity);

                // Simple check: Is there enough math space globally?
                // (Constraint: We ideally want to check if individual partitions fit, 
                // but global slack is a good enough proxy for a simulation heuristic).
                if (maxSlackLeft > loadToRelocate * 1.1) // 10% buffer
                {
                    Console.WriteLine($"[AUTOSCALE] Efficiency Check: Removing {candidate.Id} ({candidate.ConsumerProfile.Name}). Saving ${candidate.ConsumerProfile.CostPerSecond}/s");
                    ConsumerGroup.RemoveConsumer(candidate);
                    ConsumerGroup.Rebalance();
                    return true; // Only remove one per interval to be safe
                }
            }

            // --- SCALE UP IS HANDLED BY ASSIGNMENT ---
            // In MWF, if load exists that doesn't fit, 'Assign' creates consumers. 
            // However, we can do a proactive check here if we want to ensure headroom.

            return false;
        }

        /// <summary>
        /// Helper: Given a required load, find the Consumer Profile that fits it for the LEAST money.
        /// </summary>
        private ConsumerProfile GetCheapestProfileForLoad(double load)
        {
            var suitable = ConsumerGroup!.ConsumerProfiles
                .Where(p => p.MaxCapacity >= load)
                .OrderBy(p => p.CostPerSecond)
                .FirstOrDefault();

            // If nothing fits (load too huge), return the largest available (will likely overload, but best effort)
            return suitable ?? ConsumerGroup.ConsumerProfiles.OrderByDescending(p => p.MaxCapacity).First();
        }
    }

    public class BrokerSimulator
    {
        private const double TimeStepSeconds = 0.1;

        public static async Task Main()
        {
            Console.WriteLine("Starting Kafka Autoscaling Simulation (Config-Driven)...");

            IPartitionAssignmentStrategy assignmentStrategy = new CostCentricModifiedWorstFitAssignment();

            // Start metrics endpoint with strategy/run labels (from environment)
            var strategyEnv = assignmentStrategy.GetType().Name ?? System.Environment.GetEnvironmentVariable("STRATEGY");
            var runIdEnv = System.Environment.GetEnvironmentVariable("RUN_ID") ?? DateTimeOffset.UtcNow.Subtract(DateTimeOffset.UnixEpoch).TotalSeconds.ToString();//current unix epoch 
            MetricsExporter.Init(1234, strategyEnv, runIdEnv);

            // Use JSON config data provider to initialize partitions and handle rate/events
            var configPath = Path.Combine(AppContext.BaseDirectory, "simulation_config.json");
            var provider = new DataProviders.JSONConfigDataProvider(configPath);
            var partitions = provider.InitializePartitions();
            int maxRuntime = provider.MaxRuntimeSteps > 0 ? provider.MaxRuntimeSteps : 600;

            // Initialize consumer group
            var group = new ConsumerGroup("MyGroup", partitions, ConsumerProfiles.AllProfiles, ConsumerProfiles.Large, assignmentStrategy);

            // Start with 1 consumer
            group.AddConsumer();
            group.Rebalance();

            // Prepare CSV export for timestep series
            var outDir = Path.Combine(AppContext.BaseDirectory, "export_csv");
            Directory.CreateDirectory(outDir);
            var csvPath = Path.Combine(outDir, $"timeseries_{strategyEnv}_{runIdEnv}.csv");
            using var csvWriter = new StreamWriter(csvPath, false, Encoding.UTF8);
            // Header: step, timestamp, current_system_lag, messages_pending, current_production_rate, current_consumption_rate, total_system_load, current_system_cost, total_reassignments, total_rebalance_steps
            csvWriter.WriteLine("step,timestamp,current_system_lag,messages_pending,current_production_rate,current_consumption_rate,total_system_load,current_system_cost,total_reassignments,total_rebalance_steps,rScore_value,total_consumers");

            var rndRate = new Random();

            double lastLagTime = -1;

            for (int step = 1; step <= maxRuntime; step++)
             {
                 Console.WriteLine($"\n--- SIMULATION STEP {step} ---");
                // Let provider process rate changes / events for this timestep
                 provider.Process(group.AllPartitions, step);
                 // Production
                 group.AllPartitions.ForEach(p => p.Produce(TimeStepSeconds));

                 // Consumption
                 long stepConsumed = 0;
                 foreach (var c in group.Consumers)
                 {
                    stepConsumed += c.Consume(TimeStepSeconds);
                 }

                // Autoscale check every 30 seconds
                if (step % 30 == 0)
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

                var counter = new Dictionary<string, int>();
                foreach(var consumer in group.Consumers)
                {
                    var profileCode = consumer.ConsumerProfile.ShortCode;
                    if (!counter.ContainsKey(profileCode))
                        counter[profileCode] = 0;
                    counter[profileCode]++;
                }

                Console.WriteLine("Consumer Profiles:");
                foreach(var kvp in counter)
                {
                    Console.WriteLine($"  {kvp.Key}: {kvp.Value}");
                }


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

                    double lagTime = consumerLag / consumer.MaxCapacity;

                    //Console.WriteLine($"{consumer.Id}: Profile={consumer.ConsumerProfile.ShortCode} Messages={consumerLag:F0} Lag={lagTime:F2}s Msg Rate={consumer.GetCurrentWorkloadRate():F0} msgs/s. D={(consumer.GetCurrentWorkloadRate()-consumer.ConsumerProfile.MaxCapacity):F0} Util={utilization:F1}%. Partitions: {string.Join(", ", consumer.AssignedPartitions.Select(p => p.Id))}");
                }

                // CSV export per timestep
                double currentConsumptionRate = stepConsumed / TimeStepSeconds;

                // total system load: approximate as production rate + queued lag per SLA window
                double totalSystemLoad = totalProductionRate + (double)totalLag / group.LatencySLASeconds;

                double currentSystemCost = group.TotalCostPerSecond;

                csvWriter.WriteLine(string.Join(',', new string[] {
                    step.ToString(),
                    DateTime.UtcNow.ToString("o"),
                    totalLag.ToString(),
                    totalLag.ToString(),
                    totalProductionRate.ToString("F3"),
                    currentConsumptionRate.ToString("F3"),
                    totalSystemLoad.ToString("F3"),
                    currentSystemCost.ToString("F3"),
                    group.TotalReassignments.ToString(),
                    group.RebalanceSteps.ToString(),
                    group.RScoreValue.ToString("F5"),
                    group.Consumers.Count.ToString()
                }));

                csvWriter.Flush();


                if (lastLagTime != -1)
                {
                    double lagChange = maxLagTime - lastLagTime;
                    Console.WriteLine($"Lag Change This Step: {lagChange} messages ({(lagChange / TimeStepSeconds):F1} msgs/s)");

                    if (lagChange > 8)
                    {
                        Console.WriteLine($"[ALERT] System lag is INCREASING! (+{lagChange:F2} seconds this step)");

                        Console.ReadLine();
                    }
                }

                lastLagTime = maxLagTime;

                Thread.Sleep(200);
            }

            // close csv writer by disposing via using
            csvWriter.Close();
            csvWriter.Dispose();
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
