using MBrokerBench.Components;
using MBrokerBench.Models;
using Spectre.Console;

namespace MBrokerBench.Strategies
{
    public class CostCentricModifiedWorstFitAssignment : IPartitionAssignmentStrategy
    {
        public double RebalanceTimeSeconds { get; set; }
        public ConsumerGroup? ConsumerGroup { get; set; }

        private const double CapacityExcessFactor = 5 / (double)6; // Safety buffer
        public double SLA => ConsumerGroup?.LatencySLASeconds ?? 10;

        /// <summary>
        /// Helper: Finds the cheapest profile that fits the required load.
        /// </summary>
        private ConsumerProfile GetOptimalProfile(double requiredCapacity)
        {
            if (ConsumerGroup == null) return ConsumerProfiles.Medium;

            var candidates = ConsumerGroup.ConsumerProfiles
                .Where(p => p.MaxCapacity * CapacityExcessFactor >= requiredCapacity)
                .OrderBy(p => p.CostPerSecond)
                .ToList();

            if (candidates.Any()) return candidates.First();

            return ConsumerGroup.ConsumerProfiles.OrderByDescending(p => p.MaxCapacity).First();
        }

        private ConsumerProfile GetOptimalFitProfile(double requiredCapacity)
        {
            if (ConsumerGroup == null) return ConsumerProfiles.Medium;

            var candidates = ConsumerGroup.ConsumerProfiles
                .Where(p => p.MaxCapacity * CapacityExcessFactor <= requiredCapacity)
                .OrderByDescending(p => p.MaxCapacity)
                .ToList();

            if (candidates.Any()) return candidates.First();

            return ConsumerGroup.ConsumerProfiles.OrderBy(p => p.MaxCapacity).First();
        }

        private double GetTotalRequiredThroughput(Partition p)
        {
            // 1. Current accumulated lag
            double existingLag = p.CurrentLag;

            // 2. Lag we WILL accumulate during the rebalance pause
            // (Producers keep writing while consumers are stopped)
            double rebalanceLag = p.ProductionRate * RebalanceTimeSeconds;

            // 3. Total backlog to drain
            double totalBacklog = existingLag + rebalanceLag;

            // 4. Rate needed to drain backlog within SLA + Rate to match ongoing production
            // If SLA is 10s and Rebalance is 5s, we only have 5s left to drain!
            double effectiveWindow = Math.Max(1.0, SLA - RebalanceTimeSeconds);

            return p.ProductionRate + (totalBacklog / effectiveWindow);
        }

        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            // 'consumers' contains ALL consumers (Running + Booting)
            if (ConsumerGroup == null || consumers == null || !consumers.Any()) return;

            // 1. Clear current mappings
            foreach (var c in consumers) c.AssignedPartitions.Clear();

            var unassignedPartitions = new List<Partition>();

            // Sort consumers by total lag (process heavy consumers first)
            var sortedConsumers = consumers
                .OrderByDescending(c => c.GetCurrentTotalLag(RebalanceTimeSeconds))
                .ToList();

            // --- PHASE 1: PRESERVE EXISTING ASSIGNMENTS ---
            foreach (var currentConsumer in sortedConsumers)
            {
                var pset = partitions
                    .Where(p => p.AssignedConsumer?.Id == currentConsumer.Id)
                    .OrderByDescending(p => GetTotalRequiredThroughput(p))
                    .ToList();

                var ejectedPartitions = new List<Partition>();
                double currentLoad = 0;

                // Greedy packing: Keep as many as fit
                foreach (var p in pset)
                {
                    var partitionLoad = GetTotalRequiredThroughput(p);
                    // Note: Booting consumers have MaxCapacity > 0, so this math works.
                    // They just won't consume anything until they turn 'Running'.
                    if (currentLoad + partitionLoad <= currentConsumer.MaxCapacity * CapacityExcessFactor)
                    {
                        currentConsumer.AssignedPartitions.Add(p);
                        p.AssignedConsumer = currentConsumer;
                        currentLoad += p.ProductionRate;
                    }
                    else
                    {
                        ejectedPartitions.Add(p);
                    }
                }

                // --- HANDLING EJECTED PARTITIONS ---
                if (ejectedPartitions.Any())
                {
                    // In the previous version, we spawned a specific profile here.
                    // Now, we must look for an EXISTING empty consumer (likely created by AutoScale).

                    // We prefer 'Running' consumers, but 'Booting' is acceptable if it's all we have.
                    var emptyCandidates = consumers
                        .Where(c => c.AssignedPartitions.Count == 0)
                        .OrderByDescending(c => c.State == ConsumerState.Running) // Prefer active
                        .ThenByDescending(c => c.MaxCapacity) // Then largest (to fit the overflow)
                        .ToList();

                    if (emptyCandidates.Any())
                    {
                        // Try to fit ejected items into the first available empty consumer
                        var target = emptyCandidates.First();

                        double targetLoad = 0;

                        foreach (var ep in ejectedPartitions)
                        {
                            double epLoad = GetTotalRequiredThroughput(ep);

                            if (target.GetCurrentWorkloadRate() + ep.ProductionRate <= target.MaxCapacity * CapacityExcessFactor)
                            {
                                target.AssignedPartitions.Add(ep);
                                ep.AssignedConsumer = target;
                                targetLoad += epLoad;
                            }
                            else
                            {
                                unassignedPartitions.Add(ep);
                            }
                        }
                    }
                    else
                    {
                        // No empty consumers available? They go to Unassigned.
                        // AutoScale will see this and spawn a new one later.
                        unassignedPartitions.AddRange(ejectedPartitions);
                    }
                }
            }

            // --- PHASE 2: ASSIGN UNASSIGNED / ORPHANED ---
            var newlyUnassigned = partitions.Where(p => p.AssignedConsumer == null && !unassignedPartitions.Contains(p)).ToList();
            var finalU = unassignedPartitions.Union(newlyUnassigned)
                                             .OrderByDescending(GetTotalRequiredThroughput)
                                             .ToList();

            foreach (var partition in finalU)
            {
                // Find ANY consumer with space (Worst Fit strategy)
                // We prefer consumers that are ALREADY Running to reduce lag immediately.
                var candidate = consumers
                    .Where(c => c.RemainingCapacityWithEfficiency * CapacityExcessFactor >= partition.GetRequiredThroughput(SLA))
                    .OrderByDescending(c => c.State == ConsumerState.Running) // Priority 1: Running
                    .ThenByDescending(c => c.RemainingCapacityWithEfficiency) // Priority 2: Worst Fit (Leave gaps)
                    .FirstOrDefault();

                if(candidate == null)
                {
                    candidate = consumers
                        .OrderByDescending(c => c.State == ConsumerState.Running)
                        .ThenByDescending(c => c.MaxCapacity - c.GetCurrentWorkloadRate()) // Most raw space available
                        .FirstOrDefault();
                }

                if (candidate != null)
                {
                    candidate.AssignedPartitions.Add(partition);
                    partition.AssignedConsumer = candidate;
                }
                else
                {
                    // No space exists. 
                    // WE DO NOT CREATE A CONSUMER HERE.
                    // We leave it null. AutoScale will fix it.
                    partition.AssignedConsumer = null;
                }
            }

            // Logging for debugging
            int unassignedCount = finalU.Count(p => p.AssignedConsumer == null);
            if (unassignedCount > 0)
            {
                Logger.Log($"[Assignment] {unassignedCount} partitions could not be assigned (Waiting for AutoScale).");
            }
        }

        public Task AutoScale()
        {
            if (ConsumerGroup == null) return Task.CompletedTask;

            // 1. Try Standard Scale Down
            bool scaledDown = TryStandardScaleDown();
            if (scaledDown) return Task.CompletedTask;

            // 2. Try Financial Merging (Only merges Running consumers)
            TryFinancialMerge();

            TryClusterOptimization();

            // 3. PROACTIVE SCALE UP (The replacement for on-demand creation)
            // We calculate if there is unhandled load and provision the SPECIFIC profile needed.
            CheckAndProvisionCapacity();

            return Task.CompletedTask;
        }

        private void CheckAndProvisionCapacity()
        {
            var partitions = ConsumerGroup!.AllPartitions;
            var consumers = ConsumerGroup.Consumers; // Active + Booting

            if(consumers.Any(c=>c.State== ConsumerState.Booting))
            {
                return;
            }

            if (consumers.Count >= partitions.Count)
            {
                double totalDemandCheck = partitions.Sum(p => GetTotalRequiredThroughput(p));
                double maxPossibleSupply = consumers.Sum(c => c.MaxCapacity * CapacityExcessFactor);

                if (totalDemandCheck > maxPossibleSupply)
                {
                    Logger.Log($"[AUTOSCALE] BOTTLE-NECK: Partition Count Limit Reached ({partitions.Count}). Cannot scale out further.");
                }

                return;
            }


            // DO NOT DO ANYTHING????

            double statusQuoDemand = partitions.Sum(p => p.ProductionRate + (p.CurrentLag / SLA));
            double currentEffectiveCapacity = consumers
                .Where(c => c.State == ConsumerState.Running)
                .Sum(c => c.MaxCapacity * CapacityExcessFactor);

            if (currentEffectiveCapacity >= statusQuoDemand)
            {
                // Optional: Ensure we are actually draining if lag exists
                double totalLag = partitions.Sum(p => p.CurrentLag);
                if (totalLag > 0 && currentEffectiveCapacity <= partitions.Sum(p => p.ProductionRate))
                {

                }
                else
                {
                    return; // Status Quo is safe.
                }
            }

            // SUPPLY DEMAND LOGIC 

            // Calculate Supply vs Demand
            // this is the capacity projected
            double totalCapacity = consumers.Sum(c => c.MaxCapacity * CapacityExcessFactor);

            // demand
            double totalDemand = statusQuoDemand;//partitions.Sum(GetTotalRequiredThroughput);



            // Gap?
            if (totalDemand > totalCapacity)
            {
                double missingCapacity = totalDemand - totalCapacity;


                Logger.Log($"[AUTOSCALE] GAP {missingCapacity:F1} units.");

                while (missingCapacity > 0 && consumers.Count < partitions.Count)
                {
                    // Find best profile for the CURRENT chunk of missing capacity
                    var profile = GetOptimalProfile(missingCapacity);

                    var bestChoice = ConsumerGroup.ConsumerProfiles.Select(x =>
                        new
                        {
                            Profile = x,
                            EffectiveCap = x.MaxCapacity * CapacityExcessFactor,
                            UsefulCap = Math.Min(x.MaxCapacity * CapacityExcessFactor, missingCapacity)
                        }
                    ).Select(x =>
                    new
                    {
                        x.Profile,
                        x.EffectiveCap,
                        Efficiency = x.UsefulCap / x.EffectiveCap,
                    }).OrderByDescending(x => x.Efficiency)
                    .ThenBy(x => x.Profile.CostPerSecond)
                    .First();

                   // profile = bestChoice.Profile;

                    Logger.Log($"[AUTOSCALE] GAP {missingCapacity:F1} units. Provisioning {profile.Name} (Cost: ${profile.CostPerSecond}).");

                    Logger.Log($"   -> Spawning {profile.Name} (Cap: {profile.MaxCapacity}) to cover part of deficit.");

                    // Add the consumer
                    var newConsumer = ConsumerGroup.AddConsumer(profile.Name);

                    // Update the 'missing' amount so we know if we need ANOTHER one
                    double addedCapacity = profile.MaxCapacity * CapacityExcessFactor;
                    missingCapacity -= addedCapacity;

                    // Update local reference to ensure loop termination condition is correct
                    consumers = ConsumerGroup.Consumers;
                }
            }
        }

        private bool TryStandardScaleDown()
        {
            var consumers = ConsumerGroup!.Consumers;

            // Also, don't remove if we only have 1 consumer left.
            if (consumers.Count < 1) return false;

            double totalDemand = ConsumerGroup.AllPartitions.Sum(p => GetTotalRequiredThroughput(p));
            double currentCapacity = consumers.Sum(c => c.MaxCapacity * CapacityExcessFactor);

            if (totalDemand > currentCapacity * 0.9) return false; // If we are >90% loaded globally, don't risk it.

            var removed = false;

            var candidatesForRemoval = consumers
                .Where(c => c.State == ConsumerState.Running)
                .Select(c => new
                {
                    Consumer = c,
                    InefficiencyScore = c.GetCurrentWorkloadRate() <= 0 ? double.MaxValue : c.ConsumerProfile.CostPerSecond / c.GetCurrentWorkloadRate()
                })
                .OrderByDescending(x => x.InefficiencyScore)
                .ToList();

            foreach (var item in candidatesForRemoval)
            {
                var candidate = item.Consumer;
                double loadToRelocate = candidate.AssignedPartitions.Sum(GetTotalRequiredThroughput);

                // Check slack in OTHER consumers (Running)
                double maxSlackLeft = consumers
                    .Where(c => c.Id != candidate.Id 
                        && c.State == ConsumerState.Running
                     )
                    .Sum(c => c.RemainingCapacityWithEfficiency);

                if (maxSlackLeft * CapacityExcessFactor > loadToRelocate * 1.1)
                {
                    Logger.Log($"[AUTOSCALE] Removing Inefficient Consumer {candidate.Id} ({candidate.ConsumerProfile.Name}).");
                    ConsumerGroup.RemoveConsumer(candidate);
                    removed = true;
                }
            }

            if (removed)
            {
                ConsumerGroup.Rebalance();
            }

            return removed;
        }

        private void TryClusterOptimization()
        {
            var consumers = ConsumerGroup!.Consumers.Where(c => c.State == ConsumerState.Running).ToList();
            if (consumers.Count < 1) return; // Optimization usually needs a group

            // 1. Calculate the 'Ideal' Fleet for the current total load
            double totalSystemLoad = consumers.Sum(c => c.AssignedPartitions.Sum(GetTotalRequiredThroughput));
            double currentCost = consumers.Sum(c => c.ConsumerProfile.CostPerSecond);

            // Solve: What is the cheapest combination of profiles to hold 'totalSystemLoad'?
            // (This is a simplified Knapsack/Change-Making problem)
            var idealFleet = CalculateIdealFleet(totalSystemLoad * 1.05); // 5% buffer for safety

            double idealCost = idealFleet.Sum(p => p.CostPerSecond);

            // 2. Check if the switch is worth it
            // We need significant savings (e.g., 20%) to justify replacing the WHOLE fleet 
            // or a large chunk of it.
            double savings = currentCost - idealCost;

            // Penalty is high here: We are potentially replacing N nodes with M nodes.
            // Let's assume we replace the *entire* fleet (worst case penalty).
            double penalty = ConsumerGroup.AllPartitions.Count * 0.5;

            // If savings are huge and worth the penalty
            if (savings > 0 && savings * 20.0 > penalty) // Higher threshold (20s payoff) for cluster-wide changes
            {
                Logger.Log($"[AUTOSCALE] Cluster Optimization Detected!");
                Logger.Log($"   Current: {consumers.Count} nodes (${currentCost:F3}/s)");
                Logger.Log($"   Target:  {string.Join(", ", idealFleet.Select(p => p.Name))} (${idealCost:F3}/s)");

                // 3. EXECUTION (The tricky part)
                // We cannot just kill everyone. We need to transition.
                // Strategy: Spawn the Ideal Fleet *alongside* the current fleet.
                // Once they boot, the autoscaler will naturally kill the old inefficient ones.

                foreach (var profile in idealFleet)
                {
                    ConsumerGroup.AddConsumer(profile.Name);
                }

                // Return to prevent other actions this tick
                return;
            }
        }

        // Helper: Greedy approach to find cheapest combination
        private List<ConsumerProfile> CalculateIdealFleet(double load)
        {
            var fleet = new List<ConsumerProfile>();
            double remainingLoad = load;

            // Sort profiles by Efficiency (Cost per Unit of Capacity)
            var sortedProfiles = ConsumerGroup!.ConsumerProfiles
                .OrderBy(p => p.CostPerSecond / p.MaxCapacity)
                .ToList();

            while (remainingLoad > 0)
            {
                // Find the most efficient profile that isn't "Too Big" (wasteful)
                // OR just fill with the most efficient one until we are done.

                // Simple Greedy: Take the most efficient profile that fills a chunk of load
                var bestFit = GetOptimalProfile(remainingLoad);

                if (bestFit == null)
                {
                    // Load is huge, take the largest/most efficient one to bite a chunk off
                    bestFit = sortedProfiles.First(); // Cheapest per unit
                }

                fleet.Add(bestFit);
                remainingLoad -= (bestFit.MaxCapacity * CapacityExcessFactor);
            }

            return fleet;
        }

        private void TryFinancialMerge()
        {
            var consumers = ConsumerGroup!.Consumers;

            if (consumers.Count == 1)
            {
                var loneConsumer = consumers[0];

                if (loneConsumer.State != ConsumerState.Running) return;
                if (loneConsumer.Efficiency < 0.9) return;

                double currentLoad = loneConsumer.AssignedPartitions.Sum(GetTotalRequiredThroughput);

                var bestProfile = GetOptimalProfile(currentLoad);

                if (bestProfile.Name != loneConsumer.ConsumerProfile.Name &&
                    bestProfile.CostPerSecond < loneConsumer.ConsumerProfile.CostPerSecond)
                {
                    if (bestProfile.MaxCapacity * CapacityExcessFactor >= currentLoad)
                    {
                        double savings = loneConsumer.ConsumerProfile.CostPerSecond - bestProfile.CostPerSecond;

                        double penalty = loneConsumer.AssignedPartitions.Count * 0.5;

                        if (savings * 10.0 > penalty)
                        {
                            Logger.Log($"[AUTOSCALE] Vertical Downscale: {loneConsumer.Id} ({loneConsumer.ConsumerProfile.Name}) -> {bestProfile.Name}");
                            Logger.Log($"   Load: {currentLoad:F1} fits in {bestProfile.Name}");

                            ConsumerGroup.AddConsumer(bestProfile.Name);
                            return;
                        }
                    }
                }

                return;
            }

            // Only consider RUNNING consumers for merges
            var sortedCandidates = consumers
                .Where(c => c.State == ConsumerState.Running)
                .OrderByDescending(c => c.GetCurrentWorkloadRate() <= 0 ? 0 : c.ConsumerProfile.CostPerSecond / c.GetCurrentWorkloadRate())
                .ToList();

            for (int i = 0; i < sortedCandidates.Count; i++)
            {
                for (int j = i + 1; j < sortedCandidates.Count; j++)
                {
                    var c1 = sortedCandidates[i];
                    var c2 = sortedCandidates[j];

                    // Skip if both full
                    double load1 = c1.AssignedPartitions.Sum(GetTotalRequiredThroughput);
                    double load2 = c2.AssignedPartitions.Sum(GetTotalRequiredThroughput);

                    if (load1 > c1.CurrentEffectiveCapacity * 0.9
                        && load2 > c2.CurrentEffectiveCapacity * 0.9)
                        continue;

                    double combinedLoad = load1 + load2;


                    if (combinedLoad <= c1.MaxCapacity * CapacityExcessFactor)
                    {
                        continue;
                    }

                    if (combinedLoad <= c2.MaxCapacity * CapacityExcessFactor)
                    {
                        continue;
                    }

                    double currentCost = c1.ConsumerProfile.CostPerSecond + c2.ConsumerProfile.CostPerSecond;

                    // Find profile for combined load
                    var bestProfile = GetOptimalProfile(combinedLoad);

                    if (bestProfile.MaxCapacity * CapacityExcessFactor < combinedLoad)
                    {
                        continue;
                    }

                    double newCost = bestProfile.CostPerSecond;
                    double savingsPerSec = currentCost - newCost;

                    // Payoff Logic
                    // We assume penalty = 0.5 * partitions moved
                    int partitionsMoving = c1.AssignedPartitions.Count + c2.AssignedPartitions.Count;
                    double penalty = partitionsMoving * 0.50;

                    if (savingsPerSec > 0 && (savingsPerSec * 10.0 > penalty))
                    {
                        Logger.Log($"[AUTOSCALE] Financial Merge: {c1.Id} + {c2.Id} -> {bestProfile.Name}");

                        // Provision the new efficient consumer
                        var newConsumer = ConsumerGroup.AddConsumer(bestProfile.Name);

                        return;
                    }
                }
            }
        }
    }
}