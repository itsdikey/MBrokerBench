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

        // Penalty per node to favor consolidation (avoid fragmentation).
        // Costs: Small=0.5, Medium=1.0, Large=2.5.
        // Penalty 0.01 means:
        // 2 Small ($1.02) vs 1 Medium ($1.01) -> Medium wins slightly.
        // 5 Medium ($5.05) vs 2 Large ($5.02) -> Large wins slightly.
        private const double NodeCountPenalty = 0.01;

        /// <summary>
        /// Finds the optimal combination of profiles to satisfy the required capacity at minimum cost.
        /// </summary>
        private List<ConsumerProfile> GetOptimalFleetCombination(double requiredLoad, double minProfileCapacity = 0)
        {
            if (ConsumerGroup == null || !ConsumerGroup.ConsumerProfiles.Any() || requiredLoad <= 0) 
                return new List<ConsumerProfile>();

            var profiles = ConsumerGroup.ConsumerProfiles.OrderBy(p => p.CostPerSecond).ToList();
            var resultFleet = new List<ConsumerProfile>();

            // Normalize target to raw capacity needed
            double targetRaw = requiredLoad / CapacityExcessFactor;

            // DP for min cost to reach at least targetRaw
            // We use a reasonably bounded target to keep the DP table small.
            int maxCapToSearch = (int)Math.Ceiling(targetRaw + profiles.Max(p => p.MaxCapacity));
            
            // Adjust step size if capacities are very large, but here they are 500-2500, so 1-unit steps are fine.
            double[] minCostDP = new double[maxCapToSearch + 1];
            List<ConsumerProfile>[] bestComboDP = new List<ConsumerProfile>[maxCapToSearch + 1];
            
            for (int i = 0; i <= maxCapToSearch; i++) minCostDP[i] = double.MaxValue;
            minCostDP[0] = 0;
            bestComboDP[0] = new List<ConsumerProfile>();

            foreach (var p in profiles)
            {
                int pCap = (int)p.MaxCapacity;
                double effectiveCost = p.CostPerSecond + NodeCountPenalty; // Apply penalty here

                for (int c = 0; c <= maxCapToSearch - pCap; c++)
                {
                    if (minCostDP[c] != double.MaxValue)
                    {
                        double newCost = minCostDP[c] + effectiveCost;
                        int newCap = c + pCap;
                        if (newCost < minCostDP[newCap])
                        {
                            minCostDP[newCap] = newCost;
                            bestComboDP[newCap] = new List<ConsumerProfile>(bestComboDP[c]) { p };
                        }
                    }
                }
            }

            // Find the cheapest entry that meets or exceeds targetRaw
            double absoluteMinCost = double.MaxValue;
            int bestIdx = -1;
            for (int i = (int)Math.Ceiling(targetRaw); i <= maxCapToSearch; i++)
            {
                if (minCostDP[i] < absoluteMinCost)
                {
                    absoluteMinCost = minCostDP[i];
                    bestIdx = i;
                }
            }
            
            if (bestIdx != -1)
            {
                resultFleet = bestComboDP[bestIdx];
            }

            return resultFleet;
        }

        private double GetTotalRequiredThroughput(Partition p)
        {
            double existingLag = p.CurrentLag;
            double rebalanceLag = p.ProductionRate * RebalanceTimeSeconds;
            double totalBacklog = existingLag + rebalanceLag;
            double effectiveWindow = Math.Max(1.0, SLA - RebalanceTimeSeconds);

            return p.ProductionRate + (totalBacklog / effectiveWindow);
        }

        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            if (ConsumerGroup == null || consumers == null || !consumers.Any()) return;

            foreach (var c in consumers) c.AssignedPartitions.Clear();

            var unassignedPartitions = new List<Partition>();
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

                double currentLoad = 0;
                foreach (var p in pset)
                {
                    var partitionLoad = GetTotalRequiredThroughput(p);
                    if (currentLoad + partitionLoad <= currentConsumer.MaxCapacity * CapacityExcessFactor)
                    {
                        currentConsumer.AssignedPartitions.Add(p);
                        p.AssignedConsumer = currentConsumer;
                        currentLoad += p.ProductionRate;
                    }
                    else
                    {
                        unassignedPartitions.Add(p);
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
                double req = partition.GetRequiredThroughput(SLA);
                
                // Priority 1: Running, fits with safety buffer
                var candidate = consumers
                    .Where(c => c.State == ConsumerState.Running && c.RemainingCapacityWithEfficiency * CapacityExcessFactor >= req)
                    .OrderByDescending(c => c.RemainingCapacityWithEfficiency)
                    .FirstOrDefault();

                // Priority 2: Booting, fits with safety buffer
                if (candidate == null)
                {
                    candidate = consumers
                        .Where(c => c.State == ConsumerState.Booting && c.RemainingCapacity * CapacityExcessFactor >= req)
                        .OrderByDescending(c => c.RemainingCapacity)
                        .FirstOrDefault();
                }

                // Priority 3: Any that fits raw (no safety buffer)
                if(candidate == null)
                {
                    candidate = consumers
                        .Where(c => c.MaxCapacity >= req)
                        .OrderByDescending(c => c.State == ConsumerState.Running)
                        .ThenByDescending(c => c.RemainingCapacity)
                        .FirstOrDefault();
                }
                
                // Fallback: Pick the consumer with the most space, even if it's too small (Best effort)
                if (candidate == null)
                {
                    candidate = consumers
                        .OrderByDescending(c => c.State == ConsumerState.Running)
                        .ThenByDescending(c => c.RemainingCapacity)
                        .FirstOrDefault();
                }

                if (candidate != null)
                {
                    candidate.AssignedPartitions.Add(partition);
                    partition.AssignedConsumer = candidate;
                }
                else
                {
                    partition.AssignedConsumer = null;
                }
            }

            int unassignedCount = finalU.Count(p => p.AssignedConsumer == null);
            if (unassignedCount > 0)
            {
                Logger.Log($"[Assignment] {unassignedCount} partitions could not be assigned.");
            }
        }

        public Task AutoScale()
        {
            if (ConsumerGroup == null) return Task.CompletedTask;

            if (TryStandardScaleDown()) return Task.CompletedTask;

            TryClusterOptimization();
            CheckAndProvisionCapacity();

            return Task.CompletedTask;
        }

        private void CheckAndProvisionCapacity()
        {
            var partitions = ConsumerGroup!.AllPartitions;
            var consumers = ConsumerGroup.Consumers;

            // 1. Check Unassigned Partitions (Hard Deficit)
            var unassigned = partitions.Where(p => p.AssignedConsumer == null).ToList();
            
            if (unassigned.Any())
            {
                double unassignedLoad = unassigned.Sum(p => GetTotalRequiredThroughput(p));
                double maxUnassignedPartition = unassigned.Max(p => GetTotalRequiredThroughput(p));

                Logger.Log($"[AUTOSCALE] Found {unassigned.Count} unassigned partitions. Load: {unassignedLoad:F1}. MaxSingle: {maxUnassignedPartition:F1}");

                var newFleet = GetOptimalFleetCombination(unassignedLoad);

                if (newFleet.Any())
                {
                    double maxCapInFleet = newFleet.Max(p => p.MaxCapacity * CapacityExcessFactor);
                    
                    if (maxCapInFleet < maxUnassignedPartition)
                    {
                        var validProfile = ConsumerGroup.ConsumerProfiles
                            .Where(p => p.MaxCapacity * CapacityExcessFactor >= maxUnassignedPartition)
                            .OrderBy(p => p.CostPerSecond)
                            .FirstOrDefault();

                        if (validProfile != null) newFleet.Add(validProfile);
                    }

                    foreach (var profile in newFleet)
                    {
                        if (ConsumerGroup.Consumers.Count >= partitions.Count)
                        {
                            // VERTICAL UPGRADE: If we can't add, replace the smallest existing consumer
                            var smallest = consumers.OrderBy(c => c.MaxCapacity).FirstOrDefault();
                            if (smallest != null && smallest.MaxCapacity < newFleet.Max(p => p.MaxCapacity))
                            {
                                Logger.Log($"[AUTOSCALE] Vertical Upgrade: Removing {smallest.ConsumerProfile.Name} to make room for {profile.Name}.");
                                ConsumerGroup.RemoveConsumer(smallest);
                            }
                            else break;
                        }
                        Logger.Log($"   -> Spawning {profile.Name} (Cap: {profile.MaxCapacity}) for unassigned load.");
                        ConsumerGroup.AddConsumer(profile.Name);
                    }
                }
                return;
            }

            // 2. Global Capacity Check (Proactive / Fluid)
            double totalDemand = partitions.Sum(p => GetTotalRequiredThroughput(p));
            double currentCapacity = consumers.Sum(c => c.MaxCapacity * CapacityExcessFactor);

            if (totalDemand > currentCapacity)
            {
                double missingCapacity = totalDemand - currentCapacity;
                var targetFleet = GetOptimalFleetCombination(totalDemand);
                
                // If we are at the partition limit, we must UPGRADE existing consumers
                if (consumers.Count >= partitions.Count)
                {
                    // Find if target fleet has better profiles than our current smallest
                    var smallestActive = consumers.OrderBy(c => c.MaxCapacity).First();
                    var bestInTarget = targetFleet.OrderByDescending(p => p.MaxCapacity).First();

                    if (bestInTarget.MaxCapacity > smallestActive.MaxCapacity)
                    {
                        Logger.Log($"[AUTOSCALE] Bottleneck detected at {consumers.Count} consumers. Upgrading {smallestActive.Id} ({smallestActive.ConsumerProfile.Name} -> {bestInTarget.Name}).");
                        ConsumerGroup.RemoveConsumer(smallestActive);
                        ConsumerGroup.AddConsumer(bestInTarget.Name);
                        return;
                    }
                }
                else
                {
                    var newFleet = GetOptimalFleetCombination(missingCapacity);
                    foreach (var profile in newFleet)
                    {
                        if (ConsumerGroup.Consumers.Count >= partitions.Count) break;
                        Logger.Log($"[AUTOSCALE] Global Deficit {missingCapacity:F1}. Provisioning {profile.Name}.");
                        ConsumerGroup.AddConsumer(profile.Name);
                    }
                }
            }
        }

        private bool TryStandardScaleDown()
        {
            var consumers = ConsumerGroup!.Consumers;
            if (consumers.Count <= 1) return false;

            double totalDemand = ConsumerGroup.AllPartitions.Sum(p => GetTotalRequiredThroughput(p));
            double currentCapacity = consumers.Sum(c => c.MaxCapacity * CapacityExcessFactor);

            // Don't scale down if load is high
            if (totalDemand > currentCapacity * 0.85) return false; 

            // Sort by Inefficiency: We want to remove nodes that cost a lot for the little work they do.
            var candidatesForRemoval = consumers
                .Where(c => c.State == ConsumerState.Running && c.Efficiency > 0.8) // GRACE PERIOD: Don't kill fresh consumers
                .Select(c => new
                {
                    Consumer = c,
                    // Avoid div by zero. High score = Bad (Expensive for little work)
                    InefficiencyScore = c.GetCurrentWorkloadRate() <= 1 ? double.MaxValue : c.ConsumerProfile.CostPerSecond / c.GetCurrentWorkloadRate()
                })
                .OrderByDescending(x => x.InefficiencyScore)
                .Select(x => x.Consumer)
                .ToList();

            bool anyRemoved = false;
            
            // Calculate total available slack ONCE at the start. 
            // We use WithEfficiency to be safe.
            double globalSlack = consumers
                .Where(c => c.State == ConsumerState.Running)
                .Sum(c => c.RemainingCapacityWithEfficiency);
            
            // We must preserve enough slack for the load we are removing.
            double consumedSlack = 0;

            foreach (var item in candidatesForRemoval)
            {
                if (!consumers.Contains(item)) continue;
                
                double loadToRelocate = item.AssignedPartitions.Sum(GetTotalRequiredThroughput);
                
                // Slack available specifically in OTHER consumers (Global - MyOwnSlack)
                double mySlack = item.RemainingCapacityWithEfficiency;
                double othersSlack = globalSlack - mySlack - consumedSlack;

                if (othersSlack * CapacityExcessFactor > loadToRelocate * 1.1)
                {
                    Logger.Log($"[AUTOSCALE] Scaling down: Removing inefficient consumer {item.Id} ({item.ConsumerProfile.Name}). Score: {item.ConsumerProfile.CostPerSecond/Math.Max(1, item.GetCurrentWorkloadRate()):F4}");
                    ConsumerGroup.RemoveConsumer(item);
                    anyRemoved = true;
                    
                    // The load we just removed needs to fit into the slack of others.
                    // So we have "used up" that much slack from the pool.
                    consumedSlack += loadToRelocate;
                    
                    // Also, since 'item' is gone, its contribution to globalSlack is gone.
                    // But we handled that by subtracting 'mySlack' above. 
                    // To keep the loop consistent for the NEXT item, we need to permanently reduce globalSlack?
                    // Yes, globalSlack is strictly smaller now.
                    globalSlack -= mySlack; 
                }
            }
            
            if (anyRemoved)
            {
                ConsumerGroup.Rebalance();
            }
            
            return anyRemoved;
        }

        private void TryClusterOptimization()
        {
            var consumers = ConsumerGroup!.Consumers.Where(c => c.State == ConsumerState.Running).ToList();
            if (consumers.Count < 1) return; 

            double totalSystemLoad = consumers.Sum(c => c.AssignedPartitions.Sum(GetTotalRequiredThroughput));
            double currentCost = consumers.Sum(c => c.ConsumerProfile.CostPerSecond);

            var idealFleet = GetOptimalFleetCombination(totalSystemLoad * 1.05); 
            double idealCost = idealFleet.Sum(p => p.CostPerSecond);

            if (idealCost >= currentCost * 0.95) return; // Only optimize if >5% savings

            double savingsPerSecond = currentCost - idealCost;
            double transitionCost = idealFleet.Sum(p => p.StartupTime * p.CostPerSecond);
            double paybackSeconds = transitionCost / savingsPerSecond;

            if (paybackSeconds < 120) 
            {
                Logger.Log($"[AUTOSCALE] Cluster Optimization: Target Cost ${idealCost:F2}/s (Current ${currentCost:F2}/s). Payback {paybackSeconds:F1}s.");
                foreach (var profile in idealFleet)
                {
                     if (ConsumerGroup.Consumers.Count >= ConsumerGroup.AllPartitions.Count * 2) break;
                     ConsumerGroup.AddConsumer(profile.Name);
                }
            }
        }
    }
}    