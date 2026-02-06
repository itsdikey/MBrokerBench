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

            // SAFETY: Cap the search space to prevent OOM on massive lag spikes.
            // 50,000 is enough for ~20 large nodes (2500 each).
            int maxCapToSearch = (int)Math.Min(50000, Math.Ceiling(targetRaw + profiles.Max(p => p.MaxCapacity)));
            
            // Adjust target if it exceeded our safety cap
            targetRaw = Math.Min(targetRaw, maxCapToSearch);

            // DP for min cost to reach at least targetRaw
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
            // Consistency: Use the same calculation as ModifiedWorstFit for demand estimation
            return p.GetRequiredThroughput(SLA);
        }

        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            if (ConsumerGroup == null || consumers == null || !consumers.Any()) return;

            foreach (var c in consumers) c.AssignedPartitions.Clear();

            var unassignedPartitions = new List<Partition>();
            var sortedConsumers = consumers
                .OrderByDescending(c => c.GetCurrentTotalLag(RebalanceTimeSeconds))
                .ToList();

            // --- PHASE 1: PRESERVE EXISTING ASSIGNMENTS (STABILITY) ---
            foreach (var currentConsumer in sortedConsumers)
            {
                var pset = partitions
                    .Where(p => p.AssignedConsumer?.Id == currentConsumer.Id)
                    .OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds))
                    .ToList();

                foreach (var p in pset)
                {
                    // STABILITY: We use ProductionRate here, NOT catch-up rate.
                    // If the production rate fits, we keep the partition and let the consumer 
                    // use its headroom/efficiency to clear the lag over time.
                    if (currentConsumer.GetCurrentWorkloadRate() + p.ProductionRate <= currentConsumer.MaxCapacity)
                    {
                        currentConsumer.AssignedPartitions.Add(p);
                        p.AssignedConsumer = currentConsumer;
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
                                             .OrderByDescending(p => p.ProductionRate)
                                             .ToList();

            foreach (var partition in finalU)
            {
                // Priority 1: Running, fits production rate
                var candidate = consumers
                    .Where(c => c.State == ConsumerState.Running && c.GetCurrentWorkloadRate() + partition.ProductionRate <= c.MaxCapacity)
                    .OrderByDescending(c => c.RemainingCapacity) // Worst Fit
                    .FirstOrDefault();

                // Priority 2: Booting, fits production rate
                if (candidate == null)
                {
                    candidate = consumers
                        .Where(c => c.State == ConsumerState.Booting && c.GetCurrentWorkloadRate() + partition.ProductionRate <= c.MaxCapacity)
                        .OrderByDescending(c => c.RemainingCapacity)
                        .FirstOrDefault();
                }
                
                // Fallback: Best effort (overload)
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

            var consumers = ConsumerGroup.Consumers;
            var partitions = ConsumerGroup.AllPartitions;
            double totalLag = partitions.Sum(p => p.CurrentLag);
            double totalProduction = partitions.Sum(p => p.ProductionRate);
            bool anyoneBooting = consumers.Any(c => c.State != ConsumerState.Running);

            // 1. CALCULATE TARGET DEMAND
            // We scale for Production Rate (Steady State) + a dampened Lag component.
            // This prevents "Rebalance Storms" where lag from one rebalance triggers another.
            double lagComponent = totalLag > (totalProduction * 2) ? (totalLag / SLA) : 0;
            double targetDemand = totalProduction + lagComponent;
            
            // 2. FLEET PLANNING
            CheckAndProvisionCapacity(targetDemand, allowRemovals: !anyoneBooting);

            // 3. STABILITY GATED: Only optimize fleet types if healthy and stable.
            if (!anyoneBooting && totalLag < 1000) 
            {
                TryStandardScaleDown();
                TryClusterOptimization();
            }
            
            return Task.CompletedTask;
        }

        private void CheckAndProvisionCapacity(double targetDemand, bool allowRemovals)
        {
            var partitions = ConsumerGroup!.AllPartitions;
            var consumers = ConsumerGroup.Consumers;

            // 1. Calculate Ideal Fleet using DP
            var targetFleet = GetOptimalFleetCombination(targetDemand);
            if (!targetFleet.Any() && partitions.Any(p => p.ProductionRate > 0))
                targetFleet.Add(ConsumerGroup.DefaultProfile);

            var currentCounts = consumers.GroupBy(c => c.ConsumerProfile.Name).ToDictionary(g => g.Key, g => g.Count());
            var targetCounts = targetFleet.GroupBy(p => p.Name).ToDictionary(g => g.Key, g => g.Count());

            // 2. SCALE DOWN / SURPLUS REMOVAL (Gated by Stability)
            if (allowRemovals)
            {
                bool anyRemoved = false;
                foreach (var profileName in currentCounts.Keys.ToList())
                {
                    int targetCount = targetCounts.GetValueOrDefault(profileName, 0);
                    while (consumers.Count(c => c.ConsumerProfile.Name == profileName) > targetCount)
                    {
                        var toRemove = consumers
                            .Where(c => c.ConsumerProfile.Name == profileName && c.State == ConsumerState.Running)
                            .OrderBy(c => c.GetCurrentWorkloadRate())
                            .FirstOrDefault();
                        
                        if (toRemove == null) break;

                        double runningCapAfterRemoval = consumers
                            .Where(c => c != toRemove && c.State == ConsumerState.Running)
                            .Sum(c => c.CurrentEffectiveCapacity * CapacityExcessFactor);

                        if (runningCapAfterRemoval >= partitions.Sum(p => p.ProductionRate))
                        {
                            Logger.Log($"[AUTOSCALE] Target Fleet Enforcement: Removing surplus {profileName} {toRemove.Id}.");
                            ConsumerGroup.RemoveConsumer(toRemove);
                            anyRemoved = true;
                        }
                        else break; 
                    }
                }
                if (anyRemoved) ConsumerGroup.Rebalance();
            }

            // 3. SCALE UP / DEFICIT (Dampened)
            // We only add up to 2 consumers per tick to prevent "Explosive Scaling".
            int additionsThisTick = 0;
            foreach (var profileName in targetCounts.Keys)
            {
                int targetCount = targetCounts[profileName];
                int currentCount = consumers.Count(c => c.ConsumerProfile.Name == profileName);

                while (currentCount < targetCount && consumers.Count < partitions.Count && additionsThisTick < 2)
                {
                    Logger.Log($"[AUTOSCALE] Capacity Deficit: Provisioning {profileName}.");
                    ConsumerGroup.AddConsumer(profileName);
                    currentCount++;
                    additionsThisTick++;
                }
            }

            // 4. EMERGENCY CHECK: Unassigned partitions
            if (additionsThisTick < 2 && partitions.Any(p => p.AssignedConsumer == null))
            {
                Logger.Log($"[AUTOSCALE] Emergency: Provisioning {ConsumerGroup.DefaultProfile.Name} for unassigned partitions.");
                ConsumerGroup.AddConsumer();
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
            var consumers = ConsumerGroup!.Consumers;
            // Stability: Don't optimize if any node is booting or if the fleet is already changing
            if (consumers.Any(c => c.State != ConsumerState.Running)) return;

            double totalSystemLoad = consumers.Sum(c => c.GetCurrentWorkloadRate());
            double currentCost = consumers.Sum(c => c.ConsumerProfile.CostPerSecond);

            var idealFleet = GetOptimalFleetCombination(totalSystemLoad * 1.1); 
            double idealCost = idealFleet.Sum(p => p.CostPerSecond);

            // STICKINESS: Only swap if savings are > 20% to justify the rebalance pain
            if (idealCost >= currentCost * 0.80) return; 

            double savingsPerSecond = currentCost - idealCost;
            double transitionCost = idealFleet.Sum(p => p.StartupTime * p.CostPerSecond);
            double paybackSeconds = transitionCost / savingsPerSecond;

            if (paybackSeconds < 300) // 5 minute payback
            {
                Logger.Log($"[AUTOSCALE] Cluster Optimization: Switching to cheaper fleet (Cost ${idealCost:F2}/s vs ${currentCost:F2}/s).");
                foreach (var profile in idealFleet)
                {
                     if (ConsumerGroup.Consumers.Count >= ConsumerGroup.AllPartitions.Count) break;
                     ConsumerGroup.AddConsumer(profile.Name);
                }
            }
        }
    }
}    