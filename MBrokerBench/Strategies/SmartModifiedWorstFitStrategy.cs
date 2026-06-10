using MBrokerBench.Components;
using MBrokerBench.Models;

namespace MBrokerBench.Strategies
{
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

                        if (savingsPerSec * payoffTime > penalty)
                        {
                            Logger.Log("[AUTOSCALE] MERGE DETECTED (Financial Sense):");
                            Logger.Log($"   {c1.Id} ({c1.ConsumerProfile.Name}) + {c2.Id} ({c2.ConsumerProfile.Name}) -> {bestProfile.Name}");
                            Logger.Log($"   Load: {combinedLoad:F1}. Old Cost: ${currentCost:F2} -> New Cost: ${newCost:F2}");
                            Logger.Log($"   Savings: ${savingsPerSec:F2}/s. Penalty: ${penalty:F2} (Moves: {partitionsMoving})");

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
                Logger.Log("[AUTOSCALE] Scaling down ABORTED: System lag is increasing.");
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
                    Logger.Log($"[AUTOSCALE] Efficiency Check: Removing {candidate.Id} ({candidate.ConsumerProfile.Name}). Saving ${candidate.ConsumerProfile.CostPerSecond}/s");
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
}
