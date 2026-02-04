using MBrokerBench.Components;

namespace MBrokerBench.Strategies;

public class ModifiedWorstFitAssignment : IPartitionAssignmentStrategy
{
    public double RebalanceTimeSeconds { get; set; }
    public ConsumerGroup? ConsumerGroup { get; set; }

    private const double ScaleDownUtilizationThreshold = 0.20;
    private const double CapacityExcessFactor = 5.0 / 6.0;

    public void Assign(List<Partition> partitions, List<Consumer> consumers)
    {
        if (ConsumerGroup == null || consumers == null || !consumers.Any()) return;

        // 1. Clear Assignments
        foreach (var c in consumers) c.AssignedPartitions.Clear();

        var unassignedPartitions = new List<Partition>();

        // Sort consumers to process heavy loads first (preservation heuristic)
        var sortedConsumers = consumers
            .OrderBy(c => c.GetCurrentTotalLag(RebalanceTimeSeconds))
            .ToList();

        // --- PHASE 1: PRESERVE EXISTING ASSIGNMENTS ---
        foreach (var currentConsumer in sortedConsumers)
        {
            // Get previous ownership
            var pset = new HashSet<Partition>(partitions
                .Where(p => p.AssignedConsumer?.Id == currentConsumer.Id));

            // Sort by size
            var sortedPSet = pset.OrderBy(p => p.GetTotalLag(RebalanceTimeSeconds)).ToList();

            // Try to keep as many as possible
            for (int i = sortedPSet.Count - 1; i >= 0; i--)
            {
                var p = sortedPSet[i];
                // Check capacity (Note: Booting consumers have MaxCapacity > 0, so this passes)
                if (currentConsumer.GetCurrentWorkloadRate() + p.ProductionRate <= currentConsumer.MaxCapacity)
                {
                    currentConsumer.AssignedPartitions.Add(p);
                    p.AssignedConsumer = currentConsumer;
                }
                else
                {
                    // No room, stop packing
                    break;
                }
                pset.Remove(p); // Remove from 'to-be-reassigned' set
            }

            if (pset.Count == 0) continue;

            // --- REASSIGN OVERFLOW (The items that didn't fit) ---
            var remaining = sortedPSet.Where(x => pset.Contains(x)).ToList();

            // MODIFICATION: Instead of ConsumerGroup.AddConsumer(), find an Empty consumer in the passed list.
            // This grabs consumers created by AutoScale that are waiting for work.
            var overflowConsumer = consumers
                .FirstOrDefault(c => c.AssignedPartitions.Count == 0);

            if (overflowConsumer != null)
            {
                foreach (var p in remaining)
                {
                    if (overflowConsumer.GetCurrentWorkloadRate() + p.ProductionRate <= overflowConsumer.MaxCapacity)
                    {
                        overflowConsumer.AssignedPartitions.Add(p);
                        p.AssignedConsumer = overflowConsumer;
                    }
                    else
                    {
                        unassignedPartitions.Add(p);
                    }
                }
            }
            else
            {
                // No empty consumers available? Dump to unassigned.
                unassignedPartitions.AddRange(remaining);
            }
        }

        // --- PHASE 2: HANDLE UNASSIGNED ---
        var newlyUnassigned = partitions.Where(p => p.AssignedConsumer == null && !unassignedPartitions.Contains(p)).ToList();
        var finalU = unassignedPartitions.Union(newlyUnassigned)
                                         .OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds))
                                         .ToList();

        foreach (var partition in finalU)
        {
            // 1. Try to fit into ANY consumer with space (Running OR Booting)
            // We prioritize Running consumers to reduce Lag immediately.
            var bestFitConsumer = consumers
                .Where(c => c.GetCurrentWorkloadRate() + partition.ProductionRate <= c.MaxCapacity)
                .OrderByDescending(c => c.State == ConsumerState.Running) // Running first
                .ThenByDescending(c => c.RemainingCapacity)               // Then Worst Fit (Leaves gaps)
                .FirstOrDefault();

            if (bestFitConsumer != null)
            {
                bestFitConsumer.AssignedPartitions.Add(partition);
                partition.AssignedConsumer = bestFitConsumer;
            }
            else
            {
                // No space anywhere. 
                // We DO NOT create a consumer here. AutoScale handles that.
                partition.AssignedConsumer = null;
            }
        }

        Logger.Log($"[Assignment] Complete. Unassigned: {finalU.Count(p => p.AssignedConsumer == null)}");
    }

    public Task AutoScale()
    {
        if (ConsumerGroup == null) return Task.CompletedTask;

        var allConsumers = ConsumerGroup.Consumers; // Includes Booting
        var partitions = ConsumerGroup.AllPartitions;
        
        // Calculate global demand
        double totalRequiredThrougput = partitions.Sum(p => p.GetRequiredThroughput(ConsumerGroup.LatencySLASeconds));

        // --- SCALE DOWN CHECK ---
        // Only consider removing RUNNING consumers.
        var removable = allConsumers
            .Where(c => c.State == ConsumerState.Running)
            .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * ScaleDownUtilizationThreshold)
            .OrderBy(c => c.GetCurrentWorkloadRate())
            .FirstOrDefault();

        if (removable != null && allConsumers.Count > 1) 
        {
            // SAFETY CHECK: Can the remaining fleet handle the TOTAL load?
            // We use MaxCapacity * CapacityExcessFactor for the remaining fleet.
            double capacityAfterRemoval = allConsumers
                .Where(c => c != removable)
                .Sum(c => c.MaxCapacity * CapacityExcessFactor);

            if (capacityAfterRemoval >= totalRequiredThrougput)
            {
                Logger.Log($"[AUTOSCALE] Scaling DOWN ({removable.Id}). Load {removable.GetCurrentWorkloadRate():F1} fits into slack.");
                ConsumerGroup.RemoveConsumer(removable);
                ConsumerGroup.Rebalance();
                return Task.CompletedTask;
            }
            else
            {
                // Logger.Log($"[AUTOSCALE] Prevented Panic Kill of {removable.Id}. Remaining capacity {capacityAfterRemoval:F0} < Required {totalRequiredThrougput:F0}");
            }
        }

        // --- SCALE UP CHECK ---
        double totalRate = partitions.Sum(p => p.ProductionRate);
        long totalLag = partitions.Sum(p => p.GetTotalLag(ConsumerGroup.RebalanceTimeSeconds));
        
        // We use the stricter of "Rate+Lag" or "Just Rate" (usually Rate+Lag is higher)
        double requiredCap = totalRequiredThrougput; 

        int requiredCount = (int)Math.Ceiling(requiredCap / (CapacityExcessFactor * ConsumerGroup.ConsumerCapacity));
        if (partitions.Any() && requiredCount < 1) requiredCount = 1;

        // Compare against TOTAL consumers (Active + Booting)
        // This prevents panic scaling.
        if (requiredCount > allConsumers.Count)
        {
            int toAdd = requiredCount - allConsumers.Count;
            
            // DAMPENER: Don't add more than 3 consumers at once to prevent explosion
            toAdd = Math.Min(toAdd, 3);
            
            Logger.Log($"[AUTOSCALE] Scaling UP by {toAdd} (Req: {requiredCount}, Has: {allConsumers.Count}).");
            for (int i = 0; i < toAdd; i++) ConsumerGroup.AddConsumer();
            // AddConsumer triggers Rebalance internally usually, or Tick does.
        }

        return Task.CompletedTask;
    }
}