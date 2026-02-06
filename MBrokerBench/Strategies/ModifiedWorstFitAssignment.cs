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
            var pset = partitions
                .Where(p => p.AssignedConsumer?.Id == currentConsumer.Id)
                .OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds))
                .ToList();

            foreach (var p in pset)
            {
                // Check capacity (Note: Booting consumers have MaxCapacity > 0, so this passes)
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

            if (bestFitConsumer == null)
            {
                // Fallback: Best Effort - assign to consumer with most remaining capacity even if it exceeds MaxCapacity
                bestFitConsumer = consumers
                    .OrderByDescending(c => c.State == ConsumerState.Running)
                    .ThenByDescending(c => c.RemainingCapacity)
                    .FirstOrDefault();
            }

            if (bestFitConsumer != null)
            {
                bestFitConsumer.AssignedPartitions.Add(partition);
                partition.AssignedConsumer = bestFitConsumer;
            }
            else
            {
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
            .Where(c => c.State == ConsumerState.Running && c.Efficiency > 0.8) // Grace period
            .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * ScaleDownUtilizationThreshold)
            .OrderBy(c => c.GetCurrentWorkloadRate())
            .FirstOrDefault();

        if (removable != null && allConsumers.Count > 1) 
        {
            // SAFETY CHECK: Can the remaining fleet handle the TOTAL load?
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
        }

        // --- SCALE UP CHECK ---
        double requiredCap = totalRequiredThrougput; 

        int requiredCount = (int)Math.Ceiling(requiredCap / (CapacityExcessFactor * ConsumerGroup.ConsumerCapacity));
        if (partitions.Any() && requiredCount < 1) requiredCount = 1;

        // Cap at partition count
        requiredCount = Math.Min(requiredCount, partitions.Count);

        // Compare against TOTAL consumers (Active + Booting)
        if (requiredCount > allConsumers.Count)
        {
            int toAdd = requiredCount - allConsumers.Count;
            
            // DAMPENER: Don't add more than 2 consumers at once (conservative)
            toAdd = Math.Min(toAdd, 2);
            
            Logger.Log($"[AUTOSCALE] Scaling UP by {toAdd} (Req: {requiredCount}, Has: {allConsumers.Count}).");
            for (int i = 0; i < toAdd; i++) ConsumerGroup.AddConsumer();
        }

        return Task.CompletedTask;
    }
}