using MBrokerBench.Components;
using MBrokerBench.Models;

namespace MBrokerBench.Strategies;

public class BootingAwareModifiedWorstFitAssignment : IPartitionAssignmentStrategy
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
            .OrderByDescending(c => c.GetCurrentTotalLag(RebalanceTimeSeconds))
            .ToList();

        // --- PHASE 1: PRESERVE EXISTING ASSIGNMENTS ---
        foreach (var currentConsumer in sortedConsumers)
        {
            // Get previous ownership
            var pset = new HashSet<Partition>(partitions
                .Where(p => p.AssignedConsumer?.Id == currentConsumer.Id));

            // Sort by size (heaviest first to pack them)
            var sortedPSet = pset.OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds)).ToList();

            // Try to keep as many as possible
            for (int i = sortedPSet.Count - 1; i >= 0; i--)
            {
                var p = sortedPSet[i];
                // Check capacity (Note: Booting consumers have MaxCapacity > 0, so this passes)
                if (currentConsumer.GetCurrentWorkloadRate() + p.ProductionRate <= currentConsumer.MaxCapacity * CapacityExcessFactor)
                {
                    currentConsumer.AssignedPartitions.Add(p);
                    p.AssignedConsumer = currentConsumer;
                }
                else
                {
                    // No room, remove from set so it goes to overflow
                    pset.Remove(p);
                }
            }

            if (pset.Count == sortedPSet.Count) continue; // All fit

            // --- REASSIGN OVERFLOW (The items that didn't fit) ---
            var remaining = sortedPSet.Where(x => !pset.Contains(x)).ToList();
            if(remaining.Count == 0) continue;

            // Find an Empty consumer (Running preferred, then Booting)
            var overflowConsumer = consumers
                .Where(c => c.AssignedPartitions.Count == 0)
                .OrderByDescending(c => c.State == ConsumerState.Running) // Prefer active
                .ThenByDescending(c => c.MaxCapacity)
                .FirstOrDefault();

            if (overflowConsumer != null)
            {
                foreach (var p in remaining)
                {
                    if (overflowConsumer.GetCurrentWorkloadRate() + p.ProductionRate <= overflowConsumer.MaxCapacity * CapacityExcessFactor)
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
                                         .OrderByDescending(p => p.ProductionRate) // Largest rates first
                                         .ToList();

        foreach (var partition in finalU)
        {
            // 1. Try to fit into ANY consumer with space (Running OR Booting)
            // We prioritize Running consumers to reduce Lag immediately.
            var bestFitConsumer = consumers
                .Where(c => c.GetCurrentWorkloadRate() + partition.ProductionRate <= c.MaxCapacity * CapacityExcessFactor)
                .OrderByDescending(c => c.State == ConsumerState.Running) // Priority 1: Running
                .ThenByDescending(c => c.RemainingCapacity)               // Priority 2: Worst Fit (Leave large gaps for others)
                .FirstOrDefault();

            if (bestFitConsumer != null)
            {
                bestFitConsumer.AssignedPartitions.Add(partition);
                partition.AssignedConsumer = bestFitConsumer;
            }
            else
            {
                // No space anywhere? Create a consumer immediately if we are below limit.
                if (ConsumerGroup.Consumers.Count < partitions.Count)
                {
                    Logger.Log($"[Assignment] Partition {partition.Id} unassignable. Spawning new consumer.");
                    var newConsumer = ConsumerGroup.AddConsumer(ConsumerProfiles.Large.Name);
                    
                    // Assign immediately to the new consumer
                    newConsumer.AssignedPartitions.Add(partition);
                    partition.AssignedConsumer = newConsumer;
                    
                    // Add to local list so subsequent iterations can see it
                    consumers.Add(newConsumer);
                }
                else
                {
                    Logger.Log($"[Assignment] Partition {partition.Id} unassignable and max consumers reached. Left unassigned.");
                    partition.AssignedConsumer = null;
                }
            }
        }

        int uCount = finalU.Count(p => p.AssignedConsumer == null);
        if(uCount > 0)
            Logger.Log($"[Assignment] Complete. Unassigned: {uCount} (Waiting for Capacity)");
    }

    public Task AutoScale()
    {
        if (ConsumerGroup == null) return Task.CompletedTask;

        var allConsumers = ConsumerGroup.Consumers; // Includes Booting
        var partitions = ConsumerGroup.AllPartitions;

        // --- SCALE DOWN CHECK ---
        // Only consider removing RUNNING consumers.
        // We ensure we don't scale down if we have unassigned partitions or high global load.
        
        bool hasUnassigned = partitions.Any(p => p.AssignedConsumer == null);
        if (!hasUnassigned)
        {
            var removable = allConsumers
                .Where(c => c.State == ConsumerState.Running)
                .Where(c => c.GetCurrentWorkloadRate() < c.MaxCapacity * ScaleDownUtilizationThreshold)
                .OrderBy(c => c.GetCurrentWorkloadRate()) // Least loaded first
                .ThenBy(c => c.ConsumerProfile.CostPerSecond) // Then cheapest (if we have varied costs, maybe most expensive? Standard MWF assumes uniform usually)
                .FirstOrDefault();

            if (removable != null && allConsumers.Count > 1) 
            {
                // Verify if others can take the load
                double loadToMove = removable.GetCurrentWorkloadRate();
                double slackAvailable = allConsumers
                    .Where(c => c.Id != removable.Id && c.State == ConsumerState.Running)
                    .Sum(c => c.RemainingCapacityWithEfficiency * CapacityExcessFactor); // Be conservative with slack

                if (slackAvailable > loadToMove * 1.2) // 20% buffer
                {
                    Logger.Log($"[AUTOSCALE] Scaling DOWN (Removing {removable.Id}).");
                    ConsumerGroup.RemoveConsumer(removable);
                    ConsumerGroup.Rebalance();
                    return Task.CompletedTask;
                }
            }
        }

        return Task.CompletedTask;
    }
}