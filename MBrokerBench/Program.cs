using System.Text.Json;
using System.Threading.Tasks;

namespace MBrokerBench
{
    // Simulation runtime classes (models moved to MBrokerBench/Models/SimulationModels.cs)

    public class Partition
    {
        public string Id { get; }
        public long CurrentLag { get; set; } // Messages or bytes pending consumption
        public double ProductionRate { get; set; } // Messages(or bytes)/sec

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

        public double RemainingCapacity => MaxCapacity - GetCurrentWorkloadRate();

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

    public class PaperLeastLoadedBinPackStrategy : IPartitionAssignmentStrategy
    {
        // Configuration from Paper Section: "The Proposed Binpack-Based Autoscaler"
        public double RebalanceTimeSeconds { get; set; }
        public ConsumerGroup? ConsumerGroup { get; set; }

        // Parameters f_up and f_down as defined in the paper 
                    // f_up: Scale up threshold (e.g., 0.8). Ensures bin is not used to full capacity.
        private const double F_Up = 0.8;

        // f_down: Scale down threshold (e.g., 0.4). Controls frequency of scale down.
        private const double F_Down = 0.4;

        // Enums to represent the decision output of 'scaleNeeded'
        private enum ScaleAction { NONE, UP, DOWN, REASS }

        /// <summary>
        /// Implements the actual assignment of partitions to consumers.
        /// Corresponds to the execution logic inside 'Procedure doScale'
                    /// It uses 'Total Lag' (Eq. 8) which includes rebalancing time.
                    /// </summary>
        public void Assign(List<Partition> partitions, List<Consumer> consumers)
        {
            if (ConsumerGroup == null || !consumers.Any()) return;

            // Clear existing assignments (The paper does not support stickiness/incremental rebalancing)
            // "This makes the incremental rebalancing protocol not suitable for latency-awareness"
            foreach (var c in consumers) c.AssignedPartitions.Clear();
            foreach (var p in partitions) p.AssignedConsumer = null;

            // Calculate Lag_total for all partitions (Eq. 8) 
                        // Lag_total = Existing Lag + (Arrival Rate * Rebalance Time)
                        // In the code: p.GetTotalLag(tr) handles this calculation.

            // Execute Least-Loaded Packing using the FULL capacity (metrics not scaled by f_up/f_down during actual assignment)
            // The paper implies we pack into the available consumers optimizing for balance.
            PerformLeastLoadedBinPack(
                partitions,
                consumers,
                ConsumerGroup.ConsumerCapacity,
                ConsumerGroup.LatencySLASeconds,
                scaleFactor: 1.0 // Use real capacity for actual assignment
            );

            Console.WriteLine($"[ScaleWithLag] Assigned {partitions.Count} partitions to {consumers.Count} consumers using Least-Loaded heuristic.");
        }

        /// <summary>
        /// Implements "Algorithm 1: ScaleWithLag".
                    /// This is the main entry point triggered every decision interval.
                    /// </summary>
        public Task AutoScale()
        {
            if (ConsumerGroup == null) return Task.CompletedTask;

            var consumers = ConsumerGroup.Consumers;
            var partitions = ConsumerGroup.AllPartitions;
            double mu_capacity = ConsumerGroup.ConsumerCapacity;     // Average processing capacity 
            double w_sla = ConsumerGroup.LatencySLASeconds; // Desired latency 

            // 1. Call Procedure scaleNeeded 
            ScaleAction action = GetScaleNeeded(partitions, consumers, mu_capacity, w_sla);

            // 2. Logic from Procedure 'doScale' 
                        // Depending on action, we calculate G_next (target consumer count) and apply changes.

            List<Partition> totalLagPartitions = partitions; // In simulation, we treat these with TotalLag logic

            if (action == ScaleAction.UP || action == ScaleAction.REASS)
            {
                // Calculate G_(t+1) using Least-Loaded with f_up 
                int requiredCount = SimulateLeastLoaded(totalLagPartitions, mu_capacity, w_sla, F_Up);

                if (requiredCount > consumers.Count)
                {
                    // Scale UP by difference
                    int toAdd = requiredCount - consumers.Count;
                    Console.WriteLine($"[Algorithm 1] Action: UP. Adding {toAdd} replicas.");
                    for (int i = 0; i < toAdd; i++) ConsumerGroup.AddConsumer();
                    ConsumerGroup.Rebalance();
                }
                else if (action == ScaleAction.REASS)
                {
                    // Trigger Rebalance without changing count 
                    Console.WriteLine($"[Algorithm 1] Action: REASS. Rebalancing existing replicas.");
                    ConsumerGroup.Rebalance();
                }
            }
            else if (action == ScaleAction.DOWN)
            {
                // Calculate G_(t+1) using Least-Loaded with f_down
                int requiredCount = SimulateLeastLoaded(totalLagPartitions, mu_capacity, w_sla, F_Down);

                // Safety: Ensure we have at least 1 consumer if there is work
                if (requiredCount < 1 && partitions.Any()) requiredCount = 1;

                if (requiredCount < consumers.Count)
                {
                    // Scale DOWN by difference
                    int toRemove = consumers.Count - requiredCount;
                    Console.WriteLine($"[Algorithm 1] Action: DOWN. Removing {toRemove} replicas.");

                    // Remove the 'Least Loaded' consumers to minimize disruption, or just last ones.
                    // The paper relies on rebalance to fix assignment, so removal order is less critical 
                    // but removing empty ones is safer.
                    for (int i = 0; i < toRemove; i++)
                    {
                        // Simple removal logic
                        if (ConsumerGroup.Consumers.Any())
                            ConsumerGroup.RemoveConsumer(ConsumerGroup.Consumers.Last());
                    }
                    ConsumerGroup.Rebalance();
                }
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Implements "Procedure scaleNeeded".
                    /// Determines if UP, DOWN, or REASS (Reassign) is required.
                    /// </summary>
        private ScaleAction GetScaleNeeded(List<Partition> partitions, List<Consumer> currentConsumers, double mu, double w_sla)
        {
            int currentCount = currentConsumers.Count;

            // Line 2: G_(t+1) <- Least-Loaded(..., f_up)
                        // Note: The paper uses current metrics (lambda', lag') for this check, not totalLag (which is for doScale).
                        // However, usually autoscalers project lag. We will use the standard lag here.
            int neededForUp = SimulateLeastLoaded(partitions, mu, w_sla, F_Up);

            // Line 3: if |G_(t+1)| > |G_t| then return UP 
            if (neededForUp > currentCount)
            {
                return ScaleAction.UP;
            }

            // Line 6: G_(t+1) <- Least-Loaded(..., f_down) 
            int neededForDown = SimulateLeastLoaded(partitions, mu, w_sla, F_Down);

            // Line 7: if |G_(t+1)| < |G_t| then return DOWN 
            if (neededForDown < currentCount)
            {
                return ScaleAction.DOWN;
            }

            // Line 10: if assignmentViolatesTheSLA(...) return REASS 
            if (CheckSLAViolation(currentConsumers, mu, w_sla, F_Up))
            {
                return ScaleAction.REASS;
            }

            return ScaleAction.NONE;
        }

        /// <summary>
        /// Implements "Function assignmentViolatesTheSLA".
        /// Checks if any current consumer violates the Latency or Capacity constraints.
        /// </summary>
        private bool CheckSLAViolation(List<Consumer> consumers, double mu, double w_sla, double f)
        {
            // Line 1: foreach m_j in G_m
            foreach (var consumer in consumers)
            {
                double lag_mj = consumer.AssignedPartitions.Sum(p => p.CurrentLag); // lag defined in Eq 3
                double lambda_mj = consumer.AssignedPartitions.Sum(p => p.ProductionRate); // lambda defined in Eq 4 

                // Line 2: Check constraints 
                            // Constraint 1: Lag > Capacity * SLA * f
                bool lagViolation = lag_mj > (mu * w_sla * f);

                // Constraint 2: ArrivalRate > Capacity * f
                bool capacityViolation = lambda_mj > (mu * f);

                if (lagViolation || capacityViolation)
                {
                    return true; // Return true 
                }
            }
            return false;
        }

        /// <summary>
        /// Implements the "Least-Loaded" bin pack heuristic simulation.
        /// Returns the NUMBER of consumers (bins) required.
        /// Used in "scaleNeeded" to plan for next interval.
        /// </summary>
        /// <param name="muConsumerCapacity">The processing capacity of each consumer (bin).</param>
        /// <param name="f">The scaling factor (f_up or f_down).</param>
        /// <param name="partitions">The list of partitions to pack.</param>
        /// <param name="w_sla">The target SLA</param>
        private int SimulateLeastLoaded(List<Partition> partitions, double muConsumerCapacity, double w_sla, double f)
        {
            // We simulate packing partitions into theoretical bins.
            // We start with 0 bins and add as needed, OR we can try to fit into current count?
            // The paper implies finding the "Minimal set of replicas"
            // So we simulate filling bins from scratch to find the count.

            List<double> binLoads = new List<double>(); // Stores current rate (lambda) of each bin
            List<double> binLags = new List<double>();  // Stores current lag of each bin

            // Sort partitions descending by Lag/Load to optimize packing (Standard Bin Pack practice)
            // Though "Least-Loaded" refers to the bin selection, sorting items helps.
            var sortedPartitions = partitions.OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds)).ToList();

            foreach (var p in sortedPartitions)
            {
                double pLag = p.GetTotalLag(RebalanceTimeSeconds);
                double pRate = p.ProductionRate;

                // Find the "Least Loaded" bin that can fit this partition
                // Least Loaded here means the bin with the lowest utilization (balancing load) 

                int bestBinIndex = -1;
                double minLoadFound = double.MaxValue;

                for (int i = 0; i < binLoads.Count; i++)
                {
                    // Check if this bin can accept the partition without violating constraints 
                                // Constraint: NewLag < mu * w_sla * f  AND  NewRate < mu * f
                    double projectedLag = binLags[i] + pLag;
                    double projectedRate = binLoads[i] + pRate;

                    bool fits = (projectedLag < muConsumerCapacity * w_sla * f) && (projectedRate < muConsumerCapacity * f);

                    if (fits)
                    {
                        // Heuristic: Pick the bin with the lowest current load (Least-Loaded)
                        if (binLoads[i] < minLoadFound)
                        {
                            minLoadFound = binLoads[i];
                            bestBinIndex = i;
                        }
                    }
                }

                if (bestBinIndex != -1)
                {
                    // Add to existing bin
                    binLoads[bestBinIndex] += pRate;
                    binLags[bestBinIndex] += pLag;
                }
                else
                {
                    // Create new bin (Scale up simulation)
                    // Check if partition itself is too big for a single consumer (Corner case)
                    if (pLag > muConsumerCapacity * w_sla * f || pRate > muConsumerCapacity * f)
                    {
                        // It requires a dedicated consumer (or exceeds capacity completely)
                        // We add a bin, but strictly it violates SLA. We pack it anyway to count it.
                    }
                    binLoads.Add(pRate);
                    binLags.Add(pLag);
                }
            }

            return binLoads.Count;
        }

        /// <summary>
        /// Executes the actual Least-Loaded Heuristic on REAL consumers objects.
        /// Used inside 'Assign'.
        /// </summary>
        private void PerformLeastLoadedBinPack(
            List<Partition> partitions,
            List<Consumer> consumers,
            double mu,
            double w_sla,
            double scaleFactor)
        {
            // Sort partitions to assign heavy items first
            var sortedPartitions = partitions.OrderByDescending(p => p.GetTotalLag(RebalanceTimeSeconds)).ToList();

            foreach (var p in sortedPartitions)
            {
                // Find "Least Loaded" consumer that satisfies constraints
                // "Least-Loaded heuristic ... maintains uniform and balanced load"

                var candidates = consumers.Where(c =>
                {
                    // Check constraints (Eq. 5)
                    double newWorkload = c.GetCurrentWorkloadRate() + p.ProductionRate;
                    double newLag = c.GetCurrentTotalLag(RebalanceTimeSeconds) + p.GetTotalLag(RebalanceTimeSeconds);

                    return newWorkload <= (mu * scaleFactor) &&
                           newLag <= (mu * w_sla * scaleFactor);
                }).ToList();

                Consumer? target = null;

                if (candidates.Any())
                {
                    // Select the one with the lowest current workload (Least Loaded)
                    // This corresponds to 'Worst Fit' (Maximize remaining capacity)
                    target = candidates.OrderBy(c => c.GetCurrentWorkloadRate()).First();
                }
                else
                {
                    // Fallback: If no consumer satisfies SLA (overload), pick the absolute least loaded 
                    // to spread the pain, even if it violates constraints.
                    target = consumers.OrderBy(c => c.GetCurrentWorkloadRate()).FirstOrDefault();
                }

                if (target != null)
                {
                    target.AssignedPartitions.Add(p);
                    p.AssignedConsumer = target;
                }
            }
        }
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
                // Simple check: if there's nothing to process, we aim for 0 consumers.
                // Downscale logic below will handle the safe removal of one consumer at a time.
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
                // downscale everything
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

    public class ConsumerGroup
    {
        public string GroupId { get; }
        public List<Partition> AllPartitions { get; }
        public List<Consumer> Consumers { get; private set; } = new List<Consumer>();

        public double ConsumerCapacity => _consumerCapacity;

        private readonly IPartitionAssignmentStrategy _assignmentStrategy;
        private readonly double _consumerCapacity;

        public double RebalanceTimeSeconds { get; } = 5.0; // rebalance blocking time

        public double LatencySLASeconds { get; } = 10.0;   // SLA window

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

            //We are emulating rebalance time by blocking partition production for that duration if reassigned.
            //This will cause backlog to appear on reassigned partitions.
            var partitionConsumerMap = new Dictionary<string, string?>();

            foreach (var consumer in Consumers) 
            {
                foreach (var partition in consumer.AssignedPartitions) 
                {
                    partitionConsumerMap[partition.Id] = consumer.Id;
                }
            }

            _assignmentStrategy.Assign(AllPartitions, Consumers);

            foreach(var partition in AllPartitions)
            {
                if (partitionConsumerMap.TryGetValue(partition.Id, out var previousConsumerId))
                {
                    if (previousConsumerId != partition.AssignedConsumer?.Id)
                    {
                        partition.Produce(RebalanceTimeSeconds);
                    }
                }
            }

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

        public void Autoscale()
        {
            _assignmentStrategy.AutoScale();
        }
    }

    public class BrokerSimulator
    {
        private const double TimeStepSeconds = 1.0;
        private const double ConsumerBaseCapacity = 1000.0; // bytes/sec per consumer

        public static async Task Main()
        {
            Console.WriteLine("Starting Kafka Autoscaling Simulation (Config-Driven)...");

            IPartitionAssignmentStrategy assignmentStrategy = new ModifiedWorstFitAssignment();

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

            var group = new ConsumerGroup("MyGroup", partitions, ConsumerBaseCapacity, assignmentStrategy);

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
                    .Max(p => p == null ? 0 : p.CurrentLag / (p.AssignedConsumer?.MaxCapacity ?? ConsumerBaseCapacity));

                var totalProductionRate = group.AllPartitions.Sum(p => p.ProductionRate);
                var averageProductionRate = group.AllPartitions.Count > 0 ? totalProductionRate / group.AllPartitions.Count : 0.0;


                Console.WriteLine($"Total System Lag: {totalLag} messages. Total Production Rate: {totalProductionRate:F1} msgs/s. Average Production Rate: {averageProductionRate:F1} msgs/s");
                Console.WriteLine($"Max Estimated Latency (Worst-Case): {maxLagTime:F2} seconds (Target: {group.LatencySLASeconds}s)");

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

                    Console.WriteLine($"  {consumer.Id}: Messages={consumerLag:F0} msg Rate={consumer.GetCurrentWorkloadRate():F0} msgs/s. Util={utilization:F1}%. Partitions: {string.Join(", ", consumer.AssignedPartitions.Select(p => p.Id))}");
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
