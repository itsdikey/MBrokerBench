namespace MBrokerBench
{
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
}
