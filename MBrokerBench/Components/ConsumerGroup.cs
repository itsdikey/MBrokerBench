using MBrokerBench.Models;

namespace MBrokerBench.Components
{
    public class ConsumerGroup
    {
        public string GroupId { get; }
        public List<Partition> AllPartitions { get; }
        public List<Consumer> Consumers { get; private set; } = new List<Consumer>();

        public List<Consumer> ActiveConsumers => Consumers.Where(c => c.State == ConsumerState.Running).ToList();
        public List<Consumer> BootingConsumers => Consumers.Where(c => c.State == ConsumerState.Booting).ToList();


        public double TotalCostPerSecond => Consumers.Sum(x => x.ConsumerProfile.CostPerSecond);

        public IReadOnlyList<ConsumerProfile> ConsumerProfiles { get; private set; }
        public ConsumerProfile DefaultProfile { get; private set; }
        public double ConsumerCapacity => _consumerCapacity;

        private readonly IPartitionAssignmentStrategy _assignmentStrategy;

        private readonly double _consumerCapacity;


        public double RebalanceTimeSeconds { get; } = 5; // rebalance blocking time

        public double LatencySLASeconds { get; } = 10;   // SLA window

       

        // Statistics for CSV/export
        private int _rebalanceSteps = 0;
        private int _totalReassignments = 0;
        private int _scaleUpOperations = 0;
        private int _scaleDownOperations = 0;
        private double _rScoreValue = 0.0;

        public int RebalanceSteps => _rebalanceSteps;
        public int TotalReassignments => _totalReassignments;
        public int ScaleUpOperations => _scaleUpOperations;
        public int ScaleDownOperations => _scaleDownOperations;
        public double RScoreValue => _rScoreValue;

        public ConsumerGroup(
            string groupId,
            List<Partition> partitions,
            List<ConsumerProfile> consumerProfiles, 
            ConsumerProfile defaultProfile,
            IPartitionAssignmentStrategy assignmentStrategy
            )
        {
            GroupId = groupId;
            AllPartitions = partitions;
            ConsumerProfiles = consumerProfiles;
            DefaultProfile = defaultProfile;

            _consumerCapacity = defaultProfile.MaxCapacity;

            _assignmentStrategy = assignmentStrategy;
            _assignmentStrategy.RebalanceTimeSeconds = RebalanceTimeSeconds;
            _assignmentStrategy.ConsumerGroup = this;
        }

        public Consumer AddConsumer(string? profileName = null, bool instant = false)
        {
            var profile = ConsumerProfiles.FirstOrDefault(p => p.Name == profileName) ?? DefaultProfile;
            var newConsumer = new Consumer($"C-{Guid.NewGuid()}", profile);
            
            if (instant)
            {
                newConsumer.State = ConsumerState.Running;
                newConsumer.StartupTimeRemaining = 0;
            }

            Consumers.Add(newConsumer);
            _scaleUpOperations++;
            MetricsExporter.SetConsumers(Consumers.Count);
            MetricsExporter.IncScaleUp();
            Console.WriteLine($"[SCALED UP] New Consumer {newConsumer.Id} {newConsumer.ConsumerProfile.ShortCode} added. Total: {Consumers.Count}");
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
            _scaleDownOperations++;
            MetricsExporter.SetConsumers(Consumers.Count);
            MetricsExporter.IncScaleDown();
            Console.WriteLine($"[SCALED DOWN] Consumer {consumer.Id} removed. Total: {Consumers.Count}");
        }

        // Rebalance all partitions using the configured strategy.
        public void Rebalance()
        {
            Console.WriteLine($"--- REBALANCING (Blocking for {RebalanceTimeSeconds}s) ---");

            // We are emulating rebalance time by blocking partition consumption for that duration if reassigned.
            var partitionConsumerMap = new Dictionary<string, string?>();

            foreach (var consumer in Consumers) 
            {
                foreach (var partition in consumer.AssignedPartitions) 
                {
                    partitionConsumerMap[partition.Id] = consumer.Id;
                }
            }

            _assignmentStrategy.Assign(AllPartitions, Consumers); // ActiveConsumers

            // increment rebalance counter
            _rebalanceSteps++;

            List<ReassignedPartitionDetails> reassignedPartitionsDetails = new List<ReassignedPartitionDetails>();

            foreach(var partition in AllPartitions)
            {
                if (partitionConsumerMap.TryGetValue(partition.Id, out var previousConsumerId))
                {
                    if (previousConsumerId != partition.AssignedConsumer?.Id)
                    {
                        // count reassignments
                        _totalReassignments++;
                        
                        if(partition.AssignedConsumer != null)
                        {
                            reassignedPartitionsDetails.Add(new ReassignedPartitionDetails(partition.AssignedConsumer.MaxCapacity, partition.ProductionRate));
                        }

                        // Apply the rebalance pause penalty directly here
                        partition.RebalancePenaltyRemaining = RebalanceTimeSeconds;
                    }
                }
            }

            _rScoreValue = MathUtils.CalculateRScore(reassignedPartitionsDetails);

            // Update partition metrics labels after rebalance
            foreach (var p in AllPartitions)
            {
                MetricsExporter.SetPartition(p.Id, p.CurrentLag, p.ProductionRate);
                MetricsExporter.SetPartitionAssignment(p.Id, p.AssignedConsumer?.Id);
            }

            // Update consumer metrics after rebalance
            foreach (var c in Consumers)
            {
                double util = c.GetCurrentWorkloadRate() / c.MaxCapacity * 100.0;
                MetricsExporter.SetConsumerMetrics(c.Id, util, c.AssignedPartitions.Count);
            }
        }

        public void Autoscale()
        {
            Rebalance();
            _assignmentStrategy.AutoScale();
        }

        private double _lastRebalanceTime;
        private double _totalTime = 0;

        // Configurable autoscale/rebalance timing (set once per class load)
        private static readonly double InitialAutoscaleDelaySeconds =
            double.TryParse(Environment.GetEnvironmentVariable("INITIAL_AUTOSCALE_DELAY_SECONDS"), out var d1) ? d1 : 30;
        private static readonly double AutoscaleIntervalSeconds =
            double.TryParse(Environment.GetEnvironmentVariable("AUTOSCALE_INTERVAL_SECONDS"), out var d2) ? d2 : 30;

        public void SyncRealConsumers(string profileName, int count)
        {
            var profile = ConsumerProfiles.FirstOrDefault(p => p.Name == profileName) ?? DefaultProfile;
            
            var existing = Consumers.Where(c => c.ConsumerProfile.Name == profileName).ToList();
            int current = existing.Count;

            if (current < count)
            {
                // Need to add consumers — preserves any autoscale-added consumers
                for (int i = current; i < count; i++)
                {
                    var c = new Consumer($"RC-{profile.ShortCode}-{i}", profile);
                    c.State = ConsumerState.Running;
                    c.Efficiency = 1.0; // SYNC-created consumers represent READY K8s pods at full speed
                    c.StartupTimeRemaining = 0;
                    Consumers.Add(c);
                }
            }
            else if (current > count)
            {
                // Need to remove excess consumers (scale-down from K8s)
                var toRemove = existing.OrderBy(c => c.AssignedPartitions.Count).TakeLast(current - count).ToList();
                foreach (var c in toRemove)
                {
                    foreach (var p in c.AssignedPartitions.ToList())
                    {
                        p.AssignedConsumer = null;
                        c.AssignedPartitions.Remove(p);
                    }
                    Consumers.Remove(c);
                }
            }
            // else current == count: no change needed
            
            MetricsExporter.SetConsumers(Consumers.Count);
        }

        // Shared helper: advances time, ticks consumers, handles lifecycle transitions,
        // triggers Autoscale on boot->running, and triggers periodic Autoscale using
        // INITIAL_AUTOSCALE_DELAY_SECONDS for the first fire and AUTOSCALE_INTERVAL_SECONDS
        // for subsequent fires.
        private void TickShared(double timeStepSeconds)
        {
            _totalTime += timeStepSeconds;

            bool consumerCameOnline = false;
            foreach (var c in Consumers)
            {
                var oldState = c.State;
                c.Tick(timeStepSeconds);

                if (oldState == ConsumerState.Booting && c.State == ConsumerState.Running)
                {
                    consumerCameOnline = true;
                    Console.WriteLine($"[LIFECYCLE] Consumer {c.Id} is now ONLINE (Warmup: {c.Efficiency * 100:F0}%).");
                }
            }

            if (consumerCameOnline)
            {
                Autoscale();
            }

            // Periodic autoscale: first fire at INITIAL_AUTOSCALE_DELAY_SECONDS, then every AUTOSCALE_INTERVAL_SECONDS
            bool shouldPeriodicAutoscale = _lastRebalanceTime == 0
                ? _totalTime >= InitialAutoscaleDelaySeconds
                : _totalTime - _lastRebalanceTime >= AutoscaleIntervalSeconds;

            if (shouldPeriodicAutoscale)
            {
                Autoscale();
                _lastRebalanceTime = _totalTime;
            }
        }

        // Virtual mode: simulates production, runs shared lifecycle/autoscale logic,
        // then runs consumption via virtual consumers. Returns simulated consumed count.
        public long TickVirtual(double timeStepSeconds)
        {
            // Production
            AllPartitions.ForEach(p => p.Produce(timeStepSeconds));

            // Shared lifecycle, boot detection, and periodic Autoscale
            TickShared(timeStepSeconds);

            // Consumption: virtual consumers drain the simulated backlog
            long stepConsumed = 0;
            foreach (var c in Consumers)
            {
                stepConsumed += c.Consume(timeStepSeconds);
            }

            return stepConsumed;
        }

        // Real-control-only mode: runs shared lifecycle/autoscale logic only.
        // No production simulation (DataProvider/Kafka handles it).
        // No consumption simulation (Kafka consumers drain real lag).
        public void TickRealControlOnly(double timeStepSeconds)
        {
            TickShared(timeStepSeconds);
        }
    }
}
