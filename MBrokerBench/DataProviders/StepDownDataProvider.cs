using MBrokerBench.Components;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MBrokerBench.DataProviders
{
    public sealed class StepDownDataProvider : IDataProvider
    {
        public const string ScenarioHighToLow = "HighToLow";

        private readonly Random _rng = new();
        private readonly string _scenario;

        public int MaxRuntimeSteps { get; } = 1200; // 10 minutes

        // Phase settings
        private const int HighLoadDuration = 180; // 3 minutes of high load
        private const int LowLoadDuration = 300;  // 5 minutes of low load
        private const double HighLoadTotalRate = 5500.0; // Forces multiple Large consumers
        private const double LowLoadTotalRate = 600.0;   // Should fit in 1 Medium or 2 Smalls

        public StepDownDataProvider(string scenario = ScenarioHighToLow)
        {
            _scenario = scenario;
        }

        public List<Partition> InitializePartitions()
        {
            // Use 5 partitions to allow for some distribution complexity
            return Enumerable.Range(0, 5)
                .Select(i => new Partition(i.ToString()))
                .ToList();
        }

        public List<Partition> Process(List<Partition> partitions, int timeStep)
        {
            double currentTotalRate;

            if (timeStep <= HighLoadDuration)
            {
                currentTotalRate = HighLoadTotalRate;
            }
            else if (timeStep <= HighLoadDuration + LowLoadDuration)
            {
                currentTotalRate = LowLoadTotalRate;
            }
            else
            {
                // Go back to high load
                currentTotalRate = HighLoadTotalRate;
            }

            // Add small noise (jitter) +/- 5%
            double noise = (currentTotalRate * 0.05) * (_rng.NextDouble() * 2 - 1);
            currentTotalRate += noise;

            // Distribute uniformly for now to test pure vertical scaling
            double ratePerPartition = currentTotalRate / partitions.Count;

            foreach (var p in partitions)
            {
                p.ProductionRate = ratePerPartition;
            }

            return partitions;
        }
    }
}