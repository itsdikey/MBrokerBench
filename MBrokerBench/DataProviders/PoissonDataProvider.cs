using MBrokerBench.Components;

namespace MBrokerBench.DataProviders
{
    public sealed class PoissonDataProvider : IDataProvider
    {
        public const string ScenarioUniform = "Uniform";
        public const string ScenarioSkewed5 = "Skewed5";
        public const string ScenarioSkewed9 = "Skewed9";


        private readonly Random _rng = new ();
        private readonly string _scenario;

        // 20 minute duration (1200 seconds)
        public int MaxRuntimeSteps { get; } = 1200;

        // Total System Throughput (λ)
        private const double PeakTotalLambda = 4500.0;
        private const double OffPeakTotalLambda = 800.0;

        public PoissonDataProvider(string scenario = ScenarioUniform)
        {
            _scenario = scenario;
        }

        public List<Partition> InitializePartitions()
        {
            int count = (_scenario == "Skewed9") ? 9 : 5;
            return Enumerable.Range(0, count)
                .Select(i => new Partition(i.ToString()))
                .ToList();
        }

        public List<Partition> Process(List<Partition> partitions, int timeStep)
        {
            // 1. Determine base intensity for this phase of the 20-min window
            double systemLambda = (timeStep >= 300 && timeStep <= 900)
                                    ? PeakTotalLambda : OffPeakTotalLambda;

            for (int i = 0; i < partitions.Count; i++)
            {
                var partition = partitions[i];

                // 2. Calculate the "Target" average rate for this partition based on scenario
                double targetLambda = GetTargetRate(i, partitions.Count, systemLambda);

                // 3. Generate the Poisson Random Variable (The actual message count for this second)
                int actualArrivals = GeneratePoisson(targetLambda);
                partition.ProductionRate = actualArrivals;

                //partition.ProductionRate = targetLambda;
            }

            return partitions;
        }

        private double GetTargetRate(int index, int total, double totalLambda)
        {
            return _scenario switch
            {
                "Skewed5" => (index < 3) ? (totalLambda * 0.80) / 3 : (totalLambda * 0.20) / 2,
                "Skewed9" => (index < 2) ? (totalLambda * 0.50) / 2 : (totalLambda * 0.50) / 7,
                _ => totalLambda / total
            };
        }

        private int GeneratePoisson(double lambda)
        {
            if (lambda <= 0) return 0;
            double L = Math.Exp(-lambda);
            double p = 1.0;
            int k = 0;
            do
            {
                k++;
                p *= _rng.NextDouble();
            } while (p > L);
            return k - 1;
        }
    }
}