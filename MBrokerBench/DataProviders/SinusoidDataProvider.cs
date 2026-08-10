using MBrokerBench.Components;

namespace MBrokerBench.DataProviders
{
    public sealed class SinusoidDataProvider : IDataProvider, IScenarioOwner
    {
        public const string ScenarioUniform = "Uniform";
        public const string ScenarioSkewed5 = "Skewed5";
        public const string ScenarioSkewed9 = "Skewed9";

        public string ScenarioName => _scenario;

        private readonly Random _rng = new();
        private readonly string _scenario;

        public int MaxRuntimeSteps { get; } = 1200; // 20 minutes

        // --- Adjusted Sinusoid Parameters for 500-2500 Capacity Profiles ---
        private const double PeriodSeconds = 300.0;
        private const double Amplitude = 1200.0;     // Swings +/- 1200
        private const double VerticalShift = 1800.0;  // Base of 1800

        // --- Adjusted Pareto Noise (Stresses the "Large" profile spikes) ---
        private const double ParetoAlpha = 3.0;      // Lower alpha = heavier tail (more spikes)
        private const double ParetoXm = 450.0;       // Higher minimum noise floor

        public SinusoidDataProvider(string scenario = "Uniform")
        {
            _scenario = scenario;
        }

        public List<Partition> InitializePartitions()
        {
            int count = (_scenario == "Skewed36" || _scenario == "Uniform36") ? 36 : (_scenario == "Skewed9" || _scenario == "Uniform") ? 9 : 5;
            return Enumerable.Range(0, count)
                .Select(i => new Partition(i.ToString()))
                .ToList();
        }

        public List<Partition> Process(List<Partition> partitions, int timeStep)
        {
            // 1. Calculate the System-wide Rate at this second
            double totalSystemRate = GetCurrentSinusoidRate(timeStep);

            // Scale traffic for 36-partition runs
            if (_scenario == "Skewed36" || _scenario == "Uniform36")
            {
                totalSystemRate *= 5.0; // Baseline 5x, dynamically adjusted by python run scripts
            }

            // 2. Distribute with Skew Logic
            for (int i = 0; i < partitions.Count; i++)
            {
                double targetRate = GetTargetRate(i, partitions.Count, totalSystemRate);

                partitions[i].ProductionRate = targetRate;
            }

            return partitions;
        }

        private double GetCurrentSinusoidRate(double timeSeconds)
        {
            // A * sin(2πt/T) + V
            double sineComponent = Amplitude * Math.Sin((2 * Math.PI * timeSeconds) / PeriodSeconds)
                                   + VerticalShift;

            // Add the heavy-tailed Pareto noise
            double u = 1.0 - _rng.NextDouble();
            double noiseComponent = ParetoXm / Math.Pow(u, 1.0 / ParetoAlpha);

            return Math.Max(0, sineComponent + noiseComponent);
        }

        private double GetTargetRate(int index, int total, double totalRate)
        {
            return _scenario switch
            {
                ScenarioSkewed5 => (index < 2) ? (totalRate * 0.80) / 2 : (totalRate * 0.20) / 3,

                // 50% of total sine wave + noise goes to first 2 partitions
                ScenarioSkewed9 => (index < 2) ? (totalRate * 0.50) / 2 : (totalRate * 0.50) / 7,

                // 50% of total sine wave + noise goes to first 8 partitions, other 50% to remaining 28 partitions
                "Skewed36" => (index < 8) ? (totalRate * 0.50) / 8 : (totalRate * 0.50) / 28,

                // Uniform distribution
                ScenarioUniform or "Uniform36" => totalRate / total,

                _ => totalRate / total
            };
        }
    }
}