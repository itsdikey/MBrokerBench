using System.Text.RegularExpressions;

namespace MBrokerBench.Models
{
    /// <summary>
    /// Selects which per-second cost basis drives consumer-profile selection.
    /// </summary>
    public enum ProfileCostModel
    {
        /// <summary>
        /// Legacy synthetic cost scores (default). The experiment behaviour, CSV output
        /// and generated tables are unchanged under this model.
        /// </summary>
        Synthetic = 0,

        /// <summary>
        /// Explicit opt-in. Profile costs become the continuous regular AWS Fargate
        /// Linux/x86 on-demand per-second rates (hourly rate divided by 3600), so the
        /// CC-MWF controller chooses and scales profiles against real serverless prices.
        /// The 60-second invoice minimum is intentionally absent here; it is applied only
        /// when the per-task Fargate ledger is settled for reporting.
        /// </summary>
        Fargate = 1,
    }

    public record ConsumerProfile(string Name, double MaxCapacity, double CostPerSecond, double StartupTime)
    {
        public double CalculateTotalCost(double duration) => duration * CostPerSecond;

        public string GetProfileSummary() => $"Name: {Name}, Max Capacity: {MaxCapacity}, Cost Per Second: {CostPerSecond}";

        private string? shortCode = null;

        // ShortCode now removes consonants from the Name and returns the remaining characters in uppercase.
        // If removing consonants results in an empty string (e.g., Name has no vowels), fall back to the first letter uppercased.
        public string ShortCode => shortCode ??= GetShortCode();

        private string GetShortCode()
        {
            if (string.IsNullOrEmpty(Name))
                return string.Empty;

            // Remove consonants (letters other than vowels). Keep vowels and non-letter characters.
            var withoutConsonants = Regex.Replace(Name, "(?i)[aeiu]", "");
            withoutConsonants = withoutConsonants.ToUpperInvariant();

            if (string.IsNullOrWhiteSpace(withoutConsonants))
            {
                // Fallback to first character uppercased
                return Name.Substring(0, 1).ToUpperInvariant();
            }

            return withoutConsonants;
        }
    }


    public static class ConsumerProfiles
    {
        /// <summary>Environment value that selects the legacy synthetic profile costs.</summary>
        public const string SyntheticCostModeName = "synthetic";

        /// <summary>Environment value that selects the Fargate-aware profile costs.</summary>
        public const string FargateCostModeName = "fargate";

        public static readonly ConsumerProfile Small = new ConsumerProfile("Small", 500, 0.5,  10); // cost per 100 bytes = 0.1
        public static readonly ConsumerProfile Medium = new ConsumerProfile("Medium", 1000, 1, 10); // this is to simulate better cost ratio, e.g., cost per 100 bytes = 0.090
        public static readonly ConsumerProfile Large = new ConsumerProfile("Large", 2500, 2.5, 10); // large cost per 100 bytes = 0.096

        public static readonly List<ConsumerProfile> AllProfiles = new List<ConsumerProfile> { Small, Medium, Large };

        // -----------------------------------------------------------------------------
        // Opt-in Fargate-aware profiles.
        //
        // Capacity, startup time and names are identical to the synthetic profiles; only
        // CostPerSecond changes, to the continuous Fargate on-demand rate for the matching
        // task size (Small 0.5 vCPU/1 GiB, Medium 1/2, Large 2/4). These continuous rates
        // are what the CC-MWF controller optimises. The 60-second invoice minimum is NOT
        // included here; it is applied only by the Fargate task ledger at settlement.
        // -----------------------------------------------------------------------------
        public static readonly ConsumerProfile FargateSmall = new ConsumerProfile("Small", 500, FargatePerSecond(nameof(Small)), 10);
        public static readonly ConsumerProfile FargateMedium = new ConsumerProfile("Medium", 1000, FargatePerSecond(nameof(Medium)), 10);
        public static readonly ConsumerProfile FargateLarge = new ConsumerProfile("Large", 2500, FargatePerSecond(nameof(Large)), 10);

        public static readonly List<ConsumerProfile> FargateAwareProfiles =
            new List<ConsumerProfile> { FargateSmall, FargateMedium, FargateLarge };

        private static double FargatePerSecond(string profileName)
            => (double)FargatePricing.GetTaskDefinition(profileName).PerSecondRate;

        /// <summary>
        /// Parses a PROFILE_COST_MODE value. Null, empty, or any unrecognised value falls
        /// back to the legacy synthetic model so default experiments are unchanged.
        /// </summary>
        public static ProfileCostModel ParseCostModel(string? value)
        {
            if (string.Equals(value, FargateCostModeName, StringComparison.OrdinalIgnoreCase))
            {
                return ProfileCostModel.Fargate;
            }

            return ProfileCostModel.Synthetic;
        }

        /// <summary>The profile list the controller should use for the given cost model.</summary>
        public static List<ConsumerProfile> GetProfiles(ProfileCostModel model)
            => model == ProfileCostModel.Fargate ? FargateAwareProfiles : AllProfiles;

        /// <summary>The default profile (used when a strategy does not name a profile).</summary>
        public static ConsumerProfile GetDefaultProfile(ProfileCostModel model)
            => model == ProfileCostModel.Fargate ? FargateLarge : Large;
    }
}
