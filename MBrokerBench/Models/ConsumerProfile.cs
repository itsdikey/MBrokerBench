using System.Text.RegularExpressions;

namespace MBrokerBench.Models
{
    public record ConsumerProfile(string Name, double MaxCapacity, double CostPerSecond)
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
        public static readonly ConsumerProfile Small = new ConsumerProfile("Small", 500, 0.5); // cost per 100 bytes = 0.1
        public static readonly ConsumerProfile Medium = new ConsumerProfile("Medium", 1000, 0.9); // this is to simulate better cost ratio, e.g., cost per 100 bytes = 0.090
        public static readonly ConsumerProfile Large = new ConsumerProfile("Large", 2500, 2.4); // large cost per 100 bytes = 0.096

        public static readonly List<ConsumerProfile> AllProfiles = new List<ConsumerProfile> { Small, Medium, Large };
    }
}
