using MBrokerBench.Components;
using MSodaClient;
using System.Text.Json.Serialization;

namespace MBrokerBench.DataProviders
{
    public class TaxiTrip
    {
        [JsonPropertyName("tpep_pickup_datetime")]
        public DateTime PickupTime { get; set; }

        [JsonPropertyName("pulocationid")]
        public string PickupLocationId { get; set; } = "0";
    }
    /// <summary>
    /// https://dev.socrata.com/foundry/data.cityofnewyork.us/2upf-qytp
    /// </summary>
    public sealed class NYTaxiDataProvider : IDataProvider, IScenarioOwner
    {
        private readonly string _scenario;
        private readonly List<TaxiTrip> _allTrips = new();
        private const int SpeedFactor = 40;

        // 14 hours (50400s) / 40 = 1260 seconds of simulation
        public int MaxRuntimeSteps { get; } = 1260;

        public string ScenarioName => _scenario;

        public NYTaxiDataProvider(string scenario = "Uniform")
        {
            _scenario = scenario;
            FetchData();
        }

        private void FetchData()
        {
            var config = new SODAV3ClientConfig
            {
                BaseUrl = "https://data.cityofnewyork.us",
                Timeout = 120,
                AppCredentials = ("90z5vpdemh492rvwnwqlfu66w", "55basrdth1fiapmuq044o7qeddjdeshxvqvk8085e95wk7scto"),
                EnableCaching = true
            };
            var client = new SODAV3Client(config);  //= new SODAV3Client("https://data.cityofnewyork.us", "90z5vpdemh492rvwnwqlfu66w", "55basrdth1fiapmuq044o7qeddjdeshxvqvk8085e95wk7scto"); // App Token optional for low volume

            // Soql query to get the 2 hour window
            // Note: Data is often delayed, ensure you are querying a date that exists in the set
            var soql = new SoqlQuery()
                .Select("tpep_pickup_datetime", "pulocationid")
                .Where("tpep_pickup_datetime >= '2019-01-02T08:00:00' AND tpep_pickup_datetime < '2019-01-02T22:00:00'")
                .Order("tpep_pickup_datetime");

            var resource = "2upf-qytp";

            var task = client.Query<TaxiTrip>(soql, resource);

            task.Wait();

            var data = task.Result;

            _allTrips.AddRange(data.ToList());
        }

        public List<Partition> InitializePartitions()
        {
            int count = _scenario == "Skewed9" ? 9 : 5;
            return Enumerable.Range(0, count)
                .Select(i => new Partition(i.ToString()))
                .ToList();
        }

        public List<Partition> Process(List<Partition> partitions, int timeStep)
        {
            // 1. Calculate the 'Reality' window for this simulation second
            // Sim Step 1 = Reality Seconds 0-39
            DateTime windowStart = DateTime.Parse("2019-01-02T08:00:00").AddSeconds(timeStep * SpeedFactor);
            DateTime windowEnd = windowStart.AddSeconds(SpeedFactor);

            // 2. Identify trips that happened in this 40-second slice
            var currentTrips = _allTrips.Where(t => t.PickupTime >= windowStart && t.PickupTime < windowEnd).ToList();
            int totalArrivals = currentTrips.Count * 10;

            // 3. Distribute arrivals based on Scenarios
            for (int i = 0; i < partitions.Count; i++)
            {
                int partitionShare = GetPartitionShare(i, partitions.Count, totalArrivals);

                partitions[i].ProductionRate = partitionShare;
            }

            return partitions;
        }

        private int GetPartitionShare(int index, int total, int totalArrivals)
        {
            double percentage = _scenario switch
            {
                "Skewed5" => (index < 3) ? 0.80 / 3 : 0.20 / 2,
                "Skewed9" => (index < 2) ? 0.50 / 2 : 0.50 / 7,
                _ => 1.0 / total
            };

            return (int)Math.Round(totalArrivals * percentage);
        }
    }
}