using ConsolePlot;
using ConsolePlot.Drawing.Tools;
using MBrokerBench.Components;
using MBrokerBench.DataProviders;
using MBrokerBench.Models;
using MBrokerBench.Strategies;
using Microsoft.Extensions.Logging;
using System.Text;

namespace MBrokerBench
{
    // Centralized logger to enable/disable console output from one place.
    public static class Logger
    {
        // Set to false to silence all Logger.Log calls.
        public static bool Enabled { get; set; } = true;

        public static void Log(string message, LogLevel logLevel = LogLevel.Information)
        {
            if (!Enabled)
            {
                return;
            }

            var oldColor = Console.ForegroundColor;
            Console.ForegroundColor = logLevel switch
            {
                LogLevel.Information => ConsoleColor.White,
                LogLevel.Warning => ConsoleColor.Yellow,
                LogLevel.Error => ConsoleColor.Red,
                LogLevel.Debug => ConsoleColor.Gray,
                _ => ConsoleColor.White,
            };
            Console.WriteLine(message);
            Console.ForegroundColor = oldColor;
        }

        public static void Log(string format, LogLevel logLevel = LogLevel.Information, params object[] args)
        {
            if (!Enabled)
            {
                return;
            }

            var oldColor = Console.ForegroundColor;
            Console.ForegroundColor = logLevel switch
            {
                LogLevel.Information => ConsoleColor.White,
                LogLevel.Warning => ConsoleColor.Yellow,
                LogLevel.Error => ConsoleColor.Red,
                LogLevel.Debug => ConsoleColor.Gray,
                _ => ConsoleColor.White,
            };
            Console.WriteLine(format, args);
            Console.ForegroundColor = oldColor;
        }
    }

    public class BrokerSimulator
    {
        private const double TimeStepSeconds = 1;

        public enum DebugMode
        {
            Console,
            Plot
        }

        public const DebugMode Mode = DebugMode.Console;

        public static async Task Main()
        {
            Logger.Log("Starting Kafka Autoscaling Simulation (Config-Driven)...");

            string strategyName = System.Environment.GetEnvironmentVariable("STRATEGY") ?? "CostCentric";
            IPartitionAssignmentStrategy assignmentStrategy;

            var shortCodeAlgorithm = "unknown";

            switch (strategyName)
            {
                case "CostCentric":
                    assignmentStrategy = new CostCentricModifiedWorstFitAssignment();
                    shortCodeAlgorithm = "cc_mwf";
                    break;
                case "BootingAware":
                    assignmentStrategy = new BootingAwareModifiedWorstFitAssignment();
                    shortCodeAlgorithm = "ba_mwf";
                    break;
                case "ScaleWithLag":
                    assignmentStrategy = new PaperLeastLoadedBinPackStrategy();
                    shortCodeAlgorithm = "swl";
                    break;
                case "Linear":
                    assignmentStrategy = new ModifiedWorstFitAssignment(); // Uses total load / capacity
                    shortCodeAlgorithm = "linear";
                    break;
                case "ModifiedWorstFit":
                default:
                    assignmentStrategy = new ModifiedWorstFitAssignment();
                    shortCodeAlgorithm = "mwf";
                    break;
            }
            
            Logger.Log($"Selected Strategy: {assignmentStrategy.GetType().Name}");

            // Start metrics endpoint with strategy/run labels (from environment)
            var strategyEnv = assignmentStrategy.GetType().Name;
            var runIdEnv = System.Environment.GetEnvironmentVariable("RUN_ID") ?? DateTimeOffset.UtcNow.Subtract(DateTimeOffset.UnixEpoch).TotalSeconds.ToString();//current unix epoch 
            MetricsExporter.Init(1234, strategyEnv, runIdEnv);

            // Use JSON config data provider to initialize partitions and handle rate/events
            var configPath = Path.Combine(AppContext.BaseDirectory, "simulation_config.json");
            //var provider = new JSONConfigDataProvider(configPath);
            //var provider = new PoissonDataProvider(PoissonDataProvider.ScenarioSkewed9);
            //var provider = new SinusoidDataProvider(SinusoidDataProvider.ScenarioSkewed9);

            // Optional: Replay or Save simulation. Use environment variables:
            // REPLAY_SIM_PATH -> path to recorded simulation to replay
            // SAVE_SIM_PATH   -> path to save recorded simulation (wraps the selected provider)
            string replayPath = System.Environment.GetEnvironmentVariable("REPLAY_SIM_PATH");
            string savePath = System.Environment.GetEnvironmentVariable("SAVE_SIM_PATH");

            dynamic provider;

            string providerName = "";

            if (!string.IsNullOrEmpty(replayPath))
            {
                provider = new ReplayDataProvider(replayPath);
            }
            else
            {
                // Select provider based on environment variable
                string dataProviderEnv = System.Environment.GetEnvironmentVariable("DATA_PROVIDER") ?? "Poisson";
                IDataProvider baseProvider = dataProviderEnv switch
                {
                    "Poisson" => new PoissonDataProvider(PoissonDataProvider.ScenarioSkewed9),
                    "Sinusoid" => new SinusoidDataProvider(SinusoidDataProvider.ScenarioUniform),
                    "NYTaxi" or _ => new NYTaxiDataProvider(SinusoidDataProvider.ScenarioUniform)
                };


                providerName = dataProviderEnv + ((baseProvider is IScenarioOwner so)?$"_{so.ScenarioName}":"");

                // var baseProvider = new NYTaxiDataProvider(SinusoidDataProvider.ScenarioSkewed9);
                // var baseProvider = new PoissonDataProvider(SinusoidDataProvider.ScenarioSkewed9);
                if (!string.IsNullOrEmpty(savePath))
                {
                    provider = new SimulationRecorder(savePath, baseProvider);
                }
                else
                {
                    provider = baseProvider;
                }
            }

            var partitions = provider.InitializePartitions();
            int maxRuntime = provider.MaxRuntimeSteps > 0 ? provider.MaxRuntimeSteps : 600;

            // Prepare CSV export for timestep series
            var outDir = Path.Combine(AppContext.BaseDirectory, "export_csv");
            Directory.CreateDirectory(outDir);

            string resultAnalytics = Path.Combine(outDir, $"analysis_{shortCodeAlgorithm}_{providerName}.csv");

            // Initialize consumer group
            var group = new ConsumerGroup("MyGroup", partitions, ConsumerProfiles.AllProfiles, ConsumerProfiles.Large, assignmentStrategy);
            var csvPath = Path.Combine(outDir, $"timeseries_{strategyEnv}_{runIdEnv}.csv");
            using var csvWriter = new StreamWriter(csvPath, false, Encoding.UTF8);

            // Build CSV header and include a column per consumer profile type
            var headerParts = new List<string>
            {
                "step",
                "timestamp",
                "current_system_lag",
                "messages_pending",
                "current_production_rate",
                "current_consumption_rate",
                "total_system_load",
                "current_system_cost",
                "total_reassignments",
                "total_rebalance_steps",
                "rScore_value",
                "total_consumers"
            };

            // Add some additional per-step columns
            headerParts.Add("reassignments_this_step");
            headerParts.Add("max_estimated_latency_seconds");
            headerParts.Add("partitions_violating_sla");

            // Add columns for each consumer profile (counts)
            foreach (var prof in ConsumerProfiles.AllProfiles)
            {
                // use profile short code for compactness
                headerParts.Add($"count_{prof.ShortCode}");
            }

            // Add per-profile production and backlog columns
            foreach (var prof in ConsumerProfiles.AllProfiles)
            {
                headerParts.Add($"production_{prof.ShortCode}");
            }
            foreach (var prof in ConsumerProfiles.AllProfiles)
            {
                headerParts.Add($"backlog_{prof.ShortCode}");
            }

            csvWriter.WriteLine(string.Join(',', headerParts));

            var rndRate = new Random();

            double lastLagTime = -1;
            int lastTotalReassignments = group.TotalReassignments;

            double sumProductionRate = 0;
            double sumCost = 0;
            double sumMaxLatency = 0;
            int totalViolationSteps = 0;

            #region Plot Init
            List<int> steps = new List<int>();
            List<double> productionRate = new List<double>();
            // create plot sized to your console
            var plt = new Plot(120, 30);
            #endregion

            Logger.Enabled = Mode == DebugMode.Console;

            for (int step = 1; step <= maxRuntime; step++)
            {
                if(Mode == DebugMode.Plot)
                {
                    Console.CursorLeft = 0;
                    Console.CursorTop = 0;

                    steps.Add(step);
                    productionRate.Add(group.AllPartitions.Sum(p => p.ProductionRate));

                    // prepare arrays
                    var xs = steps.Select(i => (double)i).ToArray();
                    var ys = productionRate.ToArray();


                    // axis/grid settings (optional)
                    plt.Axis.IsVisible = true;
                    plt.Grid.IsVisible = true;
                    plt.Ticks.IsVisible = true;

                    // add the series (line)
                    plt.AddSeries(xs, ys, new PointPen(SystemPointBrushes.Dot, ConsoleColor.Green));

                    // draw & render
                    Console.OutputEncoding = System.Text.Encoding.UTF8;
                    plt.Draw();
                    plt.Render();
                }
            

                Logger.Log($"\n--- SIMULATION STEP {step} ---");
                // Let provider process rate changes / events for this timestep
                provider.Process(group.AllPartitions, step);

                var stepConsumed = group.Tick(TimeStepSeconds);

                // Reporting
                Logger.Log($"Current Consumers: {group.Consumers.Count}");
                Logger.Log($"Current Partitions: {group.AllPartitions.Count}");
                long totalLag = group.AllPartitions.Sum(p => p.CurrentLag);

                double maxLagTime = group.AllPartitions
                    .Where(p => p.AssignedConsumer != null)
                    .DefaultIfEmpty()
                    .Max(p => p == null ? 0 : (p.CurrentLag + p.ProductionRate) / (p.AssignedConsumer?.MaxCapacity ?? 1000));

                var totalProductionRate = group.AllPartitions.Sum(p => p.ProductionRate);
                var averageProductionRate = group.AllPartitions.Count > 0 ? totalProductionRate / group.AllPartitions.Count : 0.0;
                var consumptionRate = stepConsumed / TimeStepSeconds;

                Logger.Log("ALGORITHM: " + assignmentStrategy.GetType().Name);


                var isBootingAssignedPartition = group.Consumers.Any(c => c.State == ConsumerState.Booting && c.AssignedPartitions.Count != 0);
                if (isBootingAssignedPartition)
                {
                    Logger.Log("Is Booting Partition Assigned", LogLevel.Warning);
                }

                var unassignedPartitions = group.AllPartitions.Where(p => p.AssignedConsumer == null).ToList();

                if (unassignedPartitions.Count > 0)
                {
                    Logger.Log($"Unassigned Partitions: {unassignedPartitions.Count}", LogLevel.Error);
                    foreach (var p in unassignedPartitions)
                    {
                        Logger.Log($"  Partition {p.Id} Lag={p.CurrentLag} Rate={p.ProductionRate}", LogLevel.Error);
                    }
                }

                Logger.Log($"Total System Lag: {totalLag} messages. Total Production Rate: {totalProductionRate:F1} msgs/s. Total Consumption Rate: {consumptionRate:F1} msgs/s. Average Production Rate: {averageProductionRate:F1} msgs/s");
                Logger.Log($"Max Estimated Latency (Worst-Case): {maxLagTime:F2} seconds (Target: {group.LatencySLASeconds}s)");
                Logger.Log($"Total System Cost: {group.TotalCostPerSecond}");

                var counter = new Dictionary<string, int>();
                foreach(var consumer in group.Consumers)
                {
                    var profileCode = consumer.ConsumerProfile.ShortCode;
                    if (!counter.ContainsKey(profileCode))
                        counter[profileCode] = 0;
                    counter[profileCode]++;
                }

                Logger.Log("Consumer Profiles:");
                foreach(var kvp in counter)
                {
                    Logger.Log($"  {kvp.Key}: {kvp.Value}");
                }


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

                    double lagTime = consumerLag / consumer.MaxCapacity;

                    //Console.WriteLine($"{consumer.Id}: Profile={consumer.ConsumerProfile.ShortCode} Messages={consumerLag:F0} Lag={lagTime:F2}s Msg Rate={consumer.GetCurrentWorkloadRate():F0} msgs/s. D={(consumer.GetCurrentWorkloadRate()-consumer.ConsumerProfile.MaxCapacity):F0} Util={utilization:F1}%. Partitions: {string.Join(", ", consumer.AssignedPartitions.Select(p => p.Id))}");
                }

                // CSV export per timestep
                double currentConsumptionRate = stepConsumed / TimeStepSeconds;

                // total system load: approximate as production rate + queued lag per SLA window
                double totalSystemLoad = totalProductionRate + (double)totalLag / group.LatencySLASeconds;

                double currentSystemCost = group.TotalCostPerSecond;

                // Build base columns
                var rowParts = new List<string>
                {
                    step.ToString(),
                    DateTime.UtcNow.ToString("o"),
                    totalLag.ToString(),
                    totalLag.ToString(),
                    totalProductionRate.ToString("F3"),
                    currentConsumptionRate.ToString("F3"),
                    totalSystemLoad.ToString("F3"),
                    currentSystemCost.ToString("F3"),
                    group.TotalReassignments.ToString(),
                    group.RebalanceSteps.ToString(),
                    group.RScoreValue.ToString("F5"),
                    group.Consumers.Count.ToString()
                };

                // reassignments this step (delta)
                var reassignmentsThisStep = group.TotalReassignments - lastTotalReassignments;
                lastTotalReassignments = group.TotalReassignments;
                rowParts.Add(reassignmentsThisStep.ToString());

                // max estimated latency seconds
                rowParts.Add(maxLagTime.ToString("F3"));

                // partitions violating SLA
                int partitionsViolating = group.AllPartitions.Count(p => {
                    // if (p.CurrentLag <= 0) return false;
                    if (p.AssignedConsumer == null) return true; // unassigned with lag -> violation
                    double est = (p.CurrentLag + p.ProductionRate) / p.AssignedConsumer.MaxCapacity;
                    return est > group.LatencySLASeconds;
                });
                rowParts.Add(partitionsViolating.ToString());

                // Metrics Tracking for final output
                sumProductionRate += totalProductionRate;
                sumCost += currentSystemCost;
                sumMaxLatency += maxLagTime;
                if (partitionsViolating > 0)
                {
                    totalViolationSteps++;
                }

                // Append counts per consumer profile in the same order as header
                foreach (var prof in ConsumerProfiles.AllProfiles)
                {
                    var cnt = group.Consumers.Count(c => c.ConsumerProfile.Name == prof.Name);
                    rowParts.Add(cnt.ToString());
                }

                // Append per-profile production (sum of production rates for partitions assigned to consumers of that profile)
                foreach (var prof in ConsumerProfiles.AllProfiles)
                {
                    double prod = group.Consumers
                        .Where(c => c.ConsumerProfile.Name == prof.Name)
                        .Sum(c => c.AssignedPartitions.Sum(p => p.ProductionRate));
                    rowParts.Add(prod.ToString("F3"));
                }

                // Append per-profile backlog (sum of CurrentLag for partitions assigned to consumers of that profile)
                foreach (var prof in ConsumerProfiles.AllProfiles)
                {
                    double backlog = group.Consumers
                        .Where(c => c.ConsumerProfile.Name == prof.Name)
                        .Sum(c => c.AssignedPartitions.Sum(p => p.CurrentLag));
                    rowParts.Add(backlog.ToString("F0"));
                }

                csvWriter.WriteLine(string.Join(',', rowParts));

                csvWriter.Flush();


                if (lastLagTime != -1)
                {
                    double lagChange = maxLagTime - lastLagTime;
                    Logger.Log($"Lag Change This Step: {lagChange} messages ({(lagChange / TimeStepSeconds):F1} msgs/s)");

                    if (lagChange > 8)
                    {
                        Logger.Log($"[ALERT] System lag is INCREASING! (+{lagChange:F2} seconds this step)");

                        // Console.ReadLine();
                    }
                }

                lastLagTime = maxLagTime;

                Thread.Sleep(1);
            }

            // close csv writer by disposing via using
            csvWriter.Close();
            csvWriter.Dispose();

            // Output Final Results
            double finalAvgProd = sumProductionRate / maxRuntime;
            double finalAvgCost = sumCost / maxRuntime;
            double finalAvgLat = sumMaxLatency / maxRuntime;
            int finalViolDur = totalViolationSteps * (int)TimeStepSeconds;

            Logger.Enabled = true;
            Logger.Log("\n" + new string('=', 60));
            Logger.Log("FINAL SIMULATION SUMMARY");
            Logger.Log(new string('=', 60));
            Logger.Log($"{"Avg Prod (msg/s):",-20} {finalAvgProd:F2}");
            Logger.Log($"{"Avg Cost ($):",-20} {finalAvgCost:F2}");
            Logger.Log($"{"Avg Latency (s):",-20} {finalAvgLat:F2}");
            Logger.Log($"{"Viol. Dur. (s):",-20} {finalViolDur}");
            Logger.Log(new string('=', 60));
            Logger.Log($"{finalAvgProd:F2}\t{finalAvgCost:F2}\t{finalAvgLat:F2}\t{finalViolDur}");
            Logger.Log(new string('=', 60) + "\n");

            // Write final summary to resultAnalytics file for later processing
            File.WriteAllText(resultAnalytics, $"{finalAvgProd:F2},{finalAvgCost:F2},{finalAvgLat:F2},{finalViolDur}");

            // If we recorded the simulation, save it now
            if (provider is SimulationRecorder recorder)
            {
                try
                {
                    recorder.Save();
                    Logger.Log($"Recorded simulation saved to: {recorder.Path}");
                }
                catch (Exception ex)
                {
                    Logger.Log($"Failed to save recorded simulation: {ex.Message}");
                }
            }

            await MetricsExporter.Finalizer();

            // Console.ReadLine();

            // while (true)
            // {
            //     System.Threading.Thread.Sleep(10000);
            // }

            // Allow metrics server to be stopped gracefully
            MetricsExporter.Stop().Wait();

            Logger.Log("Simulation finished.");
        }
    }
}
