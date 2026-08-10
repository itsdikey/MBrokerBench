#define PRETTY
using ConsolePlot;
using ConsolePlot.Drawing.Tools;
using MBrokerBench.Components;
using MBrokerBench.DataProviders;
using MBrokerBench.Models;
using MBrokerBench.Strategies;
using Microsoft.Extensions.Logging;
using System.Text;
using Terminal.Gui;

namespace MBrokerBench
{
    // Centralized logger to enable/disable console output from one place.
    public static class Logger
    {
        // Set to false to silence all Logger.Log calls.
        public static bool Enabled { get; set; } = true;

        public static List<string> Logs { get; } = new List<string>();
        public static Action<string>? OnLog { get; set; }

        public static void Log(string message, LogLevel logLevel = LogLevel.Information)
        {
            if (!Enabled)
            {
                return;
            }

#if PRETTY
            var logLine = $"[{DateTime.Now:HH:mm:ss}] {message}";
            lock (Logs)
            {
                Logs.Add(logLine);
                if (Logs.Count > 100) Logs.RemoveAt(0);
            }
            OnLog?.Invoke(logLine);
#else
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
#endif
        }

        public static void Log(string format, LogLevel logLevel = LogLevel.Information, params object[] args)
        {
            Log(string.Format(format, args), logLevel);
        }
    }

    public class BrokerSimulator
    {
        private const double TimeStepSeconds = 1;
        private static bool _headless;

        public enum DebugMode
        {
            Console,
            Plot
        }

        public const DebugMode Mode = DebugMode.Console;

#if PRETTY
        private static Window? _logWin;
        private static ListView? _logListView;
        private static Window? _statusWin;
        private static Label? _statusLabel;
        private static Window? _partitionWin;
        private static ListView? _partitionListView;
        private static Window? _consumerWin;
        private static ListView? _consumerListView;
#endif

        public static async Task Main()
        {
            // Headless mode: skip Terminal.Gui TUI, use console logging
            _headless = Environment.GetEnvironmentVariable("NO_TUI") == "true" || Environment.GetEnvironmentVariable("PRODUCER_MODE") == "true";
            bool headless = _headless;

            if (!headless)
            {
#if PRETTY
                Application.Init();
                var top = Application.Top;

                var win = new Window("MBrokerBench - Kafka Autoscaling Simulation")
                {
                    X = 0,
                    Y = 0,
                    Width = Dim.Fill(),
                    Height = Dim.Fill()
                };

                _statusWin = new Window("Status")
                {
                    X = 0,
                    Y = 0,
                    Width = Dim.Percent(30),
                    Height = 10
                };
                _statusLabel = new Label("Initializing...") { X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill() };
                _statusWin.Add(_statusLabel);

                _partitionWin = new Window("Partitions")
                {
                    X = Pos.Right(_statusWin),
                    Y = 0,
                    Width = Dim.Percent(40),
                    Height = 10
                };
                _partitionListView = new ListView(new List<string>()) { X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill() };
                _partitionWin.Add(_partitionListView);

                _consumerWin = new Window("Consumers")
                {
                    X = Pos.Right(_partitionWin),
                    Y = 0,
                    Width = Dim.Fill(),
                    Height = 10
                };
                _consumerListView = new ListView(new List<string>()) { X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill() };
                _consumerWin.Add(_consumerListView);

                _logWin = new Window("Logs")
                {
                    X = 0,
                    Y = 10,
                    Width = Dim.Fill(),
                    Height = Dim.Fill()
                };
                _logListView = new ListView(Logger.Logs) { X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill() };
                _logWin.Add(_logListView);

                win.Add(_statusWin, _partitionWin, _consumerWin, _logWin);
                top.Add(win);

                top.KeyPress += (e) =>
                {
                    if (e.KeyEvent.Key == Key.Q || e.KeyEvent.Key == (Key)'q')
                    {
                        Application.RequestStop();
                    }
                };

                Logger.OnLog = (msg) =>
                {
                    Application.MainLoop.Invoke(() =>
                    {
                        try
                        {
                            //_logListView.SetSource(Logger.Logs.ToList());
                            //_logListView.SelectedItem = Logger.Logs.Count - 1;
                            //_logListView.TopItem = Math.Max(0, Logger.Logs.Count - 1);
                        }
                        catch
                        {
                            // Ignore if logs are empty or index out of range
                        }
                    });
                };

                var simTask = Task.Run(async () => await RunSimulation());

                Application.Run();
                return;
#endif
            }
            // Fall through to headless mode (no TUI) — works in background
            Logger.OnLog = (msg) => Console.WriteLine(msg);
            await RunSimulation();
        }

        private static async Task RunSimulation()
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
                case "KafkaDefault":
                    assignmentStrategy = new KafkaDefaultAssignment();
                    shortCodeAlgorithm = "kafka_default";
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
            if (System.Environment.GetEnvironmentVariable("PRODUCER_MODE") != "true")
            {
                MetricsExporter.Init(1234, strategyEnv, runIdEnv);
            }

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
                string scenarioEnv = System.Environment.GetEnvironmentVariable("SCENARIO") ?? "Uniform";
                IDataProvider baseProvider = dataProviderEnv switch
                {
                    "Poisson" => new PoissonDataProvider(scenarioEnv),
                    "Sinusoid" => new SinusoidDataProvider(scenarioEnv),
                    "Kafka" => new KafkaDataProvider(
                        System.Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092",
                        System.Environment.GetEnvironmentVariable("PROMETHEUS_URL") ?? "http://localhost:9090",
                        System.Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "test-1",
                        System.Environment.GetEnvironmentVariable("KAFKA_GROUP") ?? "test-group"
                    ),
                    "NYTaxi" or _ => new NYTaxiDataProvider(scenarioEnv)
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
            string maxStepsEnv = System.Environment.GetEnvironmentVariable("MAX_STEPS");
            int maxRuntime = !string.IsNullOrEmpty(maxStepsEnv) ? int.Parse(maxStepsEnv) : (provider.MaxRuntimeSteps > 0 ? provider.MaxRuntimeSteps : 600);

            if (System.Environment.GetEnvironmentVariable("PRODUCER_MODE") == "true")
            {
                await RunWorkloadProducer(provider, maxRuntime);
                return;
            }

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

            #region Real Scaling Init
            bool isRealScaling = System.Environment.GetEnvironmentVariable("SCALING_MODE") == "Real";
            KubernetesScalingController? k8sController = null;
            var deploymentMap = new Dictionary<string, string>
            {
                { "Small", "mbroker-consumer-small" },
                { "Medium", "mbroker-consumer-medium" },
                { "Large", "mbroker-consumer-large" }
            };

            // Feature flag: manual partition assignment via ConfigMap (default false — group-based Subscribe remains)
            bool manualPartitionAssignmentEnabled =
                System.Environment.GetEnvironmentVariable("MANUAL_PARTITION_ASSIGNMENT_ENABLED") == "true";
            string assignmentConfigMapName =
                System.Environment.GetEnvironmentVariable("ASSIGNMENT_CONFIG_MAP_NAME") ?? "mbroker-partition-assignments";

            if (isRealScaling)
            {
                Logger.Log("REAL SCALING MODE ENABLED", LogLevel.Warning);
                if (manualPartitionAssignmentEnabled)
                {
                    Logger.Log($"MANUAL PARTITION ASSIGNMENT ENABLED (ConfigMap: {assignmentConfigMapName})", LogLevel.Warning);
                }
                k8sController = new KubernetesScalingController();

                // Initial sync: read actual K8s deployment state (not our local tracking)
                foreach (var profile in ConsumerProfiles.AllProfiles)
                {
                    if (deploymentMap.TryGetValue(profile.Name, out var deploymentName))
                    {
                        int realCount = k8sController.GetKubernetesReplicaCountAsync(deploymentName).GetAwaiter().GetResult();
                        Logger.Log($"[Real Scaling] Initial {profile.Name} replica count: {realCount}");
                    }
                }

                // Ensure the assignment ConfigMap exists before the first tick.
                if (manualPartitionAssignmentEnabled)
                {
                    k8sController.EnsureConfigMapExistsAsync(assignmentConfigMapName).GetAwaiter().GetResult();
                }
            }

            // One-time flag: ensures the first real-mode tick gets partition assignments
            // using fresh Kafka lag/rate data from provider.Process, avoiding a 30-second
            // warm-up gap where partitions show as unassigned in metrics/logs/CSV.
            bool realScalingInitialRebalanceDone = false;
            #endregion

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
                    // ... (rest of plot code)
                }
            

                Logger.Log($"\n--- SIMULATION STEP {step} ---");

                if (isRealScaling && k8sController != null)
                {
                    // 1. Sync Virtual Fleet with locally-tracked K8s Deployment State.
                    //    Uses GetLastAppliedCount (not K8s API) to avoid reading back our own
                    //    patches from the previous tick. K8s pods take 10-30s to start, but
                    //    Spec.Replicas returns instantly — re-reading would cause oscillation.
                    foreach (var profile in ConsumerProfiles.AllProfiles)
                    {
                        if (deploymentMap.TryGetValue(profile.Name, out var deploymentName))
                        {
                            int realCount = k8sController.GetLastAppliedCount(deploymentName);
                            group.SyncRealConsumers(profile.Name, realCount);
                        }
                    }
                }

                // Let provider process rate changes / events for this timestep
                provider.Process(group.AllPartitions, step);

                // One-time initial rebalance using fresh Kafka lag/rate data (not stale pre-Process state).
                // Skipping this would leave partitions unassigned for the first ~30s until the
                // periodic Autoscale() fires, polluting the first metrics/log/CSV row.
                if (isRealScaling && !realScalingInitialRebalanceDone && group.Consumers.Count > 0)
                {
                    group.Rebalance();
                    realScalingInitialRebalanceDone = true;
                }

                long stepConsumed;
                if (isRealScaling)
                {
                    group.TickRealControlOnly(TimeStepSeconds);
                    stepConsumed = 0;
                }
                else
                {
                    stepConsumed = group.TickVirtual(TimeStepSeconds);
                }

                // 1b. Publish manual partition assignments via ConfigMap if the feature is enabled.
                //     Mapping strategy: group synthetic consumers by profile, sort both lists,
                //     zip pod-to-consumer by profile, publish empty arrays for extra ready pods.
                if (isRealScaling && manualPartitionAssignmentEnabled && k8sController != null)
                {
                    try
                    {
                        var readyPods = await k8sController.ListReadyConsumerPodsAsync();

                        // Build profile -> sorted pod name list
                        var podsByProfile = readyPods
                            .GroupBy(p => p.Profile.ToLower())
                            .ToDictionary(g => g.Key, g => g.Select(p => p.PodName).OrderBy(n => n).ToList());

                        // Build profile -> sorted active synthetic consumers
                        var consumersByProfile = group.ActiveConsumers
                            .GroupBy(c => c.ConsumerProfile.Name.ToLower())
                            .ToDictionary(g => g.Key, g => g.ToList());

                        var allProfiles = podsByProfile.Keys.Union(consumersByProfile.Keys).Distinct();
                        var assignmentMap = new Dictionary<string, int[]>();

                        foreach (var profile in allProfiles)
                        {
                            podsByProfile.TryGetValue(profile, out var podList);
                            consumersByProfile.TryGetValue(profile, out var consumerList);
                            var pods = podList ?? new List<string>();
                            var consumers = consumerList ?? new List<Consumer>();

                            var sortedConsumers = consumers.OrderBy(c => c.Id).ToList();

                            // Zip pods to consumers 1-to-1; extra pods get empty arrays
                            for (int i = 0; i < pods.Count; i++)
                            {
                                var pod = pods[i];
                                int[] assignedPartitionIds;
                                if (i < sortedConsumers.Count)
                                {
                                    assignedPartitionIds = sortedConsumers[i].AssignedPartitions
                                        .Select(p => int.Parse(p.Id))
                                        .OrderBy(id => id)
                                        .ToArray();
                                }
                                else
                                {
                                    assignedPartitionIds = Array.Empty<int>();
                                }
                                assignmentMap[pod] = assignedPartitionIds;
                            }
                        }

                        await k8sController.PublishPartitionAssignmentsAsync(assignmentConfigMapName, assignmentMap);
                    }
                    catch (Exception ex)
                    {
                        Logger.Log($"[ConfigMap] Assignment publication failed: {ex.Message}", LogLevel.Warning);
                    }
                }

                if (isRealScaling && k8sController != null)
                {
                    // 2. Apply any strategy-driven scaling decisions back to K8s
                    foreach (var profile in ConsumerProfiles.AllProfiles)
                    {
                        if (deploymentMap.TryGetValue(profile.Name, out var deploymentName))
                        {
                            int desiredCount = group.Consumers.Count(c => c.ConsumerProfile.Name == profile.Name);
                            int realCount = k8sController.GetLastAppliedCount(deploymentName);

                            if (desiredCount != realCount)
                            {
                                Logger.Log($"[Real Scaling] Reconciling {profile.Name}: Desired={desiredCount}, Actual={realCount}");
                                k8sController.SetReplicaCountAsync(deploymentName, desiredCount).GetAwaiter().GetResult();
                            }
                        }
                    }
                }

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

                // In real scaling mode, derive consumption rate from Kafka committed-offset deltas
                // observed by the controller. This reflects actual consumer progress, not simulated.
                double consumptionRate;
                if (isRealScaling)
                {
                    consumptionRate = provider is KafkaDataProvider kdp ? kdp.ObservedConsumptionRate : 0;
                }
                else
                {
                    consumptionRate = stepConsumed / TimeStepSeconds;
                }

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

                var consumptionRateText = isRealScaling
                    ? $"{consumptionRate:F1} msgs/s (Kafka committed offsets)"
                    : $"{consumptionRate:F1} msgs/s (simulated)";
                Logger.Log($"Total System Lag: {totalLag} messages. Total Production Rate: {totalProductionRate:F1} msgs/s. Total Consumption Rate: {consumptionRateText}. Average Production Rate: {averageProductionRate:F1} msgs/s");
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
                MetricsExporter.SetTotalConsumptionRate(consumptionRate);

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

                // CSV export per timestep — use the already-computed consumptionRate (real or simulated)
                double currentConsumptionRate = consumptionRate;

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

                if (!_headless)
                {
#if PRETTY
                Application.MainLoop.Invoke(() =>
                {
                    _statusLabel.Text = $"Step: {step}/{maxRuntime}\n" +
                                        $"Total Lag: {totalLag}\n" +
                                        $"Prod Rate: {totalProductionRate:F1}\n" +
                                        $"Cons Rate: {consumptionRate:F1}\n" +
                                        $"Avg Prod:  {averageProductionRate:F1}\n" +
                                        $"Max Lat:   {maxLagTime:F2}s\n" +
                                        $"SLA:       {group.LatencySLASeconds}s\n" +
                                        $"Cost:      ${group.TotalCostPerSecond:F2}\n" +
                                        $"Consumers: {group.Consumers.Count}";

                    var pList = group.AllPartitions.Select(p => 
                        $"P{p.Id}: L={p.CurrentLag,6} R={p.ProductionRate,5:F0} {(p.AssignedConsumer != null ? "Assigned" : "UNASSIGNED")}"
                    ).ToList();
                    _partitionListView.SetSource(pList);

                    var cList = group.Consumers.Select(c => 
                        $"{c.Id} [{c.ConsumerProfile.ShortCode}] {c.State} L={c.GetCurrentTotalLag(0),6} U={(c.GetCurrentWorkloadRate()/c.MaxCapacity*100),3:F0}% P={c.AssignedPartitions.Count}"
                    ).ToList();
                    _consumerListView.SetSource(cList);
                });
#endif
                }

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

                // In real scaling mode, pace the loop to match wall-clock time so K8s
                // pod startup, health probes, and readiness gates have time to react.
                if (isRealScaling)
                {
                    Thread.Sleep(1000);
                }
                else
                {
                    Thread.Sleep(1);
                }
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

            // Allow metrics server to be stopped gracefully
            MetricsExporter.Stop().Wait();

            Logger.Log("Simulation finished. Press 'Q' to exit.");

#if PRETTY
            // Wait for user to read final results in PRETTY mode
            // (The simulation loop has finished, but the UI is still running)
#else
            // Console.ReadLine();
#endif
        }

        private static async Task RunWorkloadProducer(IDataProvider provider, int maxRuntime)
        {
            string bootstrapServers = System.Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";
            string topic = System.Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "test-1";

            Console.WriteLine($"[WORKLOAD PRODUCER] Starting in producer mode against {bootstrapServers}...");
            Console.WriteLine($"[WORKLOAD PRODUCER] Using Data Provider: {provider.GetType().Name}");
            Console.WriteLine($"[WORKLOAD PRODUCER] Topic: {topic}");

            var config = new Confluent.Kafka.ProducerConfig { BootstrapServers = bootstrapServers };
            using var producer = new Confluent.Kafka.ProducerBuilder<Confluent.Kafka.Null, string>(config).Build();

            var partitions = provider.InitializePartitions();

            for (int step = 1; step <= maxRuntime; step++)
            {
                int virtualStep = step;
                if (maxRuntime < provider.MaxRuntimeSteps && provider.MaxRuntimeSteps > 0)
                {
                    virtualStep = (int)Math.Clamp(step * ((double)provider.MaxRuntimeSteps / maxRuntime), 1, provider.MaxRuntimeSteps);
                }
                provider.Process(partitions, virtualStep);
                
                int totalMessagesSent = 0;
                var startTime = DateTime.UtcNow;

                foreach (var partition in partitions)
                {
                    int partitionId = int.Parse(partition.Id);
                    int rate = (int)partition.ProductionRate; // messages to send in this second

                    for (int i = 0; i < rate; i++)
                    {
                        var message = new Confluent.Kafka.Message<Confluent.Kafka.Null, string>
                        {
                            Value = $"step-{step}-msg-{i}-timestamp-{DateTime.UtcNow.Ticks}"
                        };
                        
                        producer.Produce(new Confluent.Kafka.TopicPartition(topic, new Confluent.Kafka.Partition(partitionId)), message, err => {
                            if (err.Error.IsError)
                            {
                                Console.WriteLine($"[ERROR] Failed to produce to partition {partitionId}: {err.Error.Reason}");
                            }
                        });
                        totalMessagesSent++;
                    }
                }

                producer.Flush(TimeSpan.FromMilliseconds(500));

                var elapsed = DateTime.UtcNow - startTime;
                Console.WriteLine($"[Step {step}/{maxRuntime}] Produced {totalMessagesSent} messages across {partitions.Count} partitions. Elapsed: {elapsed.TotalMilliseconds:F1}ms");

                double sleepTimeMs = 1000.0 - elapsed.TotalMilliseconds;
                if (sleepTimeMs > 0)
                {
                    await Task.Delay((int)sleepTimeMs);
                }
            }

            Console.WriteLine("[WORKLOAD PRODUCER] Completed.");
        }
    }
}
