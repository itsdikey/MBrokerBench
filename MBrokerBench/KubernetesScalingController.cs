using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace MBrokerBench
{
    /// <summary>
    /// Real consumer pod entry derived from a running K8s pod.
    /// </summary>
    public sealed class RealConsumerPod
    {
        public string PodName { get; init; } = "";
        public string Profile { get; init; } = "";   // "small", "medium", "large"
    }

    public class KubernetesScalingController
    {
        private readonly IKubernetes _client;
        private readonly string _namespace;

        // Track last commanded replica count to avoid read-back-our-own-patch oscillation.
        // In real scaling mode, the control loop runs at ~1s wall-clock pace, but K8s pod startup
        // takes 10-30s. If we re-read Spec.Replicas from K8s API after every patch, we'd
        // immediately see our own change and oscillate. By tracking locally, we hold the
        // desired count steady until the strategy actually changes it.
        private readonly Dictionary<string, int> _lastAppliedCount = new();

        public KubernetesScalingController(string ns = "default")
        {
            var config = KubernetesClientConfiguration.BuildDefaultConfig();
            _client = new Kubernetes(config);
            _namespace = ns;
        }

        /// <summary>Returns the last commanded replica count (not from K8s API).</summary>
        public int GetLastAppliedCount(string deploymentName)
        {
            return _lastAppliedCount.GetValueOrDefault(deploymentName, 0);
        }

        /// <summary>Read actual replica count from K8s API (for initial sync).</summary>
        public async Task<int> GetKubernetesReplicaCountAsync(string deploymentName)
        {
            try
            {
                var deployment = await _client.AppsV1.ReadNamespacedDeploymentAsync(deploymentName, _namespace);
                int count = deployment.Spec.Replicas ?? 0;
                _lastAppliedCount[deploymentName] = count;
                return count;
            }
            catch (Exception ex)
            {
                Logger.Log($"Error reading deployment {deploymentName}: {ex.Message}", LogLevel.Error);
                return 0;
            }
        }

        public async Task SetReplicaCountAsync(string deploymentName, int replicas)
        {
            if (_lastAppliedCount.GetValueOrDefault(deploymentName) == replicas)
                return; // Already at desired count — no K8s API call needed

            try
            {
                Logger.Log($"Scaling deployment {deploymentName} to {replicas} replicas...");

                var patchStr = $"{{\"spec\": {{\"replicas\": {replicas}}}}}";
                var patch = new V1Patch(patchStr, V1Patch.PatchType.MergePatch);
                await _client.AppsV1.PatchNamespacedDeploymentAsync(patch, deploymentName, _namespace);

                _lastAppliedCount[deploymentName] = replicas;
                Logger.Log($"Successfully patched deployment {deploymentName}.");
            }
            catch (Exception ex)
            {
                Logger.Log($"Error scaling deployment {deploymentName}: {ex.Message}", LogLevel.Error);
            }
        }

        // -------------------------------------------------------------------------
        // ConfigMap helpers for manual partition assignment
        // -------------------------------------------------------------------------

        private const string AssignmentDataKey = "assignments.json";

        /// <summary>
        /// Ensures the assignment ConfigMap exists, creating it with empty data if absent.
        /// Idempotent — safe to call every tick.
        /// </summary>
        public async Task EnsureConfigMapExistsAsync(string configMapName)
        {
            try
            {
                await _client.CoreV1.ReadNamespacedConfigMapAsync(configMapName, _namespace);
                return; // Already exists
            }
            catch (Exception)
            {
                // Not found — create it
            }

            try
            {
                var cm = new V1ConfigMap
                {
                    Metadata = new V1ObjectMeta
                    {
                        Name = configMapName,
                        NamespaceProperty = _namespace
                    },
                    Data = new Dictionary<string, string>
                    {
                        { AssignmentDataKey, "{}" }
                    }
                };
                await _client.CoreV1.CreateNamespacedConfigMapAsync(cm, _namespace);
                Logger.Log($"[ConfigMap] Created ConfigMap '{configMapName}'.", LogLevel.Information);
            }
            catch (Exception ex)
            {
                Logger.Log($"[ConfigMap] Failed to create ConfigMap '{configMapName}': {ex.Message}", LogLevel.Warning);
            }
        }

        /// <summary>
        /// Lists non-Terminated consumer pods (Pending, Running, or Unknown) grouped by profile
        /// label, ordered by pod name. Excludes pods in Succeeded/Failed phases so that pods
        /// completing or failing do not hold slots in the assignment map.
        /// </summary>
        public async Task<IReadOnlyList<RealConsumerPod>> ListReadyConsumerPodsAsync()
        {
            var allPods = new List<RealConsumerPod>();
            var profileLabels = new[] { "small", "medium", "large" };
            var terminalPhases = new HashSet<string> { "Succeeded", "Failed" };

            foreach (var profile in profileLabels)
            {
                try
                {
                    // Include all non-Terminated pods so that pods still Starting/Pending
                    // receive partition assignments early, preventing startup deadlock.
                    var pods = await _client.CoreV1.ListNamespacedPodAsync(
                        _namespace,
                        labelSelector: $"app=mbroker-consumer,profile={profile}");

                    foreach (var pod in pods.Items)
                    {
                        if (terminalPhases.Contains(pod.Status.Phase))
                            continue;

                        allPods.Add(new RealConsumerPod
                        {
                            PodName = pod.Metadata.Name,
                            Profile = profile
                        });
                    }
                }
                catch (Exception ex)
                {
                    Logger.Log($"[ConfigMap] Error listing pods for profile '{profile}': {ex.Message}", LogLevel.Warning);
                }
            }

            return allPods
                .OrderBy(p => p.Profile)
                .ThenBy(p => p.PodName)
                .ToList();
        }

        /// <summary>
        /// Publishes a JSON mapping of pod name -> partition IDs into the ConfigMap.
        /// Payload format: { "pod-a": [0,3,5], "pod-b": [1,2,4] }
        /// </summary>
        public async Task PublishPartitionAssignmentsAsync(
            string configMapName,
            Dictionary<string, int[]> assignments)
        {
            var json = JsonSerializer.Serialize(assignments);

            try
            {
                var patchStr = JsonSerializer.Serialize(new
                {
                    data = new Dictionary<string, string>
                    {
                        { AssignmentDataKey, json }
                    }
                });

                var patch = new V1Patch(patchStr, V1Patch.PatchType.MergePatch);
                await _client.CoreV1.PatchNamespacedConfigMapAsync(
                    new V1Patch(patchStr, V1Patch.PatchType.MergePatch),
                    configMapName,
                    _namespace);

                Logger.Log($"[ConfigMap] Published assignments for {assignments.Count} pods to ConfigMap '{configMapName}'.", LogLevel.Information);
            }
            catch (Exception ex)
            {
                Logger.Log($"[ConfigMap] Failed to publish assignments to ConfigMap '{configMapName}': {ex.Message}", LogLevel.Warning);
            }
        }
    }
}
