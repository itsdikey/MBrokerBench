using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;

namespace MBrokerBench
{
    public class KubernetesScalingController
    {
        private readonly IKubernetes _client;
        private readonly string _namespace;
        
        // Track last commanded replica count to avoid read-back-our-own-patch oscillation.
        // In real scaling mode, Tick() runs at ~1s wall-clock pace, but K8s pod startup
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
    }
}
