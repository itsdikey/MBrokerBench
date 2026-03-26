using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;

namespace MBrokerBench
{
    public class KubernetesScalingController
    {
        private readonly IKubernetes _client;
        private readonly string _namespace;

        public KubernetesScalingController(string ns = "default")
        {
            var config = KubernetesClientConfiguration.BuildDefaultConfig();
            _client = new Kubernetes(config);
            _namespace = ns;
        }

        public async Task<int> GetReplicaCountAsync(string deploymentName)
        {
            try
            {
                var deployment = await _client.AppsV1.ReadNamespacedDeploymentAsync(deploymentName, _namespace);
                return deployment.Spec.Replicas ?? 0;
            }
            catch (Exception ex)
            {
                Logger.Log($"Error reading deployment {deploymentName}: {ex.Message}", LogLevel.Error);
                return 0;
            }
        }

        public async Task SetReplicaCountAsync(string deploymentName, int replicas)
        {
            try
            {
                Logger.Log($"Scaling deployment {deploymentName} to {replicas} replicas...");
                
                var patchStr = $"{{\"spec\": {{\"replicas\": {replicas}}}}}";
                var patch = new V1Patch(patchStr, V1Patch.PatchType.MergePatch);
                await _client.AppsV1.PatchNamespacedDeploymentAsync(patch, deploymentName, _namespace);
                
                Logger.Log($"Successfully patched deployment {deploymentName}.");
            }
            catch (Exception ex)
            {
                Logger.Log($"Error scaling deployment {deploymentName}: {ex.Message}", LogLevel.Error);
            }
        }
    }
}
