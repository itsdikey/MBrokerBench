using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using MSLogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace MBrokerConsumer.Models
{
    public class ConsumerEnvConfig
    {
        public string BootstrapServers { get; set; } = "";
        public string Topic { get; set; } = "";
        public string GroupId { get; set; } = "";
        public double MaxRateLimit { get; set; } = 500;
        public string ConsumerProfile { get; set; } = "small";
        public string LogLevel { get; set; } = "Information";
        public int DrainTimeoutSeconds { get; set; } = 25;
        public int CommitIntervalSeconds { get; set; } = 5;

        // Manual partition assignment (default off — normal Subscribe path used when false)
        public bool ManualPartitionAssignmentEnabled { get; set; } = false;
        public string AssignmentConfigMapPath { get; set; } = "/etc/mbroker-assignments/assignments.json";
        public int AssignmentPollIntervalSeconds { get; set; } = 15;
        public string PodName { get; set; } = "";
        // Maximum time to wait for this pod's entry to appear in the ConfigMap at startup
        // before exiting with a fatal error. Default 60s gives K8s enough time to start pods.
        public int AssignmentStartupTimeoutSeconds { get; set; } = 60;

        public ConsumerConfig ToConsumerConfig()
        {
            return new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                FetchWaitMaxMs = 100 // Reduce latency for rate limiting
            };
        }

        public LogLevel GetLogLevel()
        {
            return LogLevel.ToLower() switch
            {
                "trace" => MSLogLevel.Trace,
                "debug" => MSLogLevel.Debug,
                "information" => MSLogLevel.Information,
                "warning" => MSLogLevel.Warning,
                "error" => MSLogLevel.Error,
                "critical" => MSLogLevel.Critical,
                _ => MSLogLevel.Information
            };
        }
    }
}
