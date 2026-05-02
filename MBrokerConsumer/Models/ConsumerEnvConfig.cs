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

        public ConsumerConfig ToConsumerConfig()
        {
            return new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = GroupId,
                AutoOffsetReset = AutoOffsetReset.Latest,
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
