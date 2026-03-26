using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace MBrokerConsumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";
            var topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "test-1";
            var groupId = Environment.GetEnvironmentVariable("KAFKA_GROUP") ?? "test-group";
            var maxRateLimitStr = Environment.GetEnvironmentVariable("MAX_RATE_LIMIT") ?? "100";

            if (!double.TryParse(maxRateLimitStr, out double maxRateLimit))
            {
                maxRateLimit = 100;
            }

            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
            });
            var logger = loggerFactory.CreateLogger<Program>();

            logger.LogInformation("MBrokerConsumer starting...");
            logger.LogInformation($"Bootstrap Servers: {bootstrapServers}");
            logger.LogInformation($"Topic: {topic}");
            logger.LogInformation($"Group ID: {groupId}");
            logger.LogInformation($"Max Rate Limit: {maxRateLimit} msgs/s");

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                FetchWaitMaxMs = 100 // Reduce latency for rate limiting
            };

            using var consumer = new ConsumerBuilder<Ignore, byte[]>(config)
                .SetErrorHandler((_, e) => logger.LogError($"Kafka Error: {e.Reason}"))
                .Build();

            consumer.Subscribe(topic);

            var stopwatch = Stopwatch.StartNew();
            long messageCount = 0;
            long totalMessages = 0;

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            logger.LogInformation("Consumption loop started.");

            try
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));

                        if (consumeResult != null)
                        {
                            messageCount++;
                            totalMessages++;

                            // Simple rate limiting: if we exceed the rate, sleep until the next window
                            if (messageCount >= maxRateLimit)
                            {
                                var elapsed = stopwatch.Elapsed.TotalSeconds;
                                if (elapsed < 1.0)
                                {
                                    var delay = (int)((1.0 - elapsed) * 1000);
                                    if (delay > 0)
                                    {
                                        await Task.Delay(delay, cts.Token);
                                    }
                                }
                                
                                logger.LogInformation($"Consumed {messageCount} msgs in {stopwatch.Elapsed.TotalSeconds:F2}s. Total: {totalMessages}");
                                
                                messageCount = 0;
                                stopwatch.Restart();
                            }
                        }
                        else
                        {
                            // If no messages, still check the rate limiting window
                            if (stopwatch.Elapsed.TotalSeconds >= 1.0)
                            {
                                if (messageCount > 0)
                                {
                                    logger.LogInformation($"Consumed {messageCount} msgs in {stopwatch.Elapsed.TotalSeconds:F2}s. Total: {totalMessages}");
                                }
                                messageCount = 0;
                                stopwatch.Restart();
                            }
                        }
                    }
                    catch (ConsumeException e)
                    {
                        logger.LogError($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("Closing consumer...");
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
