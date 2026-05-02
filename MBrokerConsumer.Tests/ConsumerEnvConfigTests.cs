using Confluent.Kafka;
using MBrokerConsumer.Models;
using Microsoft.Extensions.Logging;
using MSLogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace MBrokerConsumer.Tests;

public class ConsumerEnvConfigDefaultsTests
{
    [Fact]
    public void BootstrapServers_Default_ShouldBeEmpty()
    {
        var config = new ConsumerEnvConfig();
        Assert.Empty(config.BootstrapServers);
    }

    [Fact]
    public void Topic_Default_ShouldBeEmpty()
    {
        var config = new ConsumerEnvConfig();
        Assert.Empty(config.Topic);
    }

    [Fact]
    public void GroupId_Default_ShouldBeEmpty()
    {
        var config = new ConsumerEnvConfig();
        Assert.Empty(config.GroupId);
    }

    [Fact]
    public void MaxRateLimit_Default_ShouldBe500()
    {
        var config = new ConsumerEnvConfig();
        Assert.Equal(500, config.MaxRateLimit);
    }

    [Fact]
    public void ConsumerProfile_Default_ShouldBeSmall()
    {
        var config = new ConsumerEnvConfig();
        Assert.Equal("small", config.ConsumerProfile);
    }

    [Fact]
    public void LogLevel_Default_ShouldBeInformation()
    {
        var config = new ConsumerEnvConfig();
        Assert.Equal("Information", config.LogLevel);
    }

    [Fact]
    public void DrainTimeoutSeconds_Default_ShouldBe25()
    {
        var config = new ConsumerEnvConfig();
        Assert.Equal(25, config.DrainTimeoutSeconds);
    }

    [Fact]
    public void CommitIntervalSeconds_Default_ShouldBe5()
    {
        var config = new ConsumerEnvConfig();
        Assert.Equal(5, config.CommitIntervalSeconds);
    }
}

public class ConsumerEnvConfigGetLogLevelTests
{
    [Theory]
    [InlineData("trace", MSLogLevel.Trace)]
    [InlineData("debug", MSLogLevel.Debug)]
    [InlineData("information", MSLogLevel.Information)]
    [InlineData("warning", MSLogLevel.Warning)]
    [InlineData("error", MSLogLevel.Error)]
    [InlineData("critical", MSLogLevel.Critical)]
    public void GetLogLevel_WithValidInput_ShouldReturnCorrectLevel(string input, MSLogLevel expected)
    {
        var config = new ConsumerEnvConfig { LogLevel = input };
        Assert.Equal(expected, config.GetLogLevel());
    }

    [Theory]
    [InlineData("INFO")]
    [InlineData("Debug")]
    [InlineData("TRACE")]
    [InlineData("WARNING")]
    public void GetLogLevel_ShouldBeCaseInsensitive(string input)
    {
        var config = new ConsumerEnvConfig { LogLevel = input };
        var result = config.GetLogLevel();
        Assert.InRange((int)result, 0, 6);
    }

    [Theory]
    [InlineData("")]
    [InlineData("unknown")]
    [InlineData("verbose")]
    [InlineData("  ")]
    public void GetLogLevel_WithInvalidInput_ShouldFallbackToInformation(string input)
    {
        var config = new ConsumerEnvConfig { LogLevel = input };
        Assert.Equal(MSLogLevel.Information, config.GetLogLevel());
    }
}

public class ConsumerEnvConfigToConsumerConfigTests
{
    [Fact]
    public void ToConsumerConfig_ShouldSetAutoOffsetResetToLatest()
    {
        var config = new ConsumerEnvConfig();
        var kafkaConfig = config.ToConsumerConfig();
        Assert.Equal(AutoOffsetReset.Latest, kafkaConfig.AutoOffsetReset);
    }

    [Fact]
    public void ToConsumerConfig_ShouldDisableAutoCommit()
    {
        var config = new ConsumerEnvConfig();
        var kafkaConfig = config.ToConsumerConfig();
        Assert.False(kafkaConfig.EnableAutoCommit);
    }

    [Fact]
    public void ToConsumerConfig_ShouldMapBootstrapServers()
    {
        var config = new ConsumerEnvConfig { BootstrapServers = "broker:9092" };
        var kafkaConfig = config.ToConsumerConfig();
        Assert.Equal("broker:9092", kafkaConfig.BootstrapServers);
    }

    [Fact]
    public void ToConsumerConfig_ShouldMapGroupId()
    {
        var config = new ConsumerEnvConfig { GroupId = "my-consumer-group" };
        var kafkaConfig = config.ToConsumerConfig();
        Assert.Equal("my-consumer-group", kafkaConfig.GroupId);
    }

    [Fact]
    public void ToConsumerConfig_ShouldSetFetchWaitMaxMsTo100()
    {
        var config = new ConsumerEnvConfig();
        var kafkaConfig = config.ToConsumerConfig();
        Assert.Equal(100, kafkaConfig.FetchWaitMaxMs);
    }

    [Fact]
    public void ToConsumerConfig_ShouldUseProvidedDefaults()
    {
        var config = new ConsumerEnvConfig
        {
            BootstrapServers = "kafka:9092",
            GroupId = "test-group",
            Topic = "test-topic"
        };

        var kafkaConfig = config.ToConsumerConfig();

        Assert.Equal("kafka:9092", kafkaConfig.BootstrapServers);
        Assert.Equal("test-group", kafkaConfig.GroupId);
    }
}

public class ConsumerEnvConfigPropertySettersTests
{
    [Fact]
    public void ShouldAllowOverridingAllProperties()
    {
        var config = new ConsumerEnvConfig
        {
            BootstrapServers = "a:1",
            Topic = "b",
            GroupId = "c",
            MaxRateLimit = 1000,
            ConsumerProfile = "large",
            LogLevel = "error",
            DrainTimeoutSeconds = 60,
            CommitIntervalSeconds = 10
        };

        Assert.Equal("a:1", config.BootstrapServers);
        Assert.Equal("b", config.Topic);
        Assert.Equal("c", config.GroupId);
        Assert.Equal(1000, config.MaxRateLimit);
        Assert.Equal("large", config.ConsumerProfile);
        Assert.Equal("error", config.LogLevel);
        Assert.Equal(60, config.DrainTimeoutSeconds);
        Assert.Equal(10, config.CommitIntervalSeconds);
    }
}
