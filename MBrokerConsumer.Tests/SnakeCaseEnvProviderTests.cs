using MBrokerConsumer.Configuration;
using MBrokerConsumer.Models;
using Microsoft.Extensions.Configuration;

namespace MBrokerConsumer.Tests;

/// <summary>
/// Tests for SnakeCaseEnvProvider — verifies that environment variables
/// with underscore naming are correctly stripped and bound to ConsumerEnvConfig.
/// </summary>
[Collection("EnvVarTests")]
public class SnakeCaseEnvProviderBindingTests : IDisposable
{
    private readonly Dictionary<string, string?> _originalEnv = new();

    public SnakeCaseEnvProviderBindingTests()
    {
        // Snapshot env vars we're about to change
        foreach (var key in new[] { "BOOTSTRAP_SERVERS", "TOPIC", "GROUP_ID", "MAX_RATE_LIMIT", "CONSUMER_PROFILE", "LOG_LEVEL", "DRAIN_TIMEOUT_SECONDS", "COMMIT_INTERVAL_SECONDS" })
        {
            _originalEnv[key] = Environment.GetEnvironmentVariable(key);
        }
    }

    public void Dispose()
    {
        // Restore original env vars
        foreach (var (key, value) in _originalEnv)
        {
            if (value == null)
                Environment.SetEnvironmentVariable(key, null);
            else
                Environment.SetEnvironmentVariable(key, value);
        }
    }

    [Fact]
    public void ShouldBindAllRequiredEnvVars()
    {
        Environment.SetEnvironmentVariable("BOOTSTRAP_SERVERS", "kafka-cluster:9092");
        Environment.SetEnvironmentVariable("TOPIC", "benchmark-topic");
        Environment.SetEnvironmentVariable("GROUP_ID", "benchmark-group");

        var config = new ConfigurationBuilder()
            .AddSnakeCaseEnvironmentVariables()
            .Build();

        var envConfig = config.Get<ConsumerEnvConfig>();

        Assert.NotNull(envConfig);
        Assert.Equal("kafka-cluster:9092", envConfig!.BootstrapServers);
        Assert.Equal("benchmark-topic", envConfig.Topic);
        Assert.Equal("benchmark-group", envConfig.GroupId);
    }

    [Fact]
    public void ShouldBindOptionalEnvVars()
    {
        Environment.SetEnvironmentVariable("MAX_RATE_LIMIT", "1000");
        Environment.SetEnvironmentVariable("CONSUMER_PROFILE", "large");
        Environment.SetEnvironmentVariable("LOG_LEVEL", "debug");
        Environment.SetEnvironmentVariable("DRAIN_TIMEOUT_SECONDS", "45");
        Environment.SetEnvironmentVariable("COMMIT_INTERVAL_SECONDS", "10");

        var config = new ConfigurationBuilder()
            .AddSnakeCaseEnvironmentVariables()
            .Build();

        var envConfig = config.Get<ConsumerEnvConfig>();

        Assert.NotNull(envConfig);
        Assert.Equal(1000, envConfig!.MaxRateLimit);
        Assert.Equal("large", envConfig.ConsumerProfile);
        Assert.Equal("debug", envConfig.LogLevel);
        Assert.Equal(45, envConfig.DrainTimeoutSeconds);
        Assert.Equal(10, envConfig.CommitIntervalSeconds);
    }

    [Fact]
    public void ShouldFallBackToDefaultsWhenEnvVarsNotSet()
    {
        // Don't set any env vars — should use ConsumerEnvConfig defaults
        var config = new ConfigurationBuilder()
            .AddSnakeCaseEnvironmentVariables()
            .Build();

        var envConfig = config.Get<ConsumerEnvConfig>() ?? new ConsumerEnvConfig();

        // Get<T> uses the parameterless constructor, so class-level initializers run.
        // Without matching env vars, all properties stay at their class defaults.
        Assert.Equal(500, envConfig.MaxRateLimit);
        Assert.Equal("small", envConfig.ConsumerProfile);
        Assert.Equal("Information", envConfig.LogLevel);
        Assert.Equal(25, envConfig.DrainTimeoutSeconds);
        Assert.Equal(5, envConfig.CommitIntervalSeconds);
    }

    [Fact]
    public void ShouldBindMixedEnvVars_SomeSetSomeNot()
    {
        Environment.SetEnvironmentVariable("BOOTSTRAP_SERVERS", "broker:9092");
        Environment.SetEnvironmentVariable("LOG_LEVEL", "error");

        var config = new ConfigurationBuilder()
            .AddSnakeCaseEnvironmentVariables()
            .Build();

        var envConfig = config.Get<ConsumerEnvConfig>() ?? new ConsumerEnvConfig();

        Assert.Equal("broker:9092", envConfig.BootstrapServers);
        Assert.Equal("error", envConfig.LogLevel);
        // Unset fields keep class-level defaults
        Assert.Equal(500, envConfig.MaxRateLimit);
        Assert.Equal("small", envConfig.ConsumerProfile);
    }

    [Fact]
    public void ShouldHandleEmptyEnvVarValues()
    {
        Environment.SetEnvironmentVariable("BOOTSTRAP_SERVERS", "");
        Environment.SetEnvironmentVariable("TOPIC", "my-topic");
        Environment.SetEnvironmentVariable("GROUP_ID", "my-group");

        var config = new ConfigurationBuilder()
            .AddSnakeCaseEnvironmentVariables()
            .Build();

        var envConfig = config.Get<ConsumerEnvConfig>();

        Assert.NotNull(envConfig);
        Assert.Equal("", envConfig!.BootstrapServers);
        Assert.Equal("my-topic", envConfig.Topic);
        Assert.Equal("my-group", envConfig.GroupId);
    }

    [Fact]
    public void ShouldBeCaseInsensitiveInKeyNormalization()
    {
        Environment.SetEnvironmentVariable("bootstrap_servers", "case-test:9092");
        Environment.SetEnvironmentVariable("TOPIC", "case-topic");
        Environment.SetEnvironmentVariable("group_id", "case-group");

        var config = new ConfigurationBuilder()
            .AddSnakeCaseEnvironmentVariables()
            .Build();

        var envConfig = config.Get<ConsumerEnvConfig>();

        Assert.NotNull(envConfig);
        Assert.Equal("case-test:9092", envConfig!.BootstrapServers);
        Assert.Equal("case-topic", envConfig.Topic);
        Assert.Equal("case-group", envConfig.GroupId);
    }
}
