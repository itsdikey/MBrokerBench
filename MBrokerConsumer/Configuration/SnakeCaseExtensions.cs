using Microsoft.Extensions.Configuration;

namespace MBrokerConsumer.Configuration
{
    public static class SnakeCaseExtensions
    {
        public static IConfigurationBuilder AddSnakeCaseEnvironmentVariables(this IConfigurationBuilder builder)
        {
            return builder.Add(new SnakeCaseEnvSource());
        }
    }
}
