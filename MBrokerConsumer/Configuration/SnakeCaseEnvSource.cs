using Microsoft.Extensions.Configuration;

namespace MBrokerConsumer.Configuration
{
    internal class SnakeCaseEnvSource : IConfigurationSource
    {
        public IConfigurationProvider Build(IConfigurationBuilder builder)
        {
            return new SnakeCaseEnvProvider();
        }
    }
}
