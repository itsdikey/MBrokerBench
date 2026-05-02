using Microsoft.Extensions.Configuration;

namespace MBrokerConsumer.Configuration
{
    internal class SnakeCaseEnvProvider : ConfigurationProvider
    {
        public override void Load()
        {
            var envVars = Environment.GetEnvironmentVariables();

            foreach (var key in envVars.Keys)
            {
                var value = envVars[key]?.ToString() ?? string.Empty;
                var snakeCaseKey = (key.ToString() ?? string.Empty);

                var normalizedKey = snakeCaseKey.Replace("_", "").ToLower();

                Data[normalizedKey] = value;
            }


        }
    }
}
