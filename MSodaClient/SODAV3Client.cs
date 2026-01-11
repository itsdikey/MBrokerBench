using System.Diagnostics;

namespace MSodaClient
{

    public record SODAV3ClientConfig
    {
        public string? BaseUrl { get; set; }
        public int Timeout { get; set; }
        public Dictionary<string,string>? Header { get; set; }
        public string? XAppToken { get; set; }
        public (string AppKey, string AppSecret)? AppCredentials { get; set; }
        public string? ResourceIdentifier { get; set; }
    }

    public sealed class SODAV3Client
    {
        private readonly HttpClient _httpClient;
        private readonly SODAV3ClientConfig? _config;

        public SODAV3Client(SODAV3ClientConfig? config = null)
        {
            _httpClient = new HttpClient();
            _config = config;
            
            if (config != null)
            {
                if (!string.IsNullOrEmpty(config.BaseUrl))
                {
                    _httpClient.BaseAddress = new Uri(config.BaseUrl);
                }

                if (config.Timeout > 0)
                {
                    _httpClient.Timeout = TimeSpan.FromSeconds(config.Timeout);
                }

                if (config.Header is not null)
                {
                    foreach (var header in config.Header)
                    {
                        _httpClient.DefaultRequestHeaders.Add(header.Key, header.Value);
                    }
                }
                if (!string.IsNullOrEmpty(config.XAppToken))
                {
                    _httpClient.DefaultRequestHeaders.Add("X-App-Token", config.XAppToken);
                }

                if(config.AppCredentials is not null)
                {
                    var byteArray = System.Text.Encoding.ASCII.GetBytes($"{config.AppCredentials.Value.AppKey}:{config.AppCredentials.Value.AppSecret}");
                    _httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
                }
            }
        }

        /// <summary>
        /// Query the Socrata v3 query endpoint using a JSON body (POST).
        /// If the configured ResourceIdentifier is present it will be used; otherwise pass resourceIdentifier param.
        /// The method will build a simple SoQL SELECT when SoqlQuery.RawQuery is not set.
        /// Returns the raw response body as string.
        /// </summary>
        public async Task<string> QueryRaw(SoqlQuery query, string? resourceIdentifier = null)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            string resourceId = resourceIdentifier ?? _config?.ResourceIdentifier ?? throw new ArgumentException("Resource identifier is required");

            // Build a simple SoQL string if RawQuery is not provided
            string soqlText = query.ToString(true);

            var payload = new Dictionary<string, object?>
            {
                ["query"] = soqlText
            };

            var json = System.Text.Json.JsonSerializer.Serialize(payload);
            using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");

            // POST to /api/v3/views/{id}/query.json
            var requestUri = $"api/v3/views/{resourceId}/query.json";
            var resp = await _httpClient.PostAsync(requestUri, content);

            var body = await resp.Content.ReadAsStringAsync();

            Debug.WriteLine(body);

            // Do not swallow errors; return body for caller to inspect
            resp.EnsureSuccessStatusCode();

            return body;
        }

        public async Task<IEnumerable<T>> Query<T>(SoqlQuery query, string? resourceIdentifier = null)
        {
            var body = await QueryRaw(query, resourceIdentifier);
            var options = new System.Text.Json.JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };
            var result = System.Text.Json.JsonSerializer.Deserialize<IEnumerable<T>>(body, options);
            if (result == null)
            {
                throw new InvalidOperationException("Deserialization returned null");
            }
            return result;
        }

        /// <summary>
        /// Export a dataset as CSV via Socrata v3 export endpoint using serialization options.
        /// Returns the CSV bytes (including BOM if requested).
        /// </summary>
        public async Task<byte[]> ExportCsv(string? resourceIdentifier = null, string separator = "\t", bool bom = true)
        {
            string resourceId = resourceIdentifier ?? _config?.ResourceIdentifier ?? throw new ArgumentException("Resource identifier is required");

            var payload = new Dictionary<string, object?>
            {
                ["serializationOptions"] = new Dictionary<string, object?>
                {
                    ["separator"] = separator,
                    ["bom"] = bom
                }
            };

            var json = System.Text.Json.JsonSerializer.Serialize(payload);
            using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");

            var requestUri = $"api/v3/views/{resourceId}/export.csv";
            var resp = await _httpClient.PostAsync(requestUri, content);

            var bytes = await resp.Content.ReadAsByteArrayAsync();

            resp.EnsureSuccessStatusCode();

            return bytes;
        }
    }
}
