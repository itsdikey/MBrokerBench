using System.Diagnostics;
using System.Collections.Concurrent;
using System.IO;
using System.Text.Json;
using System.Text;

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

        // Caching options
        public bool EnableCaching { get; set; } = false;
        /// <summary>
        /// Cache duration in seconds for cached responses. Default 60 seconds.
        /// </summary>
        public int CacheDurationSeconds { get; set; } = 60;

        /// <summary>
        /// Optional file path to persist the cache. If null a default location under LocalApplicationData will be used.
        /// </summary>
        public string? CacheFilePath { get; set; }
    }

    public sealed class SODAV3Client
    {
        private readonly HttpClient _httpClient;
        private readonly SODAV3ClientConfig? _config;

        // Simple in-memory cache: key -> (value, expiresUtc)
        private readonly ConcurrentDictionary<string, (object Value, DateTime ExpiresUtc)> _cache = new();

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

                // Load persisted cache if caching enabled
                if (config.EnableCaching)
                {
                    try
                    {
                        LoadCacheFromFile();
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"Failed to load cache from file: {ex}");
                    }
                }
            }
        }

        /// <summary>
        /// Clears the entire response cache.
        /// </summary>
        public void ClearCache()
        {
            _cache.Clear();
            try
            {
                var path = GetCacheFilePath();
                if (File.Exists(path)) File.Delete(path);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Failed to delete cache file: {ex}");
            }
        }

        /// <summary>
        /// Invalidate cached entries that reference a specific resource identifier.
        /// This looks for cache keys that contain the resource id segment.
        /// </summary>
        public void InvalidateCacheForResource(string resourceIdentifier)
        {
            if (string.IsNullOrEmpty(resourceIdentifier)) return;

            var keys = _cache.Keys.Where(k => k.Contains(resourceIdentifier, StringComparison.OrdinalIgnoreCase)).ToList();
            foreach (var k in keys)
            {
                _cache.TryRemove(k, out _);
            }

            // Persist changes
            _ = SaveCacheToFileAsync();
        }

        private static string BuildCacheKey(string requestUri, string body)
        {
            // Keep keys short - hash the body if it's long
            var combined = requestUri + "|" + body;
            if (combined.Length > 200)
            {
                using var sha = System.Security.Cryptography.SHA256.Create();
                var hash = sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(combined));
                return requestUri + "|" + Convert.ToBase64String(hash);
            }
            return combined;
        }

        private bool TryGetCachedValue<T>(string key, out T? value)
        {
            value = default;
            if (_config?.EnableCaching != true) return false;

            if (_cache.TryGetValue(key, out var entry))
            {
                if (entry.ExpiresUtc > DateTime.UtcNow)
                {
                    if (entry.Value is T t)
                    {
                        value = t;
                        return true;
                    }
                    // try to convert for common cases
                    if (typeof(T) == typeof(string) && entry.Value is byte[] bytes)
                    {
                        value = (T?)(object)System.Text.Encoding.UTF8.GetString(bytes);
                        return true;
                    }
                }
                else
                {
                    // expired
                    _cache.TryRemove(key, out _);
                    _ = SaveCacheToFileAsync();
                }
            }
            return false;
        }

        private void SetCachedValue(string key, object value)
        {
            if (_config?.EnableCaching != true) return;
            var ttl = Math.Max(1, _config.CacheDurationSeconds);
            var expires = DateTime.UtcNow.AddSeconds(ttl);
            _cache[key] = (value, expires);

            // Persist cache asynchronously; fire-and-forget
            _ = SaveCacheToFileAsync();
        }

        private string GetCacheFilePath()
        {
            if (!string.IsNullOrEmpty(_config?.CacheFilePath)) return _config.CacheFilePath!;
            var dir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "MSodaClient");
            Directory.CreateDirectory(dir);
            return Path.Combine(dir, "cache.json");
        }

        private void LoadCacheFromFile()
        {
            var path = GetCacheFilePath();
            if (!File.Exists(path)) return;

            try
            {
                var json = File.ReadAllText(path);
                if (string.IsNullOrEmpty(json)) return;

                var dict = JsonSerializer.Deserialize<Dictionary<string, CacheFileEntryDto>>(json);
                if (dict == null) return;

                foreach (var kv in dict)
                {
                    // skip expired
                    if (kv.Value.ExpiresUtc <= DateTime.UtcNow) continue;

                    object val;
                    if (kv.Value.ValueKind == "bytes")
                    {
                        val = Convert.FromBase64String(kv.Value.ValueBase64);
                    }
                    else
                    {
                        val = Encoding.UTF8.GetString(Convert.FromBase64String(kv.Value.ValueBase64));
                    }

                    _cache[kv.Key] = (val, kv.Value.ExpiresUtc);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error loading cache file: {ex}");
            }
        }

        private async Task SaveCacheToFileAsync()
        {
            try
            {
                var path = GetCacheFilePath();
                var dir = Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

                var dict = new Dictionary<string, CacheFileEntryDto>();
                foreach (var kv in _cache)
                {
                    // skip expired entries during save
                    if (kv.Value.ExpiresUtc <= DateTime.UtcNow) continue;

                    string kind;
                    string base64;
                    if (kv.Value.Value is byte[] b)
                    {
                        kind = "bytes";
                        base64 = Convert.ToBase64String(b);
                    }
                    else
                    {
                        kind = "string";
                        var s = kv.Value.Value?.ToString() ?? string.Empty;
                        base64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(s));
                    }

                    dict[kv.Key] = new CacheFileEntryDto
                    {
                        ValueBase64 = base64,
                        ValueKind = kind,
                        ExpiresUtc = kv.Value.ExpiresUtc
                    };
                }

                var json = JsonSerializer.Serialize(dict);
                await File.WriteAllTextAsync(path, json).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error saving cache file: {ex}");
            }
        }

        private record CacheFileEntryDto
        {
            public string ValueBase64 { get; set; } = string.Empty;
            public string ValueKind { get; set; } = "string";
            public DateTime ExpiresUtc { get; set; }
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

            var requestUri = $"api/v3/views/{resourceId}/query.json";

            // Check cache
            var cacheKey = BuildCacheKey(requestUri, json);
            if (TryGetCachedValue<string>(cacheKey, out var cachedBody))
            {
                Debug.WriteLine("Returning cached response for QueryRaw");
                return cachedBody!;
            }

            using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");

            // POST to /api/v3/views/{id}/query.json
            var resp = await _httpClient.PostAsync(requestUri, content);

            var body = await resp.Content.ReadAsStringAsync();

            Debug.WriteLine(body);

            // Do not swallow errors; return body for caller to inspect
            resp.EnsureSuccessStatusCode();

            // Cache successful result
            SetCachedValue(cacheKey, body);

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

            var requestUri = $"api/v3/views/{resourceId}/export.csv";

            var cacheKey = BuildCacheKey(requestUri, json);
            if (TryGetCachedValue<byte[]>(cacheKey, out var cachedBytes))
            {
                Debug.WriteLine("Returning cached response for ExportCsv");
                return cachedBytes!;
            }

            using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");

            var resp = await _httpClient.PostAsync(requestUri, content);

            var bytes = await resp.Content.ReadAsByteArrayAsync();

            resp.EnsureSuccessStatusCode();

            SetCachedValue(cacheKey, bytes);

            return bytes;
        }
    }
}
