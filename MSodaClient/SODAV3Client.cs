using System.Diagnostics;
using System.Collections.Concurrent;
using System.IO;
using System.Text.Json;
using System.Text;
using System.Threading;

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

        /// <summary>
        /// How often (seconds) to persist the in-memory cache to disk. Default 1 seconds.
        /// </summary>
        public int CachePersistIntervalSeconds { get; set; } = 1;

        /// <summary>
        /// Maximum number of cache entries to keep when persisting. 0 means unlimited. Default 0.
        /// </summary>
        public int CacheMaxEntries { get; set; } = 0;
    }

    public sealed class SODAV3Client : IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly SODAV3ClientConfig? _config;

        // Simple in-memory cache: key -> (value, expiresUtc)
        private readonly ConcurrentDictionary<string, (object Value, DateTime ExpiresUtc)> _cache = new();

        // File save synchronization
        private readonly SemaphoreSlim _fileLock = new(1,1);
        private readonly Timer? _saveTimer;
        private volatile bool _dirty = false;
        private bool _disposed = false;

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

                    // Start a periodic timer to persist the cache occasionally instead of on every update
                    var interval = Math.Max(1, config.CachePersistIntervalSeconds);
                    _saveTimer = new Timer(async _ => await SaveCacheToFileAsync().ConfigureAwait(false), null, TimeSpan.FromSeconds(interval), TimeSpan.FromSeconds(interval));
                }
            }
        }

        /// <summary>
        /// Clears the entire response cache.
        /// </summary>
        public void ClearCache()
        {
            _cache.Clear();
            _dirty = false;
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

            _dirty = true;
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
                    _dirty = true;
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

            // Mark dirty; periodic timer will persist. This avoids heavy IO on rapid updates.
            _dirty = true;
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
            if (_config?.EnableCaching != true) return;
            if (!_dirty) return; // nothing to do

            await _fileLock.WaitAsync().ConfigureAwait(false);
            try
            {
                // Re-check dirty under lock
                if (!_dirty) return;

                var path = GetCacheFilePath();
                var dir = Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

                // Build dictionary snapshot, excluding expired entries
                var dict = new Dictionary<string, CacheFileEntryDto>();
                foreach (var kv in _cache)
                {
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

                // Trim to max entries if configured
                if (_config.CacheMaxEntries > 0 && dict.Count > _config.CacheMaxEntries)
                {
                    var ordered = dict.OrderBy(kv => kv.Value.ExpiresUtc).ToList();
                    int removeCount = dict.Count - _config.CacheMaxEntries;
                    for (int i = 0; i < removeCount; i++)
                    {
                        dict.Remove(ordered[i].Key);
                    }
                }

                var json = JsonSerializer.Serialize(dict);

                // atomic write: write to temp file then move
                var tmp = path + ".tmp";
                await File.WriteAllTextAsync(tmp, json).ConfigureAwait(false);

                // Replace/Move across filesystems safely if possible
                try
                {
                    File.Move(tmp, path, true);
                }
                catch (PlatformNotSupportedException)
                {
                    // fallback
                    if (File.Exists(path)) File.Delete(path);
                    File.Move(tmp, path);
                }

                _dirty = false;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error saving cache file: {ex}");
            }
            finally
            {
                _fileLock.Release();
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

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            // Stop timer
            try
            {
                _saveTimer?.Change(Timeout.Infinite, Timeout.Infinite);
                _saveTimer?.Dispose();
            }
            catch { }

            // Flush cache synchronously
            try
            {
                SaveCacheToFileAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error flushing cache on dispose: {ex}");
            }

            _fileLock.Dispose();
            _httpClient.Dispose();
        }
    }
}
