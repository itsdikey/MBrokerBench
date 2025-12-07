using System.Net.Http;
using System.Text;
using System.Text.Json;

namespace MBrokerBench
{
    public static class PrometheusExporter
    {
        private static readonly HttpClient _http = new HttpClient();

        // Export a set of metrics for the given strategy/run from Prometheus query_range API to CSV files
        public static async Task ExportAllMetricsAsync(string prometheusUrl, string strategy, string runId, DateTime startUtc, DateTime endUtc, string step = "15s")
        {
            var metrics = new[]
            {
                "total_system_lag_messages",
                "total_system_production_rate_msgs_per_sec",
                "consumers_total",
                "partition_production_rate_msgs_per_sec",
                "partition_lag_messages",
                "partition_reassignments_total",
                "partition_reassignments",
                "consumer_utilization_percent",
                "consumer_assigned_partitions_count"
            };

            var start = startUtc.ToString("o");
            var end = endUtc.ToString("o");

            var outDir = Path.Combine(AppContext.BaseDirectory, "export_csv");

            foreach (var metric in metrics)
            {
                var query = $"{metric}{{strategy=\"{strategy}\",run=\"{runId}\"}}";
                var url = new StringBuilder();
                url.Append(prometheusUrl.TrimEnd('/'));
                url.Append("/api/v1/query_range");
                url.Append("?query=").Append(Uri.EscapeDataString(query));
                url.Append("&start=").Append(Uri.EscapeDataString(start));
                url.Append("&end=").Append(Uri.EscapeDataString(end));
                url.Append("&step=").Append(Uri.EscapeDataString(step));

                Console.WriteLine($"[EXPORT] Querying Prometheus for metric {metric} ...");

                using var resp = await _http.GetAsync(url.ToString());
                if (!resp.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[EXPORT] Prometheus query failed for {metric}: {resp.StatusCode}");
                    continue;
                }

                using var stream = await resp.Content.ReadAsStreamAsync();
                using var doc = await JsonDocument.ParseAsync(stream);
                if (!doc.RootElement.TryGetProperty("data", out var data))
                {
                    Console.WriteLine($"[EXPORT] No data for metric {metric}");
                    continue;
                }

                var results = data.GetProperty("result");
                if (results.GetArrayLength() == 0)
                {
                    Console.WriteLine($"[EXPORT] No series returned for {metric}");
                    continue;
                }

                var fileName = Path.Combine(outDir, SanitizeFileName($"{metric}_{strategy}_{runId}.csv"));
                using var writer = new StreamWriter(fileName, false, Encoding.UTF8);

                // We'll write a header that includes timestamp, value, and any label columns found across series
                // Collect union of labels
                var labelSet = new SortedSet<string>();
                foreach (var series in results.EnumerateArray())
                {
                    if (series.TryGetProperty("metric", out var metricObj))
                    {
                        foreach (var prop in metricObj.EnumerateObject())
                        {
                            labelSet.Add(prop.Name);
                        }
                    }
                }

                // Ensure timestamp & value first, then labels
                var labelsList = labelSet.ToList();
                writer.WriteLine(string.Join(',', new[] { "timestamp", "value" }.Concat(labelsList)));

                // Write rows for each series' values
                foreach (var series in results.EnumerateArray())
                {
                    var metricObj = series.GetProperty("metric");
                    // build label map
                    var labels = new Dictionary<string, string>();
                    foreach (var prop in metricObj.EnumerateObject())
                        labels[prop.Name] = prop.Value.GetString() ?? string.Empty;

                    var values = series.GetProperty("values"); // array of [ <unix_time>, "<value>" ]
                    foreach (var v in values.EnumerateArray())
                    {
                        var ts = v[0].GetRawText();
                        // ts is like "162..." as number; convert to ISO
                        if (double.TryParse(ts, out var unix))
                        {
                            var iso = DateTimeOffset.FromUnixTimeSeconds((long)unix).UtcDateTime.ToString("o");
                            var val = v[1].GetString() ?? v[1].ToString();

                            // build label values in order
                            var rowLabels = labelsList.Select(k => CsvEscape(labels.TryGetValue(k, out var vv) ? vv : string.Empty));
                            writer.WriteLine(string.Join(',', new[] { CsvEscape(iso), CsvEscape(val) }.Concat(rowLabels)));
                        }
                        else
                        {
                            // fallback: use raw
                            var val = v[1].GetString() ?? v[1].ToString();
                            var rowLabels = labelsList.Select(k => CsvEscape(labels.TryGetValue(k, out var vv) ? vv : string.Empty));
                            writer.WriteLine(string.Join(',', new[] { CsvEscape(ts), CsvEscape(val) }.Concat(rowLabels)));
                        }
                    }
                }

                Console.WriteLine($"[EXPORT] Wrote {fileName}");
            }
        }

        private static string SanitizeFileName(string s)
        {
            foreach (var c in Path.GetInvalidFileNameChars())
                s = s.Replace(c, '_');
            return s;
        }

        private static string CsvEscape(string s)
        {
            if (s.Contains('"') || s.Contains(',') || s.Contains('\n') || s.Contains('\r'))
            {
                return '"' + s.Replace("\"", "\"\"") + '"';
            }
            return s;
        }
    }
}
