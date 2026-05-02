namespace MBrokerConsumer.Services.Implementations;

internal sealed class TokenBucketLimiter
{
    private readonly object _lock = new();
    private double _tokens;
    private int _maxRate;
    private readonly TimeSpan _interval;
    private DateTime _lastRefill;

    public TokenBucketLimiter(int maxRate, TimeSpan interval)
    {
        _maxRate = maxRate;
        _interval = interval;
        _tokens = maxRate;
        _lastRefill = DateTime.UtcNow;
    }

    /// <summary>
    /// Non-blocking check. Returns true if a token was available and consumed.
    /// </summary>
    public bool TryConsume()
    {
        lock (_lock)
        {
            Refill();
            if (_tokens >= 1.0)
            {
                _tokens -= 1.0;
                return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Blocks until a token is available, then consumes it.
    /// </summary>
    public async Task ConsumeAsync()
    {
        while (true)
        {
            if (TryConsume())
                return;
            await Task.Delay(10);
        }
    }

    /// <summary>
    /// Thread-safe runtime reconfiguration of the max rate.
    /// Caps accumulated tokens to the new max rate to prevent burst spikes after rate reduction.
    /// </summary>
    public void UpdateRate(int newMaxRate)
    {
        lock (_lock)
        {
            _maxRate = newMaxRate;
            if (_tokens > _maxRate)
                _tokens = _maxRate;
        }
    }

    private void Refill()
    {
        var now = DateTime.UtcNow;
        var elapsed = now - _lastRefill;
        if (elapsed <= TimeSpan.Zero)
            return;

        var tokensToAdd = elapsed.TotalSeconds / _interval.TotalSeconds * _maxRate;
        _tokens = Math.Min(_tokens + tokensToAdd, _maxRate);
        _lastRefill = now;
    }
}
