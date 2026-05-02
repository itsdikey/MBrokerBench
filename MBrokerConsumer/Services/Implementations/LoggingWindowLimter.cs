namespace MBrokerConsumer.Services.Implementations
{
    internal class LoggingWindowLimter
    {
        private readonly TimeSpan _windowSize;
        private DateTime _windowStart;

        public TimeSpan CurrentTimePassed => DateTime.UtcNow - _windowStart;

        public LoggingWindowLimter(TimeSpan windowSize)
        {
            _windowSize = windowSize;
            _windowStart = DateTime.UtcNow;
        }

        public bool ShouldLog(out TimeSpan timePassed)
        {
            var now = DateTime.UtcNow;
            timePassed = CurrentTimePassed;
            LastTime = timePassed;
            if (timePassed >= _windowSize)
            {
                _windowStart = now;
                return true;
            }
            return false;
        }

        public TimeSpan LastTime { get; private set; }
    }
}
