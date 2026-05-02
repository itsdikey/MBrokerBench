namespace MBrokerConsumer.Models
{
    internal class ConsumptionCounter
    {
        public long CurrentWindowCount { get; private set; } = 0;
        public long TotalCount { get; private set; } = 0;

        public void Increment(long count = 1)
        {
            CurrentWindowCount += count;
            TotalCount += count;
        }

        public void ResetWindow()
        {
            CurrentWindowCount = 0;
        }
    }
}
