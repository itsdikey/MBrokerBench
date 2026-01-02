namespace MBrokerBench
{
    public class MathUtils
    {

        public static double CalculateRScore(IList<ReassignedPartitionDetails> elements)
        {
            double rScore = 0.0;
            for (int i = 0; i < elements.Count; i++)
            {
                var (NewConsumerCapacity, PartitionRate) = elements[i];
                rScore += PartitionRate / NewConsumerCapacity;
            }
            return rScore;
        }
    }

    public record struct ReassignedPartitionDetails(double NewConsumerCapacity, double PartitionRate)
    {
        public static implicit operator (double NewConsumerCapacity, double PartitionRate)(ReassignedPartitionDetails value)
        {
            return (value.NewConsumerCapacity, value.PartitionRate);
        }

        public static implicit operator ReassignedPartitionDetails((double NewConsumerCapacity, double PartitionRate) value)
        {
            return new ReassignedPartitionDetails(value.NewConsumerCapacity, value.PartitionRate);
        }
    }
}
