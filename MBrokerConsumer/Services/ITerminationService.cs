namespace MBrokerConsumer.Services
{
    public interface ITerminationService
    {
        public event Action TerminationRequested;
    }
}
