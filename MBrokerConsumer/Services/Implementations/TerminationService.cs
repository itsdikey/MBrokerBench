using System.Runtime.InteropServices;

namespace MBrokerConsumer.Services.Implementations
{
    internal class TerminationService : ITerminationService
    {
        public event Action? TerminationRequested;

        private bool _isTerminationRequested = false;

        public TerminationService() 
        {
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                RequestTermination();
            };

            AppDomain.CurrentDomain.ProcessExit += (_, e) =>
            {
                RequestTermination();
            };

            PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ =>
            {
                RequestTermination();
            });
        }

        public void RequestTermination()
        {
            if (_isTerminationRequested)
                return;

            _isTerminationRequested = true;
            TerminationRequested?.Invoke();
        }
    }
}
