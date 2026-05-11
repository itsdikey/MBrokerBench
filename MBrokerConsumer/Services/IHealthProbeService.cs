namespace MBrokerConsumer.Services;

public interface IHealthProbeService
{
    void MarkReady();
    void ReportMessageReceived();
    void Stop();
}