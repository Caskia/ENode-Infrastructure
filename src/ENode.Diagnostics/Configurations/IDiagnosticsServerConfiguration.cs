namespace ENode.Diagnostics.Configurations
{
    public interface IDiagnosticsServerConfiguration
    {
        string Host { get; set; }

        int Port { get; set; }
    }
}