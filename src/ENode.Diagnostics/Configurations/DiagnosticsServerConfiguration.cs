namespace ENode.Diagnostics.Configurations
{
    public class DiagnosticsServerConfiguration : IDiagnosticsServerConfiguration
    {
        public string Host { get; set; }

        public int Port { get; set; }
    }
}