using ECommon.Components;
using ECommon.Configurations;
using ECommon.Logging;
using ENode.Commanding;
using ENode.Configurations;
using System.Reflection;
using System.Threading;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace ENode.Diagnostics.Tests
{
    public class DiagnosticsTestBase
    {
        private ICommandProcessor _commandProcessor;
        private ENodeConfiguration _enodeConfiguration;
        private ILogger _logger;

        public DiagnosticsTestBase()
        {
            Initialize();
        }

        public void Dispose()
        {
            CleanupEnode();
        }

        protected void Initialize()
        {
            if (_enodeConfiguration != null)
            {
                CleanupEnode();
            }

            InitializeENode();
        }

        protected void ProcessCommand(ProcessingCommand command)
        {
            _commandProcessor.Process(command);
        }

        private void CleanupEnode()
        {
            Thread.Sleep(1000);
            _enodeConfiguration.Stop();
            _logger.Info("ENode shutdown.");
        }

        private void InitializeENode()
        {
            var assemblies = new[]
            {
                Assembly.GetExecutingAssembly()
            };

            _enodeConfiguration = ECommonConfiguration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler()
                .CreateENode()
                .RegisterENodeComponents()
                .RegisterBusinessComponents(assemblies)
                .UseENodeDiagnosticsService()
                .BuildContainer()
                .InitializeBusinessAssemblies(assemblies);

            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create("main");
            _commandProcessor = ObjectContainer.Resolve<ICommandProcessor>();
        }
    }
}