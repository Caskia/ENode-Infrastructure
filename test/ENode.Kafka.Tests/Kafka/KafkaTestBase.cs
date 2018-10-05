using ECommon.Configurations;
using ENode.Configurations;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace ENode.Kafka.Tests.Kafka
{
    public class KafkaTestBase
    {
        public KafkaTestBase()
        {
            BuildConfiguration();

            var assemblies = new[] { Assembly.GetExecutingAssembly() };

            var enode = ECommonConfiguration.Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .CreateENode(new ConfigurationSetting())
                .RegisterBusinessComponents(assemblies)
                .RegisterENodeComponents();

            enode.GetCommonConfiguration()
              .BuildContainer();
        }

        public IConfigurationRoot Root { get; set; }

        private void BuildConfiguration()
        {
            var basePath = Directory.GetCurrentDirectory();

            //default setting
            var builder = new ConfigurationBuilder()
                 .SetBasePath(basePath)
                 .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            var environmentName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            if (!string.IsNullOrWhiteSpace(environmentName))
            {
                builder = builder.AddJsonFile($"appsettings.{environmentName}.json", optional: true, reloadOnChange: true);
            }

            //external setting for docker
            var configDirName = "docker-config";
            var dirPath = $"{basePath}{Path.DirectorySeparatorChar}{configDirName}";
            if (Directory.Exists(dirPath))
            {
                var skipDirectory = dirPath.Length;
                if (!dirPath.EndsWith("" + Path.DirectorySeparatorChar)) skipDirectory++;
                var fileNames = Directory.EnumerateFiles(dirPath, "*.json", SearchOption.AllDirectories)
                    .Select(f => f.Substring(skipDirectory));
                foreach (var fileName in fileNames)
                {
                    builder = builder.AddJsonFile($"{configDirName}{Path.DirectorySeparatorChar}{fileName}", optional: true, reloadOnChange: true);
                }
            }

            //enviroment variables
            builder.AddEnvironmentVariables();
            Root = builder.Build();
        }
    }
}