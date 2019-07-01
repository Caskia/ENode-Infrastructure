using ECommon.Remoting;
using System.Diagnostics;
using System.Net;
using System.Reflection;

namespace ENode.Diagnostics.Clients
{
    public class Counter
    {
        private string _localIp;
        private int _processId;
        private string _processName;
        private SocketRemotingClient _socketRemotingClient;

        #region Properties

        public string LocalIp
        {
            get
            {
                if (string.IsNullOrEmpty(_localIp))
                {
                    _localIp = Utils.SocketUtils.GetLocalIPV4().ToString();
                }

                return _localIp;
            }
        }

        public int ProcessId
        {
            get
            {
                if (_processId == 0)
                {
                    _processId = Process.GetCurrentProcess().Id;
                }

                return _processId;
            }
        }

        public string ProcessName
        {
            get
            {
                if (string.IsNullOrEmpty(_processName))
                {
                    _processName = Assembly.GetEntryAssembly().FullName;
                }

                return _processName;
            }
        }

        #endregion Properties

        public Counter(CounterSetting setting)
        {
            _socketRemotingClient = new SocketRemotingClient(setting.Name, new DnsEndPoint(setting.ServerHost, setting.ServerPort));
        }
    }
}