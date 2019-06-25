using ENode.Commanding;

namespace ENode.Monitor.Tests.Commands
{
    public class ChangeBookName : Command<string>
    {
        public ChangeBookName(string id) : base(id)
        {
        }

        protected ChangeBookName()
        {
        }

        public string Name { get; set; }
    }
}