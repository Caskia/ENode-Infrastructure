using ENode.Commanding;

namespace ENode.Diagnostics.Tests.Commands
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