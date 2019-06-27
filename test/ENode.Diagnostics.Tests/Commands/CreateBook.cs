using ENode.Commanding;

namespace ENode.Diagnostics.Tests.Commands
{
    public class CreateBook : Command<string>
    {
        public CreateBook(string id) : base(id)
        {
        }

        protected CreateBook()
        {
        }

        public string Name { get; set; }
    }
}