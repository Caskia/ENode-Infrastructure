namespace ENode.Kafka
{
    public enum ENodeMessageTypeCode
    {
        CommandMessage = 1,
        DomainEventStreamMessage = 2,
        ExceptionMessage = 3,
        ApplicationMessage = 4,
    }
}