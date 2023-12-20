namespace MessageBusReader
{
    using System;

    public class ConsumerNotFoundException : Exception
    {
        public ConsumerNotFoundException(string message)
            : base(message)
        {
        }
    }
}
