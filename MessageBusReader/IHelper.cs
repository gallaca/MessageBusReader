namespace MessageBusReader
{
    using System;
    using System.Threading.Tasks;

    internal interface IHelper
    {
        Task<bool> IsInvalidConsumer(string consumerId);

        Task<string> GetConsumerId(int brand, int identityType, string identityId);
    }
}