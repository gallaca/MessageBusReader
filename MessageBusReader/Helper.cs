namespace MessageBusReader
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using Dapper;

    internal class Helper : IHelper
    {
        private readonly IDbConnection _connection;

        public Helper(string connectionString)
        {
            _connection = new SqlConnection(connectionString);
        }

        public async Task<string> GetConsumerId(int brand, int identityType, string identityId)
        {
            try
            {
                var consumerId = await _connection.QuerySingleAsync<string>($"SELECT ConsumerId FROM Consumer.ConsumerIdentity WHERE Brand = {brand} AND IdentityType = {identityType} AND IdentityId = '{identityId}';");
                if (string.IsNullOrWhiteSpace(consumerId))
                {
                    throw new ConsumerNotFoundException($"Cannot get consumer ID for type {identityType} with id '{identityId}'");
                }

                return consumerId;
            }
            catch (InvalidOperationException ex)
            {
                if (ex.Message == "Sequence contains no elements")
                {
                    throw new ConsumerNotFoundException($"Cannot get consumer ID for type {identityType} with id '{identityId}'");
                }

                Console.Error.WriteLine(ex.Message);
                throw;
            }
        }

        public async Task<bool> IsInvalidConsumer(string consumerId)
        {
            try
            {
                var count = await _connection.QuerySingleAsync<int>($"SELECT COUNT(*) FROM dbo.InvalidConsumers WHERE ConsumerId = '{consumerId}';");
                return count > 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                throw;
            }
        }
    }
}