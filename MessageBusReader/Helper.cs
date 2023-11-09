namespace MessageBusReader
{
    using Dapper;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading.Tasks;

    internal class Helper : IHelper
    {
        private readonly string _connectionString;
        private readonly IDbConnection _connection;

        public Helper(string connectionString)
        {
            _connectionString = connectionString;
            _connection = new SqlConnection(connectionString);
        }

        public async Task<bool> IsInvalidConsumer(string consumerId)
        {

            throw new NotImplementedException();

            var count = await _connection.QuerySingleAsync<int>($"SELECT COUNT(*) FROM xx WHERE ConsumerId = '{consumerId}';");
            return count > 0;
        }


    }
}