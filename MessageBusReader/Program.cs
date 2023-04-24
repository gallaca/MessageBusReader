namespace MessageBusReader
{
    using Microsoft.Azure.ServiceBus;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    // using System.Runtime.InteropServices.WindowsRuntime;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    class Program
    {
        private static QueueClient _client;
        private static TaskCompletionSource<int> _taskCompletionSource;
        private static Task<int> _loopTask;
        private static int _counter = 0;
        private static int _messageCount = 0;
        private static IDictionary<string, int> _typeCounter = new Dictionary<string, int>();
        private static long _sequenceNumber = 0;
        private static string _connectionString;
        private static Dictionary<string, Func<Message, Task>> _actions;
        private static string _logFilename;

        private static DateTime _startDate = DateTime.UtcNow;

        static void Main(string[] args)
        {
            var root = Directory.GetCurrentDirectory();
            var dotenv = Path.Combine(root, ".env");
            DotEnv.Load(dotenv);

            // await InitLogger();
            _logFilename = Path.Combine(root, $"{DateTime.UtcNow:yyyymmdd-HHmmss}.log");

            MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            string env;
            env = "PRODUCTION_CONNECTION_STRING";
            // env = "QA_CONNECTION_STRING";
            // env = "DEV_CONNECTION_STRING";
            _connectionString = Environment.GetEnvironmentVariable(env);

            // Error queue
            _client = new QueueClient(
                _connectionString,
                "error", ReceiveMode.PeekLock);

            _actions = new Dictionary<string, Func<Message, Task>>();

            var options = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            _client.RegisterMessageHandler(ProcessMessagesAsync, options);

            _taskCompletionSource = new TaskCompletionSource<int>();

            _loopTask = _taskCompletionSource.Task;

            await _loopTask;

            Console.WriteLine("Task finished");

            Console.ReadLine();
        }

        private static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Stop if we start to reprocess
            if (message.SystemProperties.SequenceNumber > _sequenceNumber)
            {
                _sequenceNumber = message.SystemProperties.SequenceNumber;
            }
            //else
            //{
            //    Console.WriteLine("We have started to repeat ourselves...");
            //    ReportTypeCounters();
            //    Console.ReadLine();
            //    return;
            //}

            // Don't run messages that have arrived after we started
            if (message.SystemProperties.EnqueuedTimeUtc >= _startDate)
            {
                Console.WriteLine("Reached recent messages...");
                ReportTypeCounters();
                Console.ReadLine();
                return;
            }

            _messageCount++;

            Console.WriteLine($"Processing {_messageCount}/{message.SystemProperties.SequenceNumber}: {message.MessageId}");

            if (message.UserProperties.TryGetValue("rbs2-msg-type", out object typeValue) == false)
            {
                Console.WriteLine("Skipping message as unable to get message type");
                return;
            }

            //if (_counter >= 5)
            //{
            //    Console.WriteLine("Stop after 5");
            //    return;
            //}

            //if (message.MessageId == "89a1436e-f2f4-4a61-9a44-0f4cededdff6")
            //{
            //    await _client.CompleteAsync(message.SystemProperties.LockToken);
            //    Console.WriteLine("Removed");
            //    Console.ReadLine();
            //}

            string type = typeValue.ToString();

            Console.WriteLine($"Message type: {type}");
            if (_typeCounter.ContainsKey(type))
            {
                _typeCounter[type] = _typeCounter[type] + 1;
            }
            else
            {
                _typeCounter[type] = 1;
            }


            var invalidMessageIds = new[] 
            {
                "bdc917fd-421b-4430-9c1c-811d6d68ce75",
                "21e1eee8-ae95-4ae9-bce4-74329a4f1d17",
                "c62a2678-3011-468b-8755-05e229cd31b9",
                "dee86d70-6ffe-4a9f-8428-0690b0492021",
                "22cb68db-b6d0-41b2-819c-69e5f632c44e",
                "77bf5c41-3993-45fc-a6e9-f7ab7aaf45d7",
                "849d7333-cbbd-4f2c-8b63-a2fd2d937de9",
                "da0c6258-8129-4a1e-953b-1bb6e098cb5f",
                "edb06856-d42e-4f9f-bae4-60fcdede499d",
                "4cfd339a-f015-4c0d-9da6-eba076b0eb46",
                "5ccbafd2-2422-450f-8cc9-88f1a76f24ba",
                "8229f17d-c29d-4651-a15f-8ae219d06345",
                "dc06949c-3a1f-4b64-b60e-798ea7c88065",
                "7d303413-589c-4093-9dc8-a35a2a2bba77",
                "ee8097c4-6c29-4efe-ad2c-8833c4caf3f4",
                "38aae04d-d522-4055-9ff5-1a303a1dd7b5",
                "54bd76e3-863b-4ed8-994c-7977b6d92e70",
                "d48be53e-48f8-4d92-b2bb-870274afcd78",
                "d6055acc-4b65-4cb4-94fd-6dea58c45a60",
                "08961e7e-a9b7-437b-ad86-7877499898ad",
                "1e75eea6-a364-4f5c-9c76-f5236829d5c2",
                "2250f5dc-63b8-4b71-8c68-7e80a9c2e19e",
                "92b01dda-38fc-4d69-a533-85479b11966b"
            };

            string[] types =
            {
                "Edrington.Data.Consumer.Events.ConsumerConsentPreferencesUpdated, Edrington.Data",
                "Edrington.Data.Consumer.Events.ConsumerCrmPreferencesUpdated, Edrington.Data",
                "Edrington.Data.Consumer.Events.ProductEnquiryMade, Edrington.Data",
                "Edrington.Data.Consumer.Commands.SendNewsletterSubscriptionRequested, Edrington.Data",
                "Edrington.Data.Consumer.Commands.SynchroniseCrmPreferences, Edrington.Data",
                "Edrington.Data.Consumer.Commands.SynchroniseConsentPreferences, Edrington.Data",
                "Edrington.Data.Consumer.Commands.UpdateCommunicationPreferences, Edrington.Data",
                "Edrington.Data.Crm.Commands.AssociateConsentWithCrmAccount, Edrington.Data",
                "Edrington.Data.Crm.Commands.CreateCrmContact, Edrington.Data",
                "Edrington.Data.Crm.Commands.SendConsentManagerIdToCrm, Edrington.Data",
                "Edrington.Data.Crm.Events.ConsentAssociatedWithCrmAccount, Edrington.Data",
                "Edrington.Data.Ecommerce.Events.ProductUpserted, Edrington.Data",
                "Edrington.Data.Order.Events.OrderRefreshFromShopDownloadedV2, Edrington.Data",
                //"Edrington.Data.CrmBridge.Commands.SyncPurchase, Edrington.Data"
            };

            var body = Encoding.UTF8.GetString(message.Body);
            var json = JObject.Parse(body);
            var brand = json.GetValue("Brand").Value<int>();
            var atUtc = json.GetValue("AtUtc").Value<DateTime>();
            var entityId = json.GetValue("EntityId").Value<string>();


            if (invalidMessageIds.Contains(message.MessageId))
            {
                Console.WriteLine("******");
                Console.WriteLine("Deleting message...");
                await Task.Delay(1000, token);
                await CompleteMessage(message);
            }
            else if (types.Length == 0 || types.Contains(type))
            {
                if (type == "Edrington.Data.Consumer.Commands.SendNewsletterSubscriptionRequested, Edrington.Data" && brand != 1)
                {
                    Console.WriteLine("******");
                    Console.WriteLine($"Skipping message {message.MessageId} to source for {entityId} and brand {brand}");
                }
                else
                {  
                    Log($"{type}, {message.MessageId}, {atUtc}, {entityId}, {brand}, {DateTime.UtcNow}");

                    Console.WriteLine("******");
                    Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");

                    // --- await CompleteMessage(message);
                    await ReturnToSource(message);
                    await Task.Delay(1000, token);

                    Console.WriteLine("Done.");
                    Console.WriteLine();
                    _counter++;
                }
            }
        }

        private static void ReportTypeCounters()
        {
            foreach (var type in _typeCounter.Keys)
            {
                Console.WriteLine($"{type} - {_typeCounter[type]}");
            }
        }

        private static Message CreateUpdateProfileCommand(string entityId, string email, string givenName, string familyName, string dob)
        {
            string json = "{" +
            "\"$type\": \"Edrington.Data.Consumer.Commands.UpdateProfile, Edrington.Data\"," +
            $"\"Email\": \"{email}\"," +
            $"\"GivenName\": \"{givenName}\"," +
            $"\"FamilyName\": \"{familyName}\"," +
            $"\"KnownAs\": null," +
            $"\"ContactNumber\": null," +
            $"\"DateofBirth\": \"{dob}\"," +
            "\"GdprOptIn\": true," +
            $"\"EntityId\": \"{entityId}\"," +
            "\"Brand\": 1," +
            "\"CountryCode\": \"gb\"," +
            "\"LanguageTag\": \"en\"," +
            $"\"AtUtc\": \"{DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")}\"" +
            "}";

            byte[] body = UTF8Encoding.UTF8.GetBytes(json);

            Message message = new Message(body);

            message.UserProperties.Add("rbs2-corr-id", "ANDYTEST");
            message.UserProperties.Add("rbs2-intent", "p2p");
            message.UserProperties.Add("rbs2-msg-id", Guid.NewGuid().ToString());
            message.UserProperties.Add("rbs2-senttime", DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"));
            message.UserProperties.Add("rbs2-msg-type", "Edrington.Data.Consumer.Commands.UpdateProfile, Edrington.Data");
            message.UserProperties.Add("rbs2-content-type", "application/json;charset=utf-8");
            message.UserProperties.Add("rbs2-error-details", "");
            message.UserProperties.Add("rbs2-source-queue", "consumer");

            return message;
        }

        private static Func<Message, Task> GetAction(Message message)
        {
            while (true)
            {
                message.UserProperties.TryGetValue("rbs2-source-queue", out object sourceValue);

                Console.WriteLine("Please select default action:");
                Console.WriteLine("1. Skip");
                Console.WriteLine("2. Complete");
                Console.WriteLine($"3. Return to source queue ({sourceValue})");

                Console.Write("Choice: ");
                string choice = Console.ReadLine();

                switch (choice)
                {
                    case "1":
                        return SkipMessage;
                    case "2":
                        return CompleteMessage;
                    case "3":
                        return ReturnToSource;
                }
            }
        }

        private static Task SkipMessage(Message message)
        {
            return Task.CompletedTask;
        }

        private static async Task CompleteMessage(Message message)
        {
            await _client.CompleteAsync(message.SystemProperties.LockToken);
        }

        private static async Task ReturnToSource(Message message)
        {
            if (message.UserProperties.TryGetValue("rbs2-source-queue", out object sourceValue) == false)
            {
                Console.WriteLine("Message does not have a source queue property");
                return;
            }

            string source = sourceValue.ToString();

            var client = new QueueClient(
                _connectionString,
                source);

            var copy = message.Clone();

            await client.SendAsync(copy);

            await client.CloseAsync();

            await _client.CompleteAsync(message.SystemProperties.LockToken);
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }

        private static void Log(string message)
        {
            File.AppendAllLines(_logFilename, new List<string> { message });
        }
    }

    public static class MessageExtensions
    {
        public static bool ContainsError(this Message message, string error)
        {
            const string errorKey = "rbs2-error-details";

            if (message.UserProperties.ContainsKey(errorKey))
            {
                string errorMessage = message.UserProperties[errorKey].ToString();

                return errorMessage.Contains(error);
            }

            return false;
        }

        public static dynamic BodyAsDynamic(this Message message)
        {
            string body = Encoding.UTF8.GetString(message.Body);

            dynamic msg = JObject.Parse(body);

            return msg;
        }
    }
}
