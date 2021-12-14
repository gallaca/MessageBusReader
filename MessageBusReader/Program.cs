namespace MessageBusReader
{
    using Microsoft.Azure.ServiceBus;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Runtime.InteropServices.WindowsRuntime;
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
        private static Dictionary<string, Func<Message, Task>> _actions;

        private static DateTime _startDate = DateTime.UtcNow;

        static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            // PROD error queue
            _client = new QueueClient(
                "--PROD SERVICE BUS CONNECTION STRING HERE--",
               "error", receiveMode: ReceiveMode.PeekLock);

            // QA
            //_client = new QueueClient(
            //    "--QA SERVICE BUS CONNECTION STRING HERE--",
            //    "error", ReceiveMode.PeekLock);

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
            // Don't run messages that have arrived after we started
            if (message.SystemProperties.EnqueuedTimeUtc >= _startDate)
            {
                Console.WriteLine("Reached recent messages...");
                Console.ReadLine();
                return;
            }

            _messageCount++;

            Console.WriteLine($"Processing {_messageCount}: {message.MessageId}");

            if (message.UserProperties.TryGetValue("rbs2-msg-type", out object typeValue) == false)
            {
                Console.WriteLine("Skipping message as unable to get message type");
                return;
            }

            //if (message.MessageId == "89a1436e-f2f4-4a61-9a44-0f4cededdff6")
            //{
            //    await _client.CompleteAsync(message.SystemProperties.LockToken);
            //    Console.WriteLine("Removed");
            //    Console.ReadLine();
            //}

            string type = typeValue.ToString();

            Console.WriteLine($"Message type: {type}");

            string[] types =
            {
                "Edrington.Data.Consumer.Commands.AddConsumerAddress, Edrington.Data",
            };

            if (types.Contains(type))
            {
                await CompleteMessage(message);
            }

            //if (types.Contains(type))
            //{
            //    //if (message.ContainsError("could not be dispatched to any handlers"))
            //    //{
            //    //}
            //    _counter++;

            //    //await ReturnToSource(message);
            //    await CompleteMessage(message);

            //    Console.WriteLine($"Completed {_counter} messages");
            //}

            //if (type == "Edrington.Data.Consumer.Commands.SynchroniseConsentPreferences, Edrington.Data")
            //{
            //    if (((string)message.BodyAsDynamic().EntityId).Contains("Syrenis", StringComparison.InvariantCultureIgnoreCase))
            //    {
            //        await CompleteMessage(message);
            //    }
            //}

            //if (type == "Edrington.Data.EventBooking.Commands.UpdateBooking, Edrington.Data")
            //{
            //    await CompleteMessage(message);
            //}

            //if (type == "Edrington.Data.Consumer.Commands.UpdateConsumerAddress, Edrington.Data")
            //{
            //    await CompleteMessage(message);
            //}

            //if (type == "Edrington.Data.Consumer.Commands.SendPasswordResetEmail, Edrington.Data")
            //{
            //    await CompleteMessage(message);
            //}

            //}
            //    _counter++;

            //    await CompleteMessage(message);

            //    Console.WriteLine($"Removed {_counter} messages");

            //    //await _client.CompleteAsync(message.SystemProperties.LockToken);

            //    //

            //    //string json = Encoding.UTF8.GetString(message.Body);

            //    //_counter++;

            //    //await _client.CompleteAsync(message.SystemProperties.LockToken);

            //    //if (obj.Brand == 8)
            //    //{
            //    //    _counter++;

            //    //    await ReturnToSource(message);

            //    //    //await _client.CompleteAsync(message.SystemProperties.LockToken);

            //    //    Console.WriteLine($"Removed {_counter} messages");
            //    //}


            //if (_actions.ContainsKey(type) == false)
            //{
            //    _actions.Add(type, GetAction(message));
            //}

            //Console.WriteLine($"Using action: {_actions[type].Method.Name}");

            //await _actions[type].Invoke(message);

            //if (message.Label.StartsWith("BallotEntryPaymentCheck"))
            //{
            //    // Put it back in ballot entry queue
            //    Console.WriteLine($"Putting message {message.Label} into ballotentry");

            //    await _ballotEntryQueue.SendAsync(message);

            //    await _client.CompleteAsync(message.SystemProperties.LockToken);
            //    Console.WriteLine($"{message.Label} completed");
            //}
            //else
            //{
            //    Console.WriteLine($"Ignoring {message.Label}");
            //}

            //await Task.Delay(100, token);

            //if (message.SystemProperties.EnqueuedTimeUtc < DateTime.Now.AddDays(-7))
            //{
            //    await _client.CompleteAsync(message.SystemProperties.LockToken);
            //    Console.WriteLine($"Removed {message.MessageId}");
            //}
            //else if (message.Label.StartsWith("SendPasswordResetEmail"))
            //{
            //    await _client.CompleteAsync(message.SystemProperties.LockToken);
            //    Console.WriteLine($"Removed {message.MessageId}");
            //}

            //if (message.SystemProperties.EnqueuedTimeUtc > DateTime.Today)
            //{
            //    string body = Encoding.UTF8.GetString(message.Body);
            //    Console.WriteLine(message.MessageId);
            //    Console.WriteLine(body);
            //}

            //Console.WriteLine("Complete? y or (n)");
            //string response = Console.ReadLine();

            //if (!string.IsNullOrWhiteSpace(response) && response.ToLower() == "y")
            //{
            //    await _client.CompleteAsync(message.SystemProperties.LockToken);
            //}
            //else if (!string.IsNullOrWhiteSpace(response) && response.ToLower() == "exit")
            //{
            //    _taskCompletionSource.SetResult(1);
            //}
        }

        private static Message CreateUpdateProfileCommand(string entityId, string email, string givenName, string familyName, string dob)
        {
            string json = "{"+
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
                "Endpoint=sb://prodedringtonservicebus.servicebus.windows.net/;SharedAccessKeyName=SharedKey;SharedAccessKey=sHAYVL6e+8TMSSIo0HhFkl1ffQ8YzoIpLsN2OQP0xww=",
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
