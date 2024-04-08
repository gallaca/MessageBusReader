namespace MessageBusReader
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json.Linq;
    using Azure.Messaging.ServiceBus;
    using System.Threading;
    using System.IO;

    class Program
    {
        private static ServiceBusClient _client;
        private static ServiceBusProcessor _processor;

        private static TaskCompletionSource<int> _taskCompletionSource;
        private static Task<int> _loopTask;
        private static int _completeCounter = 0;
        private static int _returnCounter = 0;
        private static int _messageCount = 0;
        private static int _delay = 0;

        private static IDictionary<string, ServiceBusSender> _senders = new Dictionary<string, ServiceBusSender>();

        static async Task Main(string[] args)
        {
            var root = Directory.GetCurrentDirectory();
            var dotenv = Path.Combine(root, ".env");
            DotEnv.Load(dotenv);

            string env;
            env = "PRODUCTION_CONNECTION_STRING";
            // env = "QA_CONNECTION_STRING";
            // env = "DEV_CONNECTION_STRING";
            var connectionString = Environment.GetEnvironmentVariable(env);

            // Connect to error queue
            _client = new ServiceBusClient(connectionString);

            // await MainAsync();

            // Switch to this to move deadletter back to the error queue
            await MoveDeadletter();
        }

        static async Task MainAsync()
        {
            ServiceBusProcessorOptions options = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 1,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
            };

            _processor = _client.CreateProcessor("error", options);

            _processor.ProcessMessageAsync += ProcessMessagesAsync;
            _processor.ProcessErrorAsync += ExceptionReceivedHandler;
            await  _processor.StopProcessingAsync();

            _taskCompletionSource = new TaskCompletionSource<int>();
            _loopTask = _taskCompletionSource.Task;

            await _processor.StartProcessingAsync();

            await _loopTask;

            Console.WriteLine("Task finished");

            Console.ReadLine();
        }

        private static async Task ProcessMessagesAsync(ProcessMessageEventArgs args)
        {
            var logFilePath = @"C:\Users\agallacher\OneDrive - Edrington\Documents\Support\bot-attack-february-2024";
            var logFilename = Path.Combine(logFilePath, "Log.txt");
            var errorFilename = Path.Combine(logFilePath, "Errors.txt");

            ServiceBusReceivedMessage message = args.Message;
            var body = Encoding.UTF8.GetString(message.Body);
            JObject json;

            try
            {
                json = JObject.Parse(body);
            }
            catch (Exception)
            {
                File.AppendAllText(errorFilename, args.Message.Subject);
                File.AppendAllText(errorFilename, "\n");
                throw;
            } 

            _messageCount = Interlocked.Increment(ref _messageCount);

            Console.WriteLine($"Processing {_messageCount}: {message.MessageId}");

            if (message.ApplicationProperties.TryGetValue("rbs2-msg-type", out object typeValue) == false)
            {
                Console.WriteLine("Skipping message as unable to get message type");
                return;
            }

            string type = typeValue.ToString();

            // SynchroniseConsentPreferences
            if (type == "Edrington.Data.Consumer.Commands.SynchroniseConsentPreferences, Edrington.Data")
            {
                if (message.ContainsError("Unable to find consumer for consent manager id"))
                {
                    await CompleteMessage(args);
                    return;
                }
            }

            // ShopProductIdentityUpserted
            if (type == "Edrington.Contracts.Ecommerce.Events.ShopProductIdentityUpserted, Edrington.Contracts.Ecommerce")
            {
                var brand = json.GetValue("brand").Value<int>();

                if (brand == 1)
                {
                    // Don't need this for Macallan any more 
                    await CompleteMessage(args);
                    return;
                }
            }

            // CreateSkeletonContact
            if (type == "Edrington.Data.Consumer.Commands.CreateSkeletonContact, Edrington.Data")
            {
                var brand = json.GetValue("brand").Value<int>();

                if (brand == 1)
                {
                    await CompleteMessage(args);
                    return;
                }

                if (message.ContainsError("Invalid email for contact"))
                {
                    await CompleteMessage(args);
                    return;
                }
            }

            // DeleteConsentManagementRecord
            if (type == "Edrington.Data.Consumer.Commands.DeleteConsentManagementRecord, Edrington.Data")
            {
                if (message.ContainsError("Status Code: GatewayTimeout"))
                {
                    await ReturnToSource(args, _delay);
                    _delay++;
                    return;
                }
            }

            // ProductInterestRegistered (bots) 
            if (type == "Edrington.Contracts.Ecommerce.Events.ProductInterestRegistered, Edrington.Contracts.Ecommerce")
            {
                var botScore = json.GetValue("BotScore").Value<double>();
                if (botScore < 0.4)
                {
                    await CompleteMessage(args);
                    return;
                }

                await ReturnToSource(args, _delay);
                _delay++;
                return;
            }

            // AuthenticationUserDeleted - identity deleted
            if (type == "Edrington.Data.Authentication.Events.AuthenticationUserDeleted, Edrington.Data")
            {
                if (message.ContainsError("Status (404)") && message.ContainsError("_DELETED_"))
                {
                    // delete
                    await CompleteMessage(args);
                    return;
                }

                await ReturnToSource(args, _delay);
                _delay++;
                return;
            }

            // 429s - let's try again
            if (message.ContainsError("Status (429) Too Many Requests"))
            {
                await ReturnToSource(args, _delay);
                _delay++;
                return;
            }

            // These are the messages we are going to remove from the queue without doing anything
            string[] completeTypes =
            {
                "Edrington.Data.CrmBridge.Commands.SyncEventDeal, Edrington.Data",
                "Edrington.Data.CrmBridge.Commands.SyncPurchase, Edrington.Data",
                "Edrington.Data.CrmBridge.Commands.SyncPurchases, Edrington.Data"
            };

            if (completeTypes.Contains(type))
            {
                await CompleteMessage(args);
                return;
            }

            // These are the messages we are going to try to replay 
            string[] returnTypes =
            {
                "Edrington.Data.Consumer.Commands.AccountSignUpForSubscriberRequested, Edrington.Data",
                "Edrington.Data.Consumer.Commands.MarkConsumerEmailVerified, Edrington.Data",
                "Edrington.Data.Consumer.Events.ConsumerProfileUpdated, Edrington.Data",
                "Edrington.Data.Consumer.Events.ConsumerVerifiedEmail, Edrington.Data",
                "Edrington.Data.ProductInventoryAlert.Events.PurchaseObjectInserted, Edrington.Data"
            };

            if (returnTypes.Contains(type))
            {
                await ReturnToSource(args, _delay);
                _delay += 5;
                return;
            }

            // Capture 404 for... 
            if (type == "Edrington.Data.Order.Events.OrderRefreshFromShopDownloadedV2, Edrington.Data")
            {
                if (message.ContainsError("Request to the Product API v2 failed with unsuccessful status code 404"))
                {
                    var errorMessage = message.GetErrorMessage();
                    var startOfInterestingBit = errorMessage.IndexOf("Url: https://api.edrington.com");
                    var endOfInterestingBit = errorMessage.IndexOf(")");
                    var interestingBit = errorMessage.Substring(startOfInterestingBit, endOfInterestingBit - startOfInterestingBit + 1);

                    File.AppendAllText(logFilename, interestingBit);
                    File.AppendAllText(logFilename, "\n");
                }
            }

            // And we move anything else to DLQ
            await args.DeadLetterMessageAsync(args.Message);
            return;
        }

        static async Task MoveDeadletter()
        {
            ServiceBusProcessorOptions options = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 1, // ReturnToQueue code does not support concurrent calls yet.
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                SubQueue = SubQueue.DeadLetter
            };

            string queueName = "error";

            _processor = _client.CreateProcessor(queueName, options);

            _processor.ProcessMessageAsync += args => ReturnDeadletterAsync(args, queueName);
            _processor.ProcessErrorAsync += ExceptionReceivedHandler;

            _taskCompletionSource = new TaskCompletionSource<int>();
            _loopTask = _taskCompletionSource.Task;

            await _processor.StartProcessingAsync();

            await _loopTask;
        }

        private static async Task ReturnDeadletterAsync(ProcessMessageEventArgs args, string queueName)
        {
            ServiceBusReceivedMessage message = args.Message;

            _messageCount = Interlocked.Increment(ref _messageCount);

            Console.WriteLine($"Processing {_messageCount}: {message.MessageId}");

            //if (message.ApplicationProperties.TryGetValue("rbs2-msg-type", out object typeValue) == false)
            //{
            //    Console.WriteLine("Skipping message as unable to get message type");
            //    return;
            //}

            //string type = typeValue.ToString();

            var clone = new ServiceBusMessage(args.Message);

            await ReturnToQueue(clone, queueName, 0);
            await CompleteMessage(args);
        }

        private static async Task CompleteMessage(ProcessMessageEventArgs args)
        {
            await args.CompleteMessageAsync(args.Message);

            _completeCounter++;

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{_completeCounter} messages completed");
            Console.ResetColor();
        }

        private static async Task ReturnToSource(ProcessMessageEventArgs args)
        {
            await ReturnToSource(args, 0);
        }

        private static async Task ReturnToSource(ProcessMessageEventArgs args, int delay)
        {
            string source = GetSource(args);

            if (string.IsNullOrEmpty(source))
            {
                Console.WriteLine("Message does not have a source queue property");
                return;
            }

            var copy = new ServiceBusMessage(args.Message);

            await ReturnToQueue(copy, source, delay);

            await CompleteMessage(args);
        }

        private static string GetSource(ProcessMessageEventArgs args)
        {
            if (args.Message.ApplicationProperties.TryGetValue("rbs2-source-queue", out object sourceValue))
            {
                return sourceValue.ToString();
            }

            return string.Empty;
        }

        private static async Task ReturnToQueue(ServiceBusMessage message, string queueName, int delay)
        {
            ServiceBusSender sender;

            if (_senders.ContainsKey(queueName))
            {
                sender = _senders[queueName];
            }
            else
            {
                sender = _client.CreateSender(queueName);

                _senders.Add(queueName, sender);
            }

            if (delay > 0)
            {
                await sender.ScheduleMessageAsync(message, DateTimeOffset.UtcNow.AddSeconds(delay));
            }
            else
            {
                await sender.SendMessageAsync(message);
            }

            _returnCounter++;

            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Console.WriteLine($"{_returnCounter} messages returned");
            Console.ResetColor();
        }

        static Task ExceptionReceivedHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine($"Message handler encountered an exception {args.Exception}.");
            return Task.CompletedTask;
        }
    }

    public static class MessageExtensions
    {
        const string errorKey = "rbs2-error-details";

        public static bool ContainsError(this ServiceBusReceivedMessage message, string error)
        {
            if (message.ApplicationProperties.ContainsKey(errorKey))
            {
                string errorMessage = message.ApplicationProperties[errorKey].ToString();

                return errorMessage.Contains(error);
            }

            return false;
        }

        public static string GetErrorMessage(this ServiceBusReceivedMessage message)
        {
            if (message.ApplicationProperties.ContainsKey(errorKey))
            {
                return message.ApplicationProperties[errorKey].ToString();
            }

            return string.Empty;
        }

        public static string BodyAsString(this ServiceBusReceivedMessage message)
        {
            return Encoding.UTF8.GetString(message.Body);
        }

        public static dynamic BodyAsDynamic(this ServiceBusReceivedMessage message)
        {
            string body = message.BodyAsString();

            dynamic msg = JObject.Parse(body);

            return msg;
        }
    }
}
