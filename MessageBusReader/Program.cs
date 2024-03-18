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
            // Connect to error queue
            _client = new ServiceBusClient("## SERVICE BUS CONNECTION STRING ##");

            //await MainAsync();

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

            _taskCompletionSource = new TaskCompletionSource<int>();
            _loopTask = _taskCompletionSource.Task;

            await _processor.StartProcessingAsync();

            await _loopTask;

            Console.WriteLine("Task finished");

            Console.ReadLine();
        }

        private static async Task ProcessMessagesAsync(ProcessMessageEventArgs args)
        {
            ServiceBusReceivedMessage message = args.Message;

            _messageCount = Interlocked.Increment(ref _messageCount);

            Console.WriteLine($"Processing {_messageCount}: {message.MessageId}");

            if (message.ApplicationProperties.TryGetValue("rbs2-msg-type", out object typeValue) == false)
            {
                Console.WriteLine("Skipping message as unable to get message type");
                return;
            }

            string type = typeValue.ToString();

            // Handling based on error
            //if (type == "Edrington.Contracts.Ecommerce.Events.ProductUpserted, Edrington.Contracts.Ecommerce")
            //{
            //    if (message.ContainsError("Status (429)"))
            //    {
            //        await ReturnToSource(args, _delay);
            //        _delay++;
            //        return;
            //    }

            //    if (message.ContainsError("Status (400)"))
            //    {
            //        string error = message.GetErrorMessage();
            //        int start = error.IndexOf("For the property ");

            //        if (start < 1)
            //        {
            //            return;
            //        }

            //        int end = error.IndexOf("\"", start);
            //        string interestingPart = error.Substring(start, end - start);

            //        File.AppendAllText("hubspot_errors.txt", interestingPart + "\r\n");
            //        await CompleteMessage(args);
            //        return;
            //    }
            //}

            string[] completeTypes =
            {
                // "Edrington.Contracts.Ecommerce.Events.ProductUpserted, Edrington.Contracts.Ecommerce"
            };

            if (completeTypes.Contains(type))
            {
                await CompleteMessage(args);
                return;
            }

            string[] returnTypes =
            {
                // "Edrington.Contracts.Ecommerce.Events.ProductUpserted, Edrington.Contracts.Ecommerce"
            };

            if (returnTypes.Contains(type))
            {
                await ReturnToSource(args, _delay);
                _delay += 5;
                return;
            }

            string[] skipTypes = new string[]
            {
                // "Edrington.Data.Consumer.Commands.DeleteConsentManagementRecord, Edrington.Data",
                // "Edrington.Data.Consumer.Events.ConsumerCrmPreferencesUpdated, Edrington.Data"
            };

            if (skipTypes.Contains(type))
            {
                await args.DeadLetterMessageAsync(args.Message);
                return;
            }

            Console.WriteLine("Unhandled type: " + type);
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
