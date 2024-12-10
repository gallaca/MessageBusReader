using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace MessageBusReader.QueueProcessingHandlers;

public class AcademyProcessing
{
    private static int _messageCount = 0;
    private static List<string> _foundTypes = new List<string>();
    private static int _delay = 0;

    public static async Task ProcessAcademyDeadletterAsync(ProcessMessageEventArgs args)
    {
        ServiceBusReceivedMessage message = args.Message;

        _messageCount = Interlocked.Increment(ref _messageCount);

        Console.WriteLine($"Processing {_messageCount}: {message.MessageId}");

        if (message.ApplicationProperties.TryGetValue("rbs2-msg-type", out object typeValue) == false)
        {
            Console.WriteLine("Unable to get message type - Moving to deadletter");
            await args.DeadLetterMessageAsync(args.Message);
            return;
        }

        var academyTypes = new[]
        {
            "Edrington.Academy.Contracts.Events.AcademyInteracted, Edrington.Academy.Contracts",
            "Edrington.Academy.Contracts.Events.AcademyCourseInteracted, Edrington.Academy.Contracts",
        };
        string type = typeValue.ToString();

        // Academy class/chapter create race condition.
        if (!academyTypes.Contains(type))
        {
            Console.WriteLine($"Type {type} is irrelevant - Moving to deadletter");
            await args.DeadLetterMessageAsync(args.Message);
            return;
        }

        return;
    }


    public static async Task ProcessCheckTypesAsync(ProcessMessageEventArgs args)
    {
        ServiceBusReceivedMessage message = args.Message;

        _messageCount = Interlocked.Increment(ref _messageCount);

        // Console.WriteLine($"Processing {_messageCount}: {message.MessageId}");

        if (message.ApplicationProperties.TryGetValue("rbs2-msg-type", out object typeValue) == false)
        {
            // Console.WriteLine("Unable to get message type - Moving to deadletter");
            await args.DeadLetterMessageAsync(args.Message);
            return;
        }

        string type = typeValue.ToString();

        if (_foundTypes.Contains(type))
        {
            return;
        }

        _foundTypes.Add(type);
        Console.WriteLine($"Found new type {type}");
        return;
    }

    public static async Task ProcessAcademyMessagesAsync(ProcessMessageEventArgs args)
    {
        ServiceBusReceivedMessage message = args.Message;

        _messageCount = Interlocked.Increment(ref _messageCount);

        Console.WriteLine($"Processing {_messageCount}: {message.MessageId}");

        if (message.ApplicationProperties.TryGetValue("rbs2-msg-type", out object typeValue) == false)
        {
            return;
        }

        var academyTypes = new[]
        {
            "Edrington.Academy.Contracts.Events.AcademyInteracted, Edrington.Academy.Contracts",
            "Edrington.Academy.Contracts.Events.AcademyCourseInteracted, Edrington.Academy.Contracts",
        };
        string type = typeValue.ToString();

        if (!academyTypes.Contains(type))
        {
            return;
        }

        if (message.ContainsError("VALIDATION_ERROR"))
        {
            Console.WriteLine($"Found validation error: {message}");
            await Program.ReturnToSource(args, _delay);
            _delay++;
            return;
        }

        if (message.ContainsError("Object reference not set to an instance of an object"))
        {
            Console.WriteLine($"Found missing user error: {message}");
            await Program.ReturnToSource(args, _delay);
            _delay++;
            return;
        }

        Console.WriteLine("Error message:");
        Console.WriteLine(message.GetErrorMessage());

        var removeMessageIds = new[]
        {
            "d9d0141a-f997-468c-831d-a4c155e74b64",
            "a72de5ea-4bd1-49e7-9463-0166ce04479e",
            "2354c8c2-c86d-495d-8a97-520990e1631e",
        };
        if (removeMessageIds.Contains(message.MessageId))
        {
            Console.WriteLine($"Removing message.");
            await Program.CompleteMessage(args);
            return;
        }

        // Console.WriteLine($"Not processed message: {message}");
        return;
    }
}
