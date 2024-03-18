namespace MessageBusReader
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Microsoft.Azure.ServiceBus;
    using System.Text.Unicode;
    using System.Net.Mail;

    public class Program
    {
        private static QueueClient _client;
        private static TaskCompletionSource<int> _taskCompletionSource;
        private static Task<int> _loopTask;
        private static int _counter = 0;
        private static int _messageCount = 0;
        private static TimeSpan _delay = TimeSpan.FromSeconds(0);
        private static IDictionary<string, int> _typeCounter = new Dictionary<string, int>();
        private static long _sequenceNumber = 0;
        private static string _connectionString;
        private static Dictionary<string, Func<Message, Task>> _actions;
        // --- private static string _logFilename;
        private static IHelper _helper;

        private static DateTime _startDate = DateTime.UtcNow;

        static void Main(string[] args)
        {
            var root = Directory.GetCurrentDirectory();
            var dotenv = Path.Combine(root, ".env");
            DotEnv.Load(dotenv);

            // await InitLogger();
            // ---     _logFilename = Path.Combine(root, $"{DateTime.UtcNow:yyyymmdd-HHmmss}.log");

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

            // Helper
            var databaseConnectionString = Environment.GetEnvironmentVariable("DATABASE_CONNECTION_STRING");
            _helper = new Helper(databaseConnectionString);

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

            string[] types =
            {
                //"Edrington.Data.Authentication.Commands.AdminDeleteAuthenticationUser, Edrington.Data",
                //"Edrington.Data.Authentication.Events.AuthenticationUserDeleted, Edrington.Data",
                "Edrington.Data.Consumer.Commands.AccountSignUpForSubscriberRequested, Edrington.Data",
                //"Edrington.Data.Consumer.Commands.AssignConsumerRelationshipManager, Edrington.Data",
                "Edrington.Data.Consumer.Commands.CreateSkeletonContact, Edrington.Data",
                //"Edrington.Data.Consumer.Commands.EnterBallot, Edrington.Data",
                "Edrington.Data.Consumer.Commands.SendPasswordResetEmail, Edrington.Data",
                //"Edrington.Data.Consumer.Commands.UpdateCommunicationPreferences, Edrington.Data",
                //"Edrington.Data.Consumer.Events.ConsumerConsentPreferencesUpdated, Edrington.Data",
                "Edrington.Data.Consumer.Events.ConsumerCrmPreferencesUpdated, Edrington.Data",
                //"Edrington.Data.Consumer.Commands.DeleteConsentManagementRecord, Edrington.Data",
                "Edrington.Data.Consumer.Events.ConsumerProfileUpdated, Edrington.Data",
                "Edrington.Data.Consumer.Events.ConsumerVerifiedEmail, Edrington.Data",
                //"Edrington.Data.Consumer.Events.ProductEnquiryMade, Edrington.Data",
                //"Edrington.Data.Consumer.Events.SignUpConsumerAccountIdentityCreated, Edrington.Data",
                //"Edrington.Data.Consumer.Events.SignUpConsumerAccountRequestExpired, Edrington.Data",
                "Edrington.Data.CrmBridge.Commands.SyncEventDeal, Edrington.Data",
                "Edrington.Data.CrmBridge.Commands.SyncPurchase, Edrington.Data",
                "Edrington.Data.CrmBridge.Commands.SyncPurchases, Edrington.Data",
                //"Edrington.Data.MakeTheCut.Events.MakeTheCutAnswersUpdated, Edrington.Data",
                //"Edrington.Data.Consumer.Commands.SendNewsletterSubscriptionRequested, Edrington.Data",
                "Edrington.Data.Consumer.Commands.SignUpConsumerAccountRequested, Edrington.Data",
                //"Edrington.Data.Consumer.Commands.SynchroniseCrmPreferences, Edrington.Data",
                //"Edrington.Data.Consumer.Commands.SynchroniseConsentPreferences, Edrington.Data",
                //"Edrington.Data.Crm.Commands.AssociateConsentWithCrmAccount, Edrington.Data",
                //"Edrington.Data.Crm.Commands.CreateCrmContact, Edrington.Data",
                "Edrington.Data.Crm.Commands.SendConsentManagerIdToCrm, Edrington.Data",
                //"Edrington.Data.Crm.Events.ConsentAssociatedWithCrmAccount, Edrington.Data",                
                "Edrington.Contracts.Ecommerce.Events.ProductInterestRegistered, Edrington.Contracts.Ecommerce",
                //"Edrington.Contracts.Ecommerce.Events.ProductUpserted, Edrington.Contracts.Ecommerce",
                //"Edrington.Data.Order.Events.OrderRefreshFromShopDownloadedV2, Edrington.Data",
                "Edrington.ProviderService.CrmBridge.Commands.UpdateCrmProductRecord, Edrington.ProviderService.CrmBridge",
                "Edrington.ProviderService.CrmBridge.Commands.UpdateMplConnectedProperty, Edrington.ProviderService.CrmBridge",
                "##DUMMY##"
            };

            var logFilePath = @"C:\Users\agallacher\OneDrive - Edrington\Documents\Support\bot-attack-february-2024";
            var droppedSignUpConsumerAccountRequestedFilename = Path.Combine(logFilePath, "SignUpConsumerAccountRequested.csv");
            var droppedSendConsentManagerIdToCrmFilename = Path.Combine(logFilePath, "SendConsentManagerIdToCrm.csv");
            var droppedProductInterestRegisteredFilename = Path.Combine(logFilePath, "ProductInterestRegistered.csv");
            var logFilename = Path.Combine(logFilePath, "Log.txt");

            var body = Encoding.UTF8.GetString(message.Body);
            var json = JObject.Parse(body);
            var brand = json.GetValue("Brand").Value<int>();
            // var atUtc = json.GetValue("AtUtc")?.Value<DateTime>() ?? null;
            var entityId = json.GetValue("EntityId")?.Value<string>() ?? null;
            var errorMessage = message.UserProperties["rbs2-error-details"] as string;

            File.AppendAllText(logFilename, message.MessageId + " - " + type);
            File.AppendAllText(logFilename, "\n");

            if (errorMessage.Contains("Status (429) Too Many Requests for"))
            {
                _delay += TimeSpan.FromMilliseconds(250);
                Console.WriteLine($"Return message {message.MessageId} to source with {_delay} delay for {entityId} - original error was '(429) Too many requests'");
                await ReturnToSource(message, _delay);
            }
            else
            {
                if (types.Length == 0 || types.Contains(type))
                {
                    // "Sync" events that don't need to be replayed
                    if (type == "Edrington.Data.CrmBridge.Commands.SyncEventDeal, Edrington.Data" ||
                        type == "Edrington.Data.CrmBridge.Commands.SyncPurchase, Edrington.Data" ||
                        type == "Edrington.Data.CrmBridge.Commands.SyncPurchases, Edrington.Data")
                    {                        
                        Console.WriteLine($"## DELETING: {type}");
                        await _client.CompleteAsync(message.SystemProperties.LockToken);
                    }

                    // Replay all of these 
                    if (type == "Edrington.Data.Consumer.Events.ConsumerProfileUpdated, Edrington.Data" ||
                        type == "Edrington.Data.Consumer.Commands.AccountSignUpForSubscriberRequested, Edrington.Data" ||
                        type == "Edrington.Data.Consumer.Events.ConsumerVerifiedEmail, Edrington.Data")
                    {
                        Console.WriteLine($"Return '{type}' message {message.MessageId} to source for {entityId} and brand {brand}");
                        await ReturnToSource(message);
                    }

                    if (type == "Edrington.Data.Crm.Commands.SendConsentManagerIdToCrm, Edrington.Data")
                    {
                        // Log...
                        var consentManagerId = json.GetValue("ConsentManagerId")?.Value<string>() ?? throw new InvalidOperationException("Can't get consent manager id");
                        var details = string.Join(',', new List<string> { brand.ToString(), entityId, consentManagerId });
                        File.AppendAllText(droppedSendConsentManagerIdToCrmFilename, details);
                        File.AppendAllText(droppedSendConsentManagerIdToCrmFilename, "\n");

                        // and drop
                        Console.WriteLine($"## DELETING: SendConsentManagerIdToCrm for '{details}'");
                        await _client.CompleteAsync(message.SystemProperties.LockToken);
                    }

                    if (type == "Edrington.Data.Consumer.Commands.SignUpConsumerAccountRequested, Edrington.Data")
                    { 
                        if (errorMessage.Contains("Cannot insert duplicate key in object 'Consumer.ConsumerIdentity'"))
                        {
                            Console.WriteLine($"## DELETING: SignUpConsumerAccountRequested for '{entityId}'");
                            await _client.CompleteAsync(message.SystemProperties.LockToken);
                        }
                        else
                        {
                            var auth0Id = json.GetValue("Auth0Id").Value<string>();
                            var emailAddress = json.GetValue("EmailAddress").Value<string>();
                            var firstName = json.GetValue("GivenName").Value<string>();
                            var lastName = json.GetValue("FamilyName").Value<string>();
                            var botScore = json.GetValue("BotScore").Value<double>();

                            // Force bot score to be negative if we want to drop message
                            if (botScore > 0)
                            {
                                if (botScore < 0.4 || (emailAddress.EndsWith("@releasepk.com") && firstName.Equals("Lenora") && lastName.Equals("Bengel")))
                                {
                                    botScore *= -1;

                                    Console.WriteLine("Patch in negative bot score");
                                    var botScoreProperty = json.Property("BotScore");
                                    botScoreProperty.Value = JContainer.FromObject(botScore);
                                    message.Body = UTF8Encoding.UTF8.GetBytes(json.ToString(Formatting.None));
                                }
                            }

                            // Log any message that should be dropped by endpoint - remember we need to delete the auth0 account 
                            if (botScore < 0.1)
                            {
                                var details = string.Join(',', new List<string> { brand.ToString(), auth0Id, emailAddress, firstName, lastName, botScore.ToString() });
                                File.AppendAllText(droppedSignUpConsumerAccountRequestedFilename, details);
                                File.AppendAllText(droppedSignUpConsumerAccountRequestedFilename, "\n");
                            }

                            // Replay 
                            Console.WriteLine($"Return 'SignUpConsumerAccountRequested' message {message.MessageId} to source for {entityId} and brand {brand}");
                            await ReturnToSource(message);
                        }
                    }

                    if (type == "Edrington.Data.Consumer.Commands.SendPasswordResetEmail, Edrington.Data")
                    {
                        Console.WriteLine($"Return 'SendPasswordResetEmail' message {message.MessageId} to source for {entityId} and brand {brand}");
                        await ReturnToSource(message);
                    }

                    // 
                    if (type == "Edrington.ProviderService.CrmBridge.Commands.UpdateMplConnectedProperty, Edrington.ProviderService.CrmBridge")
                    {
                        var code = json.GetValue("Code").Value<string>();
                        Console.WriteLine($"## DELETING: UpdateMplConnectedProperty for '{code}'");
                        await _client.CompleteAsync(message.SystemProperties.LockToken);
                    }

                    // 
                    if (type == "Edrington.ProviderService.CrmBridge.Commands.UpdateCrmProductRecord, Edrington.ProviderService.CrmBridge")
                    {
                        var code = json.GetValue("Code").Value<string>();
                        Console.WriteLine($"## DELETING: UpdateCrmProductRecord for '{code}'");
                        await _client.CompleteAsync(message.SystemProperties.LockToken);
                    }

                    // Dec 2023 bot attack remediation
                    if (type == "Edrington.Data.Authentication.Commands.AdminDeleteAuthenticationUser, Edrington.Data")
                    {
                        Console.WriteLine($"Return 'AdminDeleteAuthenticationUser' message {message.MessageId} to source for {entityId} and brand {brand}");
                        await ReturnToSource(message);
                    }

                    if (type == "Edrington.Data.Consumer.Commands.SendNewsletterSubscriptionRequested, Edrington.Data")
                    {
                        var emailAddress = json.GetValue("EntityId").Value<string>().ToLower();
                        var firstName = json.SelectToken("MarketingMailUserSubscriptionDto.FirstName").Value<string>().ToLower();
                        var lastName = json.SelectToken("MarketingMailUserSubscriptionDto.LastName").Value<string>().ToLower();

                        if (emailAddress.EndsWith("@outlook.com") && !emailAddress.Contains(firstName) && !emailAddress.Contains(lastName))
                        {
                            Console.WriteLine($"## DELETING: SendNewsletterSubscriptionRequested for {emailAddress}, {firstName}, {lastName}");
                            await _client.CompleteAsync(message.SystemProperties.LockToken);
                        }
                        else
                        {
                            Console.WriteLine($"Return 'SendNewsletterSubscriptionRequested' message {message.MessageId} to source for {entityId} and brand {brand}");
                            await ReturnToSource(message);
                        }
                    }

                    /* ---------- PREVIOUS STUFF ---------- */

                    // A. ProductEnquiryMade - Delete all old Register Interest 
                    if (type == "Edrington.Data.Consumer.Events.ProductEnquiryMade, Edrington.Data")
                    {
                        Console.WriteLine($"## DELETING: ProductEnquiryMade for {entityId} and product ##");
                        await _client.CompleteAsync(message.SystemProperties.LockToken);
                    }

                    //// B. Register Interest 
                    //if (type == "Edrington.Contracts.Ecommerce.Events.ProductInterestRegistered, Edrington.Contracts.Ecommerce")
                    //{
                    //    Console.WriteLine($"Return 'Register Interest' message {message.MessageId} to source for {entityId} and brand {brand}");
                    //    await ReturnToSource(message);
                    //}

                    // C. ProductUpserted - patch code in to blank name
                    if (type == "Edrington.Contracts.Ecommerce.Events.ProductUpserted, Edrington.Contracts.Ecommerce")
                    {
                        if (string.IsNullOrWhiteSpace(json.GetValue("ProductDisplayName").Value<string>()))
                        {
                            Console.WriteLine("Patch in product code for blank display name");
                            string productCode = json.GetValue("ProductCode").Value<string>();
                            var productDisplayNameProperty = json.Property("ProductDisplayName");
                            productDisplayNameProperty.Value = JContainer.FromObject(productCode);
                            message.Body = UTF8Encoding.UTF8.GetBytes(json.ToString(Formatting.None));
                        }

                        Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");
                        await ReturnToSource(message);
                    }

                    // SynchroniseConsentPreferences
                    if (type == "Edrington.Data.Consumer.Commands.SynchroniseConsentPreferences, Edrington.Data")
                    {
                        try
                        {
                            string consumerId = await _helper.GetConsumerId(brand, 9, entityId);
                            Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");
                            await ReturnToSource(message);

                        }
                        catch (ConsumerNotFoundException)
                        {
                            Console.WriteLine($"## DELETING: SynchroniseConsentPreferences message with invalid consent manager id ##");
                            await _client.CompleteAsync(message.SystemProperties.LockToken);
                        }
                    }

                    // PCRM
                    if (type == "Edrington.Data.Consumer.Commands.AssignConsumerRelationshipManager, Edrington.Data")
                    {
                        if (errorMessage.Contains("Status (429) Too Many Requests for"))
                        {
                            Console.WriteLine($"Return message {message.MessageId} to source for {entityId} - original error was '(429) Too many requests'");
                            await ReturnToSource(message);
                        }
                        else
                        {
                            Log(message);
                        }
                    }

                    // ConsumerCrmPreferencesUpdated
                    if (type == "Edrington.Data.Consumer.Events.ConsumerCrmPreferencesUpdated, Edrington.Data")
                    {
                        if (errorMessage.Contains("Violation of UNIQUE KEY constraint 'CK_BrandLocalIdentity_Id'"))
                        {
                            if (entityId.Contains("@"))
                            {
                                string consumerId = await _helper.GetConsumerId(brand, 1, entityId);
                                var entityIdProperty = json.Property("EntityId");
                                entityIdProperty.Value = JContainer.FromObject(consumerId);
                                message.Body = UTF8Encoding.UTF8.GetBytes(json.ToString(Formatting.None));
                                await ReturnToSource(message);
                            }
                            else
                            {
                                Log(message);
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");
                            await ReturnToSource(message);
                        }
                    }

                    // DeleteConsentManagementRecord
                    if (type == "Edrington.Data.Consumer.Commands.DeleteConsentManagementRecord, Edrington.Data")
                    {
                        if (string.IsNullOrEmpty(entityId))
                        {
                            Console.WriteLine($"## DELETING: DeleteConsentManagementRecord message with no entity id ##");
                            await _client.CompleteAsync(message.SystemProperties.LockToken);
                        }
                        else
                        {
                            Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");
                            await ReturnToSource(message);
                        }
                    }

                    // 1. AuthenticationUserDeleted
                    if (type == "Edrington.Data.Authentication.Events.AuthenticationUserDeleted, Edrington.Data")
                    {
                        if (errorMessage.Contains("System.AggregateException: 5 unhandled exceptions (Status (404) Not Found for PATCH https://api.hubapi.com/crm/v3/objects/contact") && errorMessage.Contains("_DELETED_"))
                        {
                            Console.WriteLine($"## DELETING: Consumer identity has been invalidated for {entityId} ##");
                            await _client.CompleteAsync(message.SystemProperties.LockToken);
                        }
                        else
                        {
                            Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");
                            await ReturnToSource(message);
                        }
                    }

                    // 2. Entity id is Consumer ID => delete if bad consumer id, otherwise replay
                    //   - ConsumerConsentPreferencesUpdated
                    //   - UpdateCommunicationPreferences
                    if (type == "Edrington.Data.Consumer.Events.ConsumerConsentPreferencesUpdated, Edrington.Data"
                        
                        || type == "Edrington.Data.Consumer.Commands.UpdateCommunicationPreferences, Edrington.Data"
                        )
                    {
                        if (await _helper.IsInvalidConsumer(entityId))
                        {
                            Console.WriteLine($"## DELETING: Invalid consumer id: {entityId} ##");
                            await _client.CompleteAsync(message.SystemProperties.LockToken);
                        }
                        else if (errorMessage.Contains("Violation of UNIQUE KEY constraint 'CK_BrandLocalIdentity_Id'"))
                        {
                            Log(message);
                        }
                        else
                        {
                            Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");
                            await ReturnToSource(message);
                        }
                    }

                    // 3. CreateSkeletonContact => just delete if Macallan, otherwise replay
                    if (type == "Edrington.Data.Consumer.Commands.CreateSkeletonContact, Edrington.Data")
                    {
                        if (brand == 1)
                        {
                            Console.WriteLine($"## DELETING: CreateSkeletonContact for {entityId} not required for Macallan ##");
                            await _client.CompleteAsync(message.SystemProperties.LockToken);
                        }
                        else
                        {
                            Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");
                            await ReturnToSource(message);
                        }
                    }


                    /* ---------- EXISTING STUFF ---------- */

                    //if (type == "Edrington.Data.Consumer.Events.ConsumerVerifiedEmail, Edrington.Data")
                    //{
                    //    Log($"{type}, {message.MessageId}, {atUtc}, {entityId}, {brand}, {DateTime.UtcNow}");
                    //    Console.WriteLine("******");

                    //    var sourceQueue = message.UserProperties["rbs2-source-queue"] as string;

                    //    if (sourceQueue == "marketingemailprovider")
                    //    {
                    //        Console.WriteLine($"Send message {message.MessageId} to `crmprovider` for {entityId} and brand {brand}");
                    //        await RedirectToQueue(message, "crmprovider");
                    //    }
                    //    else
                    //    {
                    //        Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");
                    //        await ReturnToSource(message);
                    //    }

                    //    await Task.Delay(50, token);
                    //}

                    if (type == "Edrington.Data.MakeTheCut.Events.MakeTheCutAnswersUpdated, Edrington.Data")
                    {
                        var country = json.SelectToken("UpdateDto.Answers.mmj_language_country")?.Value<string>() ?? string.Empty;
                        var how_much_have_you_explored_the_world_of_whisky = json.SelectToken("UpdateDto.Answers.how_much_have_you_explored_the_world_of_whisky_")?.Value<string>() ?? string.Empty;
                        var whiskies_owned = json.SelectToken("UpdateDto.Answers.whiskies_owned")?.Value<string>() ?? string.Empty;
                        var whiskies_purchased_regularly = json.SelectToken("UpdateDto.Answers.whiskies_purchased_regularly")?.Value<string>() ?? string.Empty;
                        var do_people_ask_your_advice_about_whisky = json.SelectToken("UpdateDto.Answers.do_people_ask_your_advice_about_whisky_")?.Value<string>() ?? string.Empty;
                        var how_often_do_you_buy_the_macallan_from_a_shop = json.SelectToken("UpdateDto.Answers.how_often_do_you_buy_the_macallan_from_a_shop_")?.Value<string>() ?? string.Empty;

                        if (brand == 1 && (country == "de-de" || country == "en-gb"))
                        {
                            if (how_much_have_you_explored_the_world_of_whisky == "male" ||
                                how_much_have_you_explored_the_world_of_whisky == "female" ||
                                whiskies_owned == "['MAC400','MAC031','MAC032',]" ||
                                whiskies_owned == "['MAC189', 'MAC042', 'MAC311']" ||
                                whiskies_purchased_regularly == "['MAC213','MAC318','MAC034',]" ||
                                do_people_ask_your_advice_about_whisky == "no_developing" ||
                                do_people_ask_your_advice_about_whisky == "no_looking" ||
                                do_people_ask_your_advice_about_whisky == "yes_settled" ||
                                do_people_ask_your_advice_about_whisky == "yes_developed" ||
                                how_often_do_you_buy_the_macallan_from_a_shop == "neat" ||
                                how_often_do_you_buy_the_macallan_from_a_shop == "ice" ||
                                how_often_do_you_buy_the_macallan_from_a_shop == "mixed" ||
                                how_often_do_you_buy_the_macallan_from_a_shop == "mix" ||
                                how_often_do_you_buy_the_macallan_from_a_shop == "water")
                            {
                                // Save the message to the file system
                                var filename = Path.Combine(@"C:\Users\agallacher\OneDrive - Edrington\Documents\Support\invalid-bot-mtc-calls-4", $"{entityId}_{message.MessageId}.txt");
                                File.WriteAllText(filename, json.ToString(formatting: Formatting.Indented));

                                await _client.CompleteAsync(message.SystemProperties.LockToken);
                            }
                            else
                            {
                                Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");
                                await ReturnToSource(message);
                            }
                        }
                    }

                    if (type == "Edrington.Data.Consumer.Commands.EnterBallot, Edrington.Data")
                    {
                        var botScore = json.GetValue("BotScore").Value<double>();

                        if (errorMessage.Contains("Ballot criteria not met") || botScore < 0.3)
                        {
                            await _client.CompleteAsync(message.SystemProperties.LockToken);
                        }
                    }

                    //if (type == "Edrington.Data.Consumer.Events.ConsumerConsentPreferencesUpdated, Edrington.Data" ||
                    //     type == "Edrington.Data.Consumer.Events.ConsumerCrmPreferencesUpdated, Edrington.Data" ||
                    //     type == "Edrington.Data.Consumer.Commands.UpdateCommunicationPreferences, Edrington.Data")
                    //{
                    //    Log($"{type}, {message.MessageId}, {atUtc}, {entityId}, {brand}, {DateTime.UtcNow}");

                    //    Console.WriteLine("******");
                    //    Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");

                    //    await ReturnToSource(message);
                    //    await Task.Delay(100, token);
                    //}

                    // Delete this old message type 
                    if (type == "Edrington.Data.Consumer.Events.SignUpConsumerAccountRequestExpired, Edrington.Data")
                    {
                        await _client.CompleteAsync(message.SystemProperties.LockToken);
                        Console.WriteLine("## DELETED ##");
                    }

                    if (type == "Edrington.Data.Consumer.Events.SignUpConsumerAccountIdentityCreated, Edrington.Data")
                    {
                        if (errorMessage.Contains("The added or subtracted value results in an un-representable DateTime"))
                        {
                            await _client.CompleteAsync(message.SystemProperties.LockToken);
                            Console.WriteLine("## DELETED ##");
                        }
                    }

                    _counter++;
                }
            }

            //if (invalidMessageIds.Contains(message.MessageId))
            //{
            //    Console.WriteLine("******");
            //    Console.WriteLine("Deleting message...");
            //    await Task.Delay(1000, token);
            //    await CompleteMessage(message);
            //}
            //else if (types.Length == 0 || types.Contains(type))
            //{
            //    if (type == "Edrington.Data.Consumer.Commands.SendNewsletterSubscriptionRequested, Edrington.Data" && brand != 1)
            //    {
            //        Console.WriteLine("******");
            //        Console.WriteLine($"Skipping message {message.MessageId} to source for {entityId} and brand {brand}");
            //    }
            //    else
            //    {  
            //        Log($"{type}, {message.MessageId}, {atUtc}, {entityId}, {brand}, {DateTime.UtcNow}");

            //        Console.WriteLine("******");
            //        Console.WriteLine($"Return message {message.MessageId} to source for {entityId} and brand {brand}");

            //        // --- await CompleteMessage(message);
            //        await ReturnToSource(message);
            //        await Task.Delay(1000, token);

            //        Console.WriteLine("Done.");
            //        Console.WriteLine();
            //        _counter++;
            //    }
            //}
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

            await RedirectToQueue(message, (string)sourceValue);
        }

        private static async Task ReturnToSource(Message message, TimeSpan delay)
        {
            if (message.UserProperties.TryGetValue("rbs2-source-queue", out object sourceValue) == false)
            {
                Console.WriteLine("Message does not have a source queue property");
                return;
            }

            await ScheduleMessage(message, (string)sourceValue, delay);
        }

        private static async Task RedirectToQueue(Message message, string queueName)
        {
            var client = new QueueClient(_connectionString, queueName);

            var copy = message.Clone();

            await client.SendAsync(copy);

            await client.CloseAsync();

            await _client.CompleteAsync(message.SystemProperties.LockToken);
        }

        private static async Task ScheduleMessage(Message message, string queueName, TimeSpan delay)
        {
            var client = new QueueClient(_connectionString, queueName);

            var copy = message.Clone();

            var offset = new DateTimeOffset(DateTime.UtcNow.Add(delay));
            await client.ScheduleMessageAsync(copy, offset);

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

        private static void Log(Message message)
        {
            var body = Encoding.UTF8.GetString(message.Body);
            var json = JObject.Parse(body);

            var filename = Path.Combine(@"C:\Users\agallacher\OneDrive - Edrington\Documents\Support\error-queue", $"{message.MessageId}.txt");
            File.WriteAllText(filename, json.ToString(formatting: Formatting.Indented));

            File.AppendAllText(filename, "\n----------\n");

            var errorMessage = message.UserProperties["rbs2-error-details"] as string;
            File.AppendAllText(filename, errorMessage);
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
