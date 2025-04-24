using Microsoft.Extensions.Configuration;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore;
using Serilog;
using Serilog.Sinks.PostgreSQL;
using StockIntegrity.Models;
using StockIntegrity.Helpers;

namespace StockIntegrity
{
    internal static class StockIntegrity
    {
        private static IConfigurationBuilder builder = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false);

        private static IConfigurationRoot config = builder.Build();

        [Required]
        private static readonly string APP_NAME = config["APP_NAME"];

        public static List<Company> companies = null;



        public static async Task Main(string[] args)
        {

            EncryptSensitiveConfigData();


            Log.Information("App Started");
            //Logging for DB setup
            var columnOptions = new Dictionary<string, ColumnWriterBase>
            {
                { "message", new RenderedMessageColumnWriter() },
                { "message_template", new MessageTemplateColumnWriter() },
                { "level", new LevelColumnWriter() },
                { "time_stamp", new TimestampColumnWriter() },
                { "exception", new ExceptionColumnWriter() },
                { "properties", new PropertiesColumnWriter() }
            };

            // Configure Serilog
            Log.Logger = new LoggerConfiguration()
                .Enrich.WithProperty("app_name", APP_NAME)
                .WriteTo.PostgreSQL(EncryptionHelper.Decrypt(config.GetConnectionString("LoggingConnection")), "logs", columnOptions, needAutoCreateTable: true)
                .CreateLogger();


            int count = 0;
            DateTime date = DateTime.UtcNow.Date;
            // Integrity will check from the past 5 years
            date = date.AddYears(-5);



            using (AppDbContext context = new AppDbContext(config))
            {
                try
                {
                    // Get company list to check integrity for
                    companies = context.Companies.ToList();
                }
                catch (Exception ex)
                {
                    Log.Fatal(ex, "Could not retrieve the companies list from the companies table.");
                }
            }

            Log.Information("Beginning to look at data integrity. Starting at Date: " + date.ToString());
            try
            {
                while (date < DateTime.UtcNow.Date.AddDays(-2))
                {
                    List<Task> runningTasks = new List<Task>();

                    // Schedule up to 6 tasks
                    for (int i = 0; i < 6; i++)
                    {
                        Console.WriteLine(date.ToString());
                        DateTime nextDate = GetNextValidDate(ref date, DateTime.UtcNow.Date.AddDays(-2));
                        if (nextDate == DateTime.MinValue)
                            break; // Stop scheduling if no more valid dates

                        Console.WriteLine($"Scheduled task for: {nextDate.ToShortDateString()}");

                        // Add the task to the list without starting it yet
                        runningTasks.Add(Task.Factory.StartNew(() => {
                            CheckDateDataIntegrity(nextDate.Date);
                            // Code to run in parallel
                        }, TaskCreationOptions.LongRunning));
                    }

                    // If no tasks were scheduled, exit loop
                    if (runningTasks.Count == 0)
                        break;

                    // Wait for all tasks to complete before continuing
                    await Task.WhenAll(runningTasks);
                }
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Unhandled Exception caused the app to crash.");
            }

            Log.CloseAndFlush();
            
        }

        static DateTime GetNextValidDate(ref DateTime date, DateTime today)
        {
            while (date < today)
            {
                DateTime validDate = date;
                date = date.AddDays(1); // Move forward

                if (validDate.DayOfWeek != DayOfWeek.Saturday && validDate.DayOfWeek != DayOfWeek.Sunday)
                {
                    return validDate; // Return the first valid weekday
                }
            }
            return DateTime.MinValue; // No more valid dates
        }

        // This function ensures only one record for each symbol each day exists
        // If there are more than one, it deletes the duplicates and adds new ones if necessary.
        public static async Task CheckDateDataIntegrity(DateTime date)
        {
            string getLastPriceURL = @"https://data.alpaca.markets/v2/stocks/bars?timeframe=1D&start=" + date.ToString("yyyy-MM-dd") + "&end=" + date.ToString("yyyy-MM-dd") + "&limit=1000&adjustment=raw&feed=iex&currency=USD&sort=asc&symbols=";
            string apiGetReq = getLastPriceURL;
            foreach (Company company in companies)
            {
                using (AppDbContext context = new AppDbContext(config))
                {
                    var records = context.DailyBars
                        .Where(r => r.Timestamp.Date == date.Date && r.Symbol.Trim() == company.Symbol.Trim())
                        .ToList();

                    if (records.Count == 1)
                    {
                        // Do nothing
                    }
                    else if (records.Count > 1)
                    {
                        // We need to delete until one remains as dupes were inserted
                        for (int i = 1; i < records.Count; i++)
                        {
                            context.DailyBars.Remove(records[i]);
                            context.SaveChanges();
                        }
                    }
                    else
                    {
                        //This makes sure we dont go over the 2048 character limit.
                        if (apiGetReq.Length >= 2043)
                        {
                            apiGetReq = apiGetReq.Substring(0, apiGetReq.Length - 1);
                            CallApiAndLoadDailyData(apiGetReq);
                            apiGetReq = getLastPriceURL;
                        }
                        else
                        {
                            apiGetReq += (company.Symbol + ",");
                        }
                    }
                }
                
            }
            apiGetReq = apiGetReq.Substring(0, apiGetReq.Length - 1);
            CallApiAndLoadDailyData(apiGetReq);

        }


        /***
         * This function encrypts the app config files upon startup.
         * 
         **/
        private static void EncryptSensitiveConfigData()
        {
            var keysToEncrypt = new[] { "ConnectionStrings:LoggingConnection", "ConnectionStrings:AppConnection", "API_KEY", "API_SECRET" };
            foreach (var key in keysToEncrypt)
            {
                string value = config[key];
                if (!EncryptionHelper.IsEncrypted(value))
                {
                    EncryptionHelper.UpdateConfigFile(EncryptionHelper.Encrypt(value), key);
                }
            }
            config.Reload();
        }

        /**
         * This function adds the required request headers to the HTTP Client to allow for api calls.
         * 
         */
        private static void ConfigureHTTPClient(HttpClient client)
        {
            client.DefaultRequestHeaders.Add("Accept", "application/json");
            client.DefaultRequestHeaders.Add("APCA-API-KEY-ID", EncryptionHelper.Decrypt(config["API_KEY"]));
            client.DefaultRequestHeaders.Add("APCA-API-SECRET-KEY", EncryptionHelper.Decrypt(config["API_SECRET"]));
        }



        public static async Task CallApiAndLoadDailyData(string apiGetReq)
        {
            // Now that we have a list of companies to insert insert based on date
            using (AppDbContext context = new AppDbContext(config))
            {
                try
                {





                    using (HttpClient client = new HttpClient())
                    {
                        try
                        {
                            ConfigureHTTPClient(client);

                            // Send GET request to the URL
                            HttpResponseMessage response = await client.GetAsync(apiGetReq);
                            string resp = await response.Content.ReadAsStringAsync();

                            BarResponse bars = JsonSerializer.Deserialize<BarResponse>(resp);

                            using (IDbContextTransaction transaction = context.Database.BeginTransaction())
                            {
                                try
                                {
                                    // Output some of the data
                                    foreach (var bar in bars.bars)
                                    {
                                        context.DailyBars.Add(new BarData(bar.Key, bar.Value[0]));
                                    }

                                    context.SaveChanges();
                                    transaction.Commit();
                                }
                                catch (Exception ex)
                                {
                                    transaction.Rollback();
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error fetching stock data: {ex.Message}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing symbol: {ex.Message}");
                }
            }
        }
    }
}
