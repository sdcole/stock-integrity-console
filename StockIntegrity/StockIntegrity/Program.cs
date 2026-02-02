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
                { "timestamp", new TimestampColumnWriter() },
                { "exception", new ExceptionColumnWriter() },
                { "properties", new PropertiesColumnWriter() }
            };

            // Configure Serilog
            Log.Logger = new LoggerConfiguration()
                .Enrich.WithProperty("app_name", APP_NAME)
                .WriteTo.PostgreSQL(EncryptionService.Decrypt(config.GetConnectionString("LoggingConnection")), "logs", columnOptions, needAutoCreateTable: true)
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
                    date = await CheckDailyBars(date);
                    //CheckDailySummary(date);
                    await Task.Delay(5000);
                    
                }
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Unhandled Exception caused the app to crash.");
            }

            Log.CloseAndFlush();
            
        }

        /**
         * CheckDailyBars
         * 
         * This function is the thread manager for the app. It will kick off 6 parallel threads one for each date.
         * It manages only 6 threads at a time to not interfere with api limits.
         * It waits till all have completed before continuing to prevent any leaks.
         * 
         * 
         * 
         * 
         * 
         **/
        public static async Task<DateTime> CheckDailyBars(DateTime date)
        {
            List<Task> runningTasks = new List<Task>();
            DateTime currDate = date;
            DateTime nextDate = new DateTime();
            // Schedule up to 6 tasks

            for (int i = 0; i < 6; i++)
            {



                nextDate = GetNextValidDate(currDate, DateTime.UtcNow.Date.AddDays(-2));
                if (nextDate == DateTime.MinValue)
                    break; // Stop scheduling if no more valid dates

                Console.WriteLine($"Scheduled task for: {nextDate.ToShortDateString()}");

                // Add the task to the list without starting it yet
                runningTasks.Add(Task.Factory.StartNew(() => {
                    CheckDateDataIntegrity(nextDate.Date);
                    // Code to run in parallel
                }, TaskCreationOptions.LongRunning));
                currDate = nextDate;
            }
            
            // If no tasks were scheduled, exit loop
            if (runningTasks.Count == 0)
                return nextDate;

            // Wait for all tasks to complete before continuing
            await Task.WhenAll(runningTasks);
            return nextDate;
        }
        static DateTime GetNextValidDate(DateTime date, DateTime today)
        {
            while (date < today)
            {
                
                date = date.AddDays(1); // Move forward
                DateTime validDate = date;
                if (validDate.DayOfWeek != DayOfWeek.Saturday && validDate.DayOfWeek != DayOfWeek.Sunday)
                {
                    return validDate; // Return the first valid weekday
                }
            }
            return DateTime.MinValue; // No more valid dates
        }

        /**
         * CheckDateDataIntegrity
         * This function ensures only one record for each ticker each day exists
         * 
         * If there are more than one, it deletes the duplicates and adds new ones if necessary.
         * If none are found it will build a Get request for the API including the symbol for that date.
         * 
         * 
         **/
        public static async Task CheckDateDataIntegrity(DateTime date)
        {
            string baseUrl = @"https://data.alpaca.markets/v2/stocks/bars?timeframe=1D&start="
                + date.ToString("yyyy-MM-dd")
                + "&end=" + date.ToString("yyyy-MM-dd")
                + "&limit=1000&adjustment=raw&feed=iex&currency=USD&sort=asc&symbols=";

            using var context = new AppDbContext(config);

            //TODO
            //Steps are as follows:
            //1. Get counts per symbol for this date
            //2. Cleanup duplicates
            //3. Determine missing symbols
            //4. Fetch missing via API (batched under 2048 char limit)
            
            // --- Step 1: Get counts per symbol for this date
            // Use explicit UTC date range to avoid timezone conversion issues
            var startOfDay = date.Date;
            var endOfDay = date.Date.AddDays(1);

            var symbolGroups = await context.DailyBars
                .Where(r => r.Timestamp >= startOfDay && r.Timestamp < endOfDay)
                .GroupBy(r => r.Symbol.Trim())
                .Select(g => new { Symbol = g.Key, Count = g.Count() })
                .ToListAsync();

            var goodSymbols = symbolGroups
                .Where(g => g.Count == 1)
                .Select(g => g.Symbol)
                .ToHashSet();

            var duplicateSymbols = symbolGroups
                .Where(g => g.Count > 1)
                .Select(g => g.Symbol)
                .ToHashSet();

            // --- Step 2: Cleanup duplicates
            foreach (var dup in duplicateSymbols)
            {
                var extras = await context.DailyBars
                    .Where(r => r.Timestamp >= startOfDay && r.Timestamp < endOfDay && r.Symbol.Trim() == dup)
                    .OrderBy(r => r.Timestamp) // keep the first one
                    .Skip(1)
                    .ToListAsync();

                context.DailyBars.RemoveRange(extras);
            }

            // --- Step 3: Determine missing symbols
            var allSymbols = companies
                .Select(c => c.Symbol.Trim())
                .ToHashSet();

            var missingSymbols = allSymbols
                .Except(goodSymbols)
                .Except(duplicateSymbols)
                .ToList();

            // --- Step 4: Fetch missing via API (batched under 2048 char limit)
            if (missingSymbols.Any())
            {
                var apiReq = baseUrl;

                foreach (var symbol in missingSymbols)
                {
                    if ((apiReq + symbol + ",").Length >= 2043)
                    {
                        // finalize current batch
                        apiReq = apiReq.TrimEnd(',');
                        CallApiAndLoadDailyData(apiReq);
                        apiReq = baseUrl;
                    }

                    apiReq += symbol + ",";
                }

                if (apiReq != baseUrl)
                {
                    apiReq = apiReq.TrimEnd(',');
                    CallApiAndLoadDailyData(apiReq);
                }
            }

            // --- Step 5: Commit cleanup deletes
            await context.SaveChangesAsync();
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
                if (!EncryptionService.IsEncrypted(value))
                {
                    EncryptionService.UpdateConfigFile(EncryptionService.Encrypt(value), key);
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
            client.DefaultRequestHeaders.Add("APCA-API-KEY-ID", EncryptionService.Decrypt(config["API_KEY"]));
            client.DefaultRequestHeaders.Add("APCA-API-SECRET-KEY", EncryptionService.Decrypt(config["API_SECRET"]));
        }


        /**
         * CallApiAndLoadDailyData
         * 
         * This function will take an api get request, make the get call, parse the json response, then lastly add and save to db.
         * 
         * 
         * 
         **/
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
                            if (bars.bars.Count == 0)
                            {
                                Log.Information("API Returned no records for call: " + apiGetReq);
                            }
                            else
                            {
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

        /**
         * CheckDailySummary
         * 
         * This function is to be used to track daily summary data (how the stock did for the day, recent data, etc..).
         * 
         * 
         **/
        public static async Task CheckDailySummary(DateTime date)
        {
            try
            {
                foreach (Company company in companies)
                {
                    using (AppDbContext context = new AppDbContext(config))
                    {
                        var records = context.SymbolDailySummaries
                            .Where(r => r.Date.Date == date.Date && r.Symbol.Trim() == company.Symbol.Trim())
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
                                context.SymbolDailySummaries.Remove(records[i]);
                                context.SaveChanges();
                            }
                        }
                        else
                        {
                            //We have figured out that there is not a symbol daily summary for the current day for the given company.
                            //Now we can start calculating the values based on the bar data.
                            SymbolDailySummary symbolDailySummary = await CalculateDailySummaryForSymbol(records[0].Symbol);

                            if (symbolDailySummary != null)
                            {
                                context.SymbolDailySummaries.Add(symbolDailySummary);
                                context.SaveChanges();
                            }
                            else
                            {
                                //Do nothing as we do not have a valid object to insert.
                            }

                        }
                    }
                }
            
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error processing the SymbolDailySummaries");
            }
            
        }



        /**
         * CalculateDailySummaryForSymbol
         * This function calculates the daily summary for a given symbol.
         * 
         * Returns an instance of DailySummary with the calculated values or NULL if there is not enough data to calculate the summary.
         *
         **/
        public static async Task<SymbolDailySummary> CalculateDailySummaryForSymbol(string symbol)
        {
            using (AppDbContext context = new AppDbContext(config))
            {
                try
                {
                    //Get the last 21 daily bars for calculations. 20 Days is a month in trading terms.
                    var last21 = await context.DailyBars
                        .Where(d => d.Symbol == symbol)
                        .OrderByDescending(d => d.Timestamp)
                        .Take(21)
                        .ToListAsync();

                    if (last21.Count != 21)
                    {
                        Log.Warning("Not enough previous daily information was available for symbol: " + symbol
                            + " to calculate summary information for: " + DateTime.UtcNow.Date);
                        return null;
                    }
                    SymbolDailySummary dailySummary = new SymbolDailySummary
                    {
                        Symbol = symbol,
                        Date = last21.First().Timestamp.Date // Set to today's date
                    };

                    //Calculate returns
                    dailySummary.Return1d = (last21[0].Close - last21[1].Close) / last21[1].Close;
                    dailySummary.Return5d = (last21[0].Close - last21[4].Close) / last21[4].Close;


                    //Calculate Volatiily
                    List<double> returnsList = GetReturnList(last21.Take(6).ToList(), 5);
                    if (returnsList == null)
                    {
                        Log.Information("Unable to process data! Could not retreive returns list for the past 5 days for: " + symbol);
                        return null;
                    }
                    dailySummary.Volatility5d = StandardDeviation(returnsList);

                    returnsList = GetReturnList(last21.Take(11).ToList(), 10);
                    if (returnsList == null)
                    {
                        Log.Information("Unable to process data! Could not retreive returns list for the past 10 days for: " + symbol);
                        return null;
                    }
                    dailySummary.Volatility10d = StandardDeviation(returnsList);



                    //Calculate SMA
                    dailySummary.Sma5 = last21.Take(5).Average(d => d.Close);
                    dailySummary.Sma10 = last21.Take(10).Average(d => d.Close);


                    //Calculate RSI
                    List<BarData> ascLast15 = last21.Take(15).ToList();
                    dailySummary.Rsi14 = CalculateRsi(ascLast15.Select(d => d.Close).ToList(), 14);

                    //Calculate Bollinger Bands
                    List<BarData> ascLast20 = last21.Take(20).ToList();
                    dailySummary.BollingerBandwidth = CalculateBollingerBandwidth(ascLast20.Select(d => d.Close).ToList(), 20);

                    //Calculate Volume
                    dailySummary.VolumeAvg5d = last21.Take(5).Average(d => d.Volume);
                    dailySummary.VolumeRatio = last21[0].Volume / dailySummary.VolumeAvg5d;

                    //List<double> 5dReturns = Get5DayReturnList(last20);
                    return dailySummary;
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error calculating daily summary for symbol: " + symbol);
                    Console.WriteLine($"Error fetching daily bar data for symbol {symbol}: {ex.Message}");
                    return null;
                }
            }


            // Fetch daily bar data for the symbol


        }


        public static List<double> GetReturnList(List<BarData> barData, int returnDuration)
        {
            try
            {
                if (barData == null || barData.Count != returnDuration + 1)
                {
                    Log.Error("Invalid bar data provided for return calculation.");
                    return null;
                }
                List<double> returnsList = new List<double>();

                for (int i = 0; i < returnDuration; i++)
                {
                    double returnValue = (barData[i].Close - barData[i + 1].Close) / barData[i + 1].Close;
                    returnsList.Add(returnValue);
                }
                return returnsList;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error calculating returns for daily bar data.");
                return null;
            }
        }

        public static double StandardDeviation(IEnumerable<double> values)
        {
            var enumerable = values.ToList();
            var avg = enumerable.Average();
            var sum = enumerable.Sum(v => Math.Pow(v - avg, 2));
            return Math.Sqrt(sum / (enumerable.Count - 1));
        }


        public static double CalculateRsi(List<double> closes, int period = 14)
        {
            if (closes == null || closes.Count < period + 1)
            {
                Log.Error("Not enough closing prices to calculate RSI.");
                throw new ArgumentException($"At least {period + 1} closing prices required");
            }
            closes.Reverse(); // Ensure the dataset is in ascending order (oldest to newest)
            // Calculate daily changes
            List<double> changes = new List<double>();
            for (int i = 1; i < closes.Count; i++)
            {
                changes.Add(closes[i] - closes[i - 1]);
            }

            // Separate gains and losses
            List<double> gains = changes.Select(c => c > 0 ? c : 0).ToList();
            List<double> losses = changes.Select(c => c < 0 ? Math.Abs(c) : 0).ToList();

            // Calculate initial average gain and loss (simple average)
            double avgGain = gains.Take(period).Average();
            double avgLoss = losses.Take(period).Average();

            // Use Wilder’s smoothing method for the rest of the period, if more data is available
            for (int i = period; i < gains.Count; i++)
            {
                avgGain = ((avgGain * (period - 1)) + gains[i]) / period;
                avgLoss = ((avgLoss * (period - 1)) + losses[i]) / period;
            }

            if (avgLoss == 0)
                return 100; // RSI is 100 if no losses

            double rs = avgGain / avgLoss;
            double rsi = 100 - (100 / (1 + rs));
            return rsi;
        }


        public static double CalculateBollingerBandwidth(List<double> closes, int period = 20)
        {
            if (closes == null || closes.Count < period)
            {
                Log.Error("Not enough closing prices to calculate Bollinger Bands.");
                throw new ArgumentException($"At least {period} closing prices required");
            }
            closes.Reverse(); // Ensure the dataset is in ascending order (oldest to newest)
            var recentCloses = closes.TakeLast(period).ToList();

            double sma = recentCloses.Average();
            double stdDev = StandardDeviation(recentCloses);  // Use your Std Dev extension method

            double upperBand = sma + 2 * stdDev;
            double lowerBand = sma - 2 * stdDev;

            double bandwidth = (upperBand - lowerBand) / sma;
            return bandwidth;
        }


    }
}
