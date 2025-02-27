﻿namespace StockIntegrity.Models
{
    public class BarData
    {
        public int Id { get; set; }  // This will be auto-generated by the database
        public string Symbol { get; set; }  // For example, "AAPL"
        public DateTime Timestamp { get; set; }  // This will be mapped from 't' (ISO 8601 datetime)
        public double Open { get; set; }  // From 'o'
        public double High { get; set; }  // From 'h'
        public double Low { get; set; }  // From 'l'
        public double Close { get; set; }  // From 'c'
        public long Volume { get; set; }  // From 'v'
        public long TradeCount { get; set; }  // From 'n'
        public double VW { get; set; }  // From 'vw'

        // Parameterless constructor for EF
        public BarData() { }

        // Constructor that takes Symbol and Bar
        public BarData(string symbol, Bar bar)
        {
            Symbol = symbol;
            Timestamp = bar.t;
            Open = bar.o;
            High = bar.h;
            Low = bar.l;
            Close = bar.c;
            Volume = bar.v;
            TradeCount = bar.n;
            VW = bar.vw;
        }
    }

}
