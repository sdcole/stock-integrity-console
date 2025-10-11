using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StockIntegrity.Models
{
    public class SymbolDailySummary
    {

        public long Id { get; set; }
        [Required]
        public string Symbol { get; set; }
        [Required]
        public DateTime Date { get; set; }

        // Price & Volume Features
        public double? Return1d { get; set; }
        public double? Return5d { get; set; }
        public double? Volatility5d { get; set; }
        public double? Volatility10d { get; set; }
        public double? Sma5 { get; set; }
        public double? Sma10 { get; set; }
        public double? Rsi14 { get; set; }
        public double? BollingerBandwidth { get; set; }
        public double? VolumeAvg5d { get; set; }
        public double? VolumeRatio { get; set; }

        // Sector & Market Context
        /*
        [MaxLength(100)]
        public string? Sector { get; set; }
        public double? SectorAvgReturn5d { get; set; }
        public double? SectorVolatility5d { get; set; }
        public double? MarketAvgReturn5d { get; set; }
        public double? MarketVolatility5d { get; set; }

        // Labels
        public double? FutureReturn5d { get; set; }

        /// <summary>
        /// 0 = stable, 1 = unstable
        /// </summary>
        public short? LabelStability { get; set; }

        /// <summary>
        /// 0 = down/stable, 1 = up
        /// </summary>
        public short? LabelDirection { get; set; }
        */
    }
}
