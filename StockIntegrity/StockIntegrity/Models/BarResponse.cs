using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StockIntegrity.Models
{
    public class BarResponse
    {
        public Dictionary<string, List<Bar>> bars { get; set; }
    }

}
