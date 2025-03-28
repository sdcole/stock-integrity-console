namespace StockIntegrity.Models
{
    public class Company
    {
        public int Id { get; set; }
        public string Ticker { get; set; }
        public string CompanyDescription { get; set; }
        public string Sector { get; set; }
    }
}
