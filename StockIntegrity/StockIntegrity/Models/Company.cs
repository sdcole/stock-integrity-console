namespace StockIntegrity.Models
{
    public class Company
    {
        public int Id { get; set; }
        public string Symbol { get; set; }
        public string CompanyDescription { get; set; }
        public int SectorId { get; set; }
    }
}
