﻿using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Reflection.Emit;
using StockIntegrity.Models;
using StockIntegrity.Helpers;
public class AppDbContext : DbContext
{
    private readonly IConfiguration _configuration;

    public AppDbContext(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public DbSet<BarData> DailyBars { get; set; }  // Add this DbSet for BarData
    public DbSet<Company> Companies { get; set; }
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        var connectionString = EncryptionHelper.Decrypt(_configuration.GetConnectionString("AppConnection"));
        optionsBuilder.UseNpgsql(connectionString);  // Use PostgreSQL, as indicated
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {


        // Configure the Company entity
        modelBuilder.Entity<Company>()
            .ToTable("companies");

        modelBuilder.Entity<Company>()
            .Property(c => c.Id)
            .HasColumnName("id")
            .IsRequired();

        modelBuilder.Entity<Company>()
            .Property(c => c.Ticker)
            .HasColumnName("ticker")
            .IsRequired()
            .HasMaxLength(10);

        modelBuilder.Entity<Company>()
            .Property(c => c.CompanyDescription)
            .HasColumnName("company_description")
            .IsRequired();

        modelBuilder.Entity<Company>()
            .Property(c => c.Sector)
            .HasColumnName("sector")
            .IsRequired()
            .HasMaxLength(100);


        // Configure BarData entity
        modelBuilder.Entity<BarData>()
            .ToTable("daily_bars");  // Table name in the database

        modelBuilder.Entity<BarData>()
            .Property(b => b.Id)
            .HasColumnName("id")
            .IsRequired();  // Primary Key, auto-generated

        modelBuilder.Entity<BarData>()
            .Property(b => b.Symbol)
            .HasColumnName("symbol")
            .IsRequired()
            .HasMaxLength(10);  // Max length for stock symbols like "AAPL", "MSFT"

        modelBuilder.Entity<BarData>()
            .Property(b => b.Timestamp)
            .HasColumnName("timestamp")
            .IsRequired();  // Timestamp in RFC-3339 format

        modelBuilder.Entity<BarData>()
            .Property(b => b.Open)
            .HasColumnName("open")
            .IsRequired();  // Opening price

        modelBuilder.Entity<BarData>()
            .Property(b => b.High)
            .HasColumnName("high")
            .IsRequired();  // Highest price

        modelBuilder.Entity<BarData>()
            .Property(b => b.Low)
            .HasColumnName("low")
            .IsRequired();  // Lowest price

        modelBuilder.Entity<BarData>()
            .Property(b => b.Close)
            .HasColumnName("close")
            .IsRequired();  // Closing price

        modelBuilder.Entity<BarData>()
            .Property(b => b.Volume)
            .HasColumnName("volume")
            .IsRequired();  // Trade volume

        modelBuilder.Entity<BarData>()
            .Property(b => b.TradeCount)
            .HasColumnName("trade_count")
            .IsRequired();  // Number of trades

        modelBuilder.Entity<BarData>()
            .Property(b => b.VW)
            .HasColumnName("vw")
            .IsRequired();  // Volume Weighted Average Price

        // Define unique constraint for Symbol and Timestamp
        modelBuilder.Entity<BarData>()
            .HasIndex(b => new { b.Symbol, b.Timestamp })
            .IsUnique();
    }
}
