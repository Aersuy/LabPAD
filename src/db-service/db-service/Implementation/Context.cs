using db_service.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace db_service.Implementation
{
    public class BrokerDbContext : DbContext
    {
        public BrokerDbContext(DbContextOptions<BrokerDbContext> options) : base(options) { }

        public DbSet<MessageDbModel> Messages => Set<MessageDbModel>();
        public DbSet<SubjectDbModel> Subjects => Set<SubjectDbModel>();
        public DbSet<DeliveryDbModel> Deliveries => Set<DeliveryDbModel>();
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SubjectDbModel>()
                .HasIndex(s => s.Name)
                .IsUnique();
            modelBuilder.Entity<MessageDbModel>()
                .HasMany(m => m.Subjects)
                .WithMany(m => m.Messages)
                .UsingEntity(j => j.ToTable("MessageSubjects"));
            modelBuilder.Entity<DeliveryDbModel>(e =>
            {
                e.HasKey(d => new { d.MessageId, d.ReceiverId });                 
                e.HasOne(d => d.Message).WithMany().HasForeignKey(d => d.MessageId);
                e.HasIndex(d => new { d.Status, d.NextAttemptAt });             
                e.Property(d => d.Status).HasConversion<string>();                
            });
        }

    }
}
