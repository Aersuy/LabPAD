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
        public DbSet<SubscriptionDbModel> Subscriptions => Set<SubscriptionDbModel>();
        public DbSet<SubscriptionSubjectDbModel> SubscriptionSubjects => Set<SubscriptionSubjectDbModel>();
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
            modelBuilder.Entity<SubscriptionDbModel>(e =>
            {
                e.HasKey(s => s.ReceiverId);
                e.Property(s => s.Name).IsRequired();
            });
            modelBuilder.Entity<SubscriptionSubjectDbModel>(e =>
            {
                e.HasKey(s => new { s.ReceiverId, s.Subject });
                e.HasOne(s => s.Subscription)
                    .WithMany(s => s.Subjects)
                    .HasForeignKey(s => s.ReceiverId)
                    .OnDelete(DeleteBehavior.Cascade);
                e.HasIndex(s => s.Subject);
            });
        }

    }
}
