using Broker.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;



namespace Broker.Db
{
    public class BrokerDbContext : DbContext
    {
        public BrokerDbContext(DbContextOptions<BrokerDbContext> options) : base(options) { }

        public DbSet<MessageDbModel> Messages => Set<MessageDbModel>();
        public DbSet<SubjectDbModel> Subjects => Set<SubjectDbModel>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SubjectDbModel>()
                .HasIndex(s => s.Name)
                .IsUnique();
            modelBuilder.Entity<MessageDbModel>()
                .HasMany(m => m.Subjects)
                .WithMany(m => m.Messages)
                .UsingEntity(j => j.ToTable("MessageSubjects"));
        }
    }
}
