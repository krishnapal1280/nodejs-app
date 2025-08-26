SILCommon/SILDbContext.cs
------------------------------------

using Microsoft.EntityFrameworkCore;
using SILCommon.Models;

namespace SILCommon
{
    public class SILDbContext : DbContext
    {
        public DbSet<SILCase> SILCases { get; set; }
        public DbSet<SILCaseDetails> SILCaseDetails { get; set; }
        public DbSet<SILCaseDocumentsDetail> SILCaseDocumentsDetails { get; set; }
        public DbSet<SILIsinAccountMapping> SILIsinAccountMappings { get; set; }
        public DbSet<SILAccountDetails> SILAccountDetails { get; set; }

        public SILDbContext(DbContextOptions<SILDbContext> options) : base(options) { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SILCase>(entity =>
            {
                entity.HasKey(e => e.CaseId);
                entity.Property(e => e.CaseId).HasMaxLength(50).IsRequired();
                entity.Property(e => e.ReceivedTimestamp).IsRequired();
                entity.Property(e => e.CreationTimestamp).IsRequired();
                entity.Property(e => e.Subject).HasMaxLength(4000).IsRequired();
                entity.Property(e => e.CaseData).HasColumnType("nvarchar(max)");
                entity.Property(e => e.ProcessingInstanceId).HasMaxLength(100);
                entity.Property(e => e.ProcessingTimestamp);
                entity.Property(e => e.Remarks).HasColumnType("nvarchar(max)");

                entity.HasMany(e => e.CaseDetails)
                    .WithOne(d => d.Case)
                    .HasForeignKey(d => d.CaseId)
                    .OnDelete(DeleteBehavior.Cascade);

                entity.HasMany(e => e.Documents)
                    .WithOne(d => d.Case)
                    .HasForeignKey(d => d.CaseId)
                    .OnDelete(DeleteBehavior.Cascade);
            });

            modelBuilder.Entity<SILCaseDetails>(entity =>
            {
                entity.HasKey(e => e.CaseDetailId);
                entity.Property(e => e.CaseId).HasMaxLength(50).IsRequired();
                entity.Property(e => e.CaseStatusCode).IsRequired();

                entity.HasOne(d => d.Case)
                    .WithMany(c => c.CaseDetails)
                    .HasForeignKey(d => d.CaseId)
                    .OnDelete(DeleteBehavior.Cascade);
            });

            modelBuilder.Entity<SILCaseDocumentsDetail>(entity =>
            {
                entity.HasKey(e => e.CaseDocID);
                entity.Property(e => e.CaseId).HasMaxLength(50).IsRequired();
                entity.Property(e => e.Filename).HasMaxLength(255).IsRequired();
                entity.Property(e => e.DoxDocumentExchangeId).HasMaxLength(100).IsRequired();
                entity.Property(e => e.DoxUploadTimestamp);

                entity.HasOne(d => d.Case)
                    .WithMany(c => c.Documents)
                    .HasForeignKey(d => d.CaseId)
                    .OnDelete(DeleteBehavior.Cascade);
            });

            modelBuilder.Entity<SILIsinAccountMapping>(entity =>
            {
                entity.HasKey(e => new { e.PositionFID, e.CaseId });
                entity.Property(e => e.PositionFID).HasMaxLength(12).IsRequired();
                entity.Property(e => e.CaseId).HasMaxLength(50).IsRequired();
                entity.Property(e => e.AccountID).HasMaxLength(50).IsRequired();
                entity.Property(e => e.InsertTimestamp).IsRequired();

                entity.HasOne<SILCase>()
                    .WithMany()
                    .HasForeignKey(e => e.CaseId)
                    .OnDelete(DeleteBehavior.Cascade);
            });

            modelBuilder.Entity<SILAccountDetails>(entity =>
            {
                entity.HasKey(e => e.AccountID);
                entity.Property(e => e.AccountID).HasMaxLength(50).IsRequired();
                entity.Property(e => e.ClientEmailAddresses).HasMaxLength(4000);
                entity.Property(e => e.ClientEmailWaiver);
                entity.Property(e => e.ClientSILSubscription);
                entity.Property(e => e.CAGPN).HasMaxLength(50);
                entity.Property(e => e.CAEmailAddress).HasMaxLength(255);
                entity.Property(e => e.CABlacklisted);
                entity.Property(e => e.ClientMailSend);
                entity.Property(e => e.CAMailSend);
                entity.Property(e => e.ClientMailSendTimestamp);
                entity.Property(e => e.CAMailSendTimestamp);
                entity.Property(e => e.InsertTimestamp).IsRequired();
            });

            base.OnModelCreating(modelBuilder);
        }
    }
}

------------------------------------------------------------------------

SILCommon/Models.cs

------------

using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace SILCommon.Models
{
    public class SILCase
    {
        [Key]
        public string CaseId { get; set; }

        [Required]
        public DateTime ReceivedTimestamp { get; set; }

        [Required]
        public DateTime CreationTimestamp { get; set; }

        [Required]
        [MaxLength(4000)]
        public string Subject { get; set; }

        [Column(TypeName = "nvarchar(max)")]
        public string CaseData { get; set; } // JSON with financialInstruments

        [MaxLength(100)]
        public string ProcessingInstanceId { get; set; }

        public DateTime? ProcessingTimestamp { get; set; }

        public int ProcessingAttempts { get; set; }

        [Column(TypeName = "nvarchar(max)")]
        public string Remarks { get; set; }

        public int AttemptCount { get; set; }

        public virtual ICollection<SILCaseDetails> CaseDetails { get; set; } = new List<SILCaseDetails>();
        public virtual ICollection<SILCaseDocumentsDetail> Documents { get; set; } = new List<SILCaseDocumentsDetail>();
    }

    public class SILCaseDetails
    {
        [Key]
        public long CaseDetailId { get; set; }

        [Required]
        [MaxLength(50)]
        public string CaseId { get; set; }

        [Required]
        public int CaseStatusCode { get; set; } // 0=Ready, 1=ISINsMapped, 2=DetailsRetrieved, 3=EmailSending, 4=Completed, -1 to -4=Failures

        [ForeignKey("CaseId")]
        public virtual SILCase Case { get; set; }
    }

    public class SILCaseDocumentsDetail
    {
        [Key]
        public long CaseDocID { get; set; }

        [Required]
        [MaxLength(50)]
        public string CaseId { get; set; }

        [Required]
        [MaxLength(255)]
        public string Filename { get; set; }

        [Required]
        [MaxLength(100)]
        public string DoxDocumentExchangeId { get; set; }

        public DateTime? DoxUploadTimestamp { get; set; }

        [ForeignKey("CaseId")]
        public virtual SILCase Case { get; set; }
    }

    public class SILIsinAccountMapping
    {
        [Key, Column(Order = 0)]
        [Required]
        [MaxLength(12)] // Typical ISIN length
        public string PositionFID { get; set; } // ISIN

        [Key, Column(Order = 1)]
        [Required]
        [MaxLength(50)]
        public string CaseId { get; set; }

        [Required]
        [MaxLength(50)]
        public string AccountID { get; set; }

        [Required]
        public DateTime InsertTimestamp { get; set; }
    }

    public class SILAccountDetails
    {
        [Key]
        [Required]
        [MaxLength(50)]
        public string AccountID { get; set; }

        [MaxLength(4000)]
        public string ClientEmailAddresses { get; set; } // Comma-separated if multiple

        public bool? ClientEmailWaiver { get; set; }

        public bool? ClientSILSubscription { get; set; }

        [MaxLength(50)]
        public string CAGPN { get; set; }

        [MaxLength(255)]
        public string CAEmailAddress { get; set; }

        public bool? CABlacklisted { get; set; }

        public bool? ClientMailSend { get; set; }

        public bool? CAMailSend { get; set; }

        public DateTime? ClientMailSendTimestamp { get; set; }

        public DateTime? CAMailSendTimestamp { get; set; }

        [Required]
        public DateTime InsertTimestamp { get; set; }
    }
}

---------------------------------------------------------------------------------------------------

SILCommon/Interfaces.cs

========================

using System.Collections.Generic;
using System.Threading.Tasks;

namespace SILCommon.Interfaces
{
    public interface IAIFClient
    {
        Task<List<string>> GetAccountIdsByIsin(string isin);
    }

    public interface ICDCClient
    {
        Task<string> GetClientEmail(string accountId);
        Task<string> GetAdvisorEmail(string accountId);
        Task<bool> GetClientSILSubscription(string accountId);
        Task<bool> GetClientEmailWaiver(string accountId);
        Task<string> GetAdvisorGPN(string accountId);
    }

    public interface IMailProcessingClient
    {
        Task SendEmail(string to, string subject, string body, List<string> doxIds);
    }

    public interface IIdpLogger
    {
        void LogInformation(string message);
        void LogWarning(string message);
        void LogError(string message, Exception ex = null);
    }
}

----------------------------------------------------------------------------------------------------------

SILCommon/MockImplementations.cs

================================

using SILCommon.Interfaces;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SILCommon.Mocks
{
    public class MockAIFClient : IAIFClient
    {
        public Task<List<string>> GetAccountIdsByIsin(string isin)
        {
            return Task.FromResult(new List<string> { "ACC001", "ACC002" });
        }
    }

    public class MockCDCClient : ICDCClient
    {
        public Task<string> GetClientEmail(string accountId) => Task.FromResult($"{accountId}_client@example.com");
        public Task<string> GetAdvisorEmail(string accountId) => Task.FromResult($"{accountId}_advisor@example.com");
        public Task<bool> GetClientSILSubscription(string accountId) => Task.FromResult(true);
        public Task<bool> GetClientEmailWaiver(string accountId) => Task.FromResult(false);
        public Task<string> GetAdvisorGPN(string accountId) => Task.FromResult($"GPN_{accountId}");
    }

    public class MockMailProcessingClient : IMailProcessingClient
    {
        public Task SendEmail(string to, string subject, string body, List<string> doxIds)
        {
            Console.WriteLine($"Mock Email Sent to {to}: Subject={subject}, Body={body}, Attachments={string.Join(",", doxIds)}");
            return Task.CompletedTask;
        }
    }

    public class MockIdpLogger : IIdpLogger
    {
        public void LogInformation(string message) => Console.WriteLine($"INFO: {message}");
        public void LogWarning(string message) => Console.WriteLine($"WARN: {message}");
        public void LogError(string message, Exception ex = null) => Console.WriteLine($"ERROR: {message} {ex?.Message}");
    }
}

-------------------------------------------------------------------------------------------------------------------------------------

SILCommon/Utilities.cs

======================

using System.Text.Json;

namespace SILCommon.Utilities
{
    public static class JsonParser
    {
        public static List<string> ExtractIsinsFromCaseData(string caseData)
        {
            var doc = JsonDocument.Parse(caseData);
            var instruments = doc.RootElement.GetProperty("ExtractedData").GetProperty("financialInstruments").EnumerateArray();
            var isins = new HashSet<string>();
            foreach (var item in instruments)
            {
                if (item.TryGetProperty("isin", out var isinElem))
                {
                    isins.Add(isinElem.GetString());
                }
            }
            return isins.ToList();
        }
    }

    public static class ConcurrencyHelper
    {
        public static string GetInstanceId() => $"{Environment.MachineName}_{System.Diagnostics.Process.GetCurrentProcess().Id}";
    }
}

---------------------------------------------------------------------------------------------------------------------------------------

SILCommon/ServiceCollectionExtensions.cs

=======================================

using Microsoft.Extensions.DependencyInjection;
using SILCommon.Interfaces;
using SILCommon.Mocks;

namespace SILCommon
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddSILServices(this IServiceCollection services)
        {
            services.AddTransient<IAIFClient, MockAIFClient>();
            services.AddTransient<ICDCClient, MockCDCClient>();
            services.AddTransient<IMailProcessingClient, MockMailProcessingClient>();
            services.AddTransient<IIdpLogger, MockIdpLogger>();
            return services;
        }
    }
}

--------------------------------------------------------------------------------------------------------------------------------------------

SILCaseClaimer/CaseClaimerService.cs

====================================

using Microsoft.EntityFrameworkCore;
using SILCommon.Interfaces;
using SILCommon.Models;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace SILCaseClaimer
{
    public class CaseClaimerService
    {
        private readonly SILDbContext _db;
        private readonly IIdpLogger _logger;

        public CaseClaimerService(SILDbContext db, IIdpLogger logger)
        {
            _db = db ?? throw new ArgumentNullException(nameof(db));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task Process(string instanceId, string specificCaseId, int requiredStatusCode)
        {
            var todayStart = DateTime.Today;
            var todayEnd = todayStart.AddDays(1);

            using var transaction = await _db.Database.BeginTransactionAsync();
            try
            {
                var query = from c in _db.SILCases
                            join d in _db.SILCaseDetails on c.CaseId equals d.CaseId
                            where d.CaseStatusCode == requiredStatusCode
                                  && (c.ProcessingInstanceId == null || c.ProcessingTimestamp < DateTime.UtcNow.AddMinutes(-30))
                                  && (string.IsNullOrEmpty(specificCaseId) ? (c.CreationTimestamp >= todayStart && c.CreationTimestamp < todayEnd) : true)
                            select new { Case = c, Details = d };

                if (!string.IsNullOrEmpty(specificCaseId))
                {
                    query = query.Where(x => x.Case.CaseId == specificCaseId);
                }

                var cases = await query.ToListAsync();

                foreach (var item in cases)
                {
                    var silCase = item.Case;
                    var details = item.Details;

                    silCase.ProcessingInstanceId = instanceId;
                    silCase.ProcessingTimestamp = DateTime.UtcNow;
                    silCase.AttemptCount++;
                    await _db.SaveChangesAsync();

                    try
                    {
                        switch (requiredStatusCode)
                        {
                            case 0:
                                details.CaseStatusCode = 1;
                                break;
                            case 1:
                                details.CaseStatusCode = 2;
                                break;
                            case 2:
                                details.CaseStatusCode = 3;
                                break;
                            default:
                                throw new InvalidOperationException($"Unsupported status code {requiredStatusCode}");
                        }
                    }
                    catch (Exception ex)
                    {
                        details.CaseStatusCode = requiredStatusCode < 0 ? requiredStatusCode : -1 * (requiredStatusCode + 1);
                        silCase.Remarks = ex.Message;
                        _logger.LogError($"Failed claiming/advancing status for {silCase.CaseId}", ex);
                    }
                    finally
                    {
                        if (details.CaseStatusCode < 0)
                        {
                            silCase.ProcessingInstanceId = null;
                            silCase.ProcessingTimestamp = null;
                        }
                        await _db.SaveChangesAsync();
                    }
                }
                await transaction.CommitAsync();
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError("Transaction failed", ex);
            }
        }
    }
}

-------------------------------------------------------------------------------------------------------------------------------------

SILISINExtractor/ISINExtractorService.cs

========================================

using Microsoft.EntityFrameworkCore;
using SILCommon.Interfaces;
using SILCommon.Models;
using SILCommon.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SILISINExtractor
{
    public class ISINExtractorService
    {
        private readonly SILDbContext _dbContext;
        private readonly IAIFClient _aifClient;
        private readonly IIdpLogger _logger;

        public ISINExtractorService(SILDbContext dbContext, IAIFClient aifClient, IIdpLogger logger)
        {
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            _aifClient = aifClient ?? throw new ArgumentNullException(nameof(aifClient));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task PollAndProcess(string instanceId, string specificCaseId, int requiredStatusCode)
        {
            var pollInterval = TimeSpan.FromSeconds(5);

            while (true)
            {
                var todayStart = DateTime.Today;
                var todayEnd = todayStart.AddDays(1);

                using var transaction = await _dbContext.Database.BeginTransactionAsync();
                try
                {
                    var query = from c in _dbContext.SILCases
                                join d in _dbContext.SILCaseDetails on c.CaseId equals d.CaseId
                                where d.CaseStatusCode == requiredStatusCode
                                      && c.ProcessingInstanceId == instanceId
                                      && (string.IsNullOrEmpty(specificCaseId) ? (c.CreationTimestamp >= todayStart && c.CreationTimestamp < todayEnd) : true)
                                select new { Case = c, Details = d };

                    if (!string.IsNullOrEmpty(specificCaseId))
                    {
                        query = query.Where(x => x.Case.CaseId == specificCaseId);
                    }

                    var cases = await query.ToListAsync();

                    foreach (var item in cases)
                    {
                        var silCase = item.Case;
                        var details = item.Details;

                        try
                        {
                            var isins = JsonParser.ExtractIsinsFromCaseData(silCase.CaseData);
                            foreach (var isin in isins)
                            {
                                var hasCurrentDayMapping = await _dbContext.SILIsinAccountMappings
                                    .AnyAsync(m => m.PositionFID == isin && m.InsertTimestamp >= todayStart && m.InsertTimestamp < todayEnd);

                                if (!hasCurrentDayMapping)
                                {
                                    var accounts = await _aifClient.GetAccountIdsByIsin(isin);
                                    foreach (var account in accounts)
                                    {
                                        _dbContext.SILIsinAccountMappings.Add(new SILIsinAccountMapping
                                        {
                                            PositionFID = isin,
                                            CaseId = silCase.CaseId,
                                            AccountID = account,
                                            InsertTimestamp = DateTime.UtcNow
                                        });
                                    }
                                    await _dbContext.SaveChangesAsync();
                                }
                            }
                            details.CaseStatusCode = 2; // DetailsRetrieved
                        }
                        catch (Exception ex)
                        {
                            details.CaseStatusCode = -2; // Failed_Details
                            silCase.Remarks = ex.Message;
                            _logger.LogError($"Failed processing ISINs for {silCase.CaseId}", ex);
                        }
                        finally
                        {
                            silCase.ProcessingInstanceId = null;
                            silCase.ProcessingTimestamp = null;
                            await _dbContext.SaveChangesAsync();
                        }
                    }
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    _logger.LogError("Transaction failed", ex);
                }

                await Task.Delay(pollInterval);
            }
        }
    }
}

----------------------------------------------------------------------------------------------------------------------------

SILAccountDetailsRetriever/AccountDetailsRetrieverService.cs

============================================================

using Microsoft.EntityFrameworkCore;
using SILCommon.Interfaces;
using SILCommon.Models;
using SILCommon.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SILAccountDetailsRetriever
{
    public class AccountDetailsRetrieverService
    {
        private readonly SILDbContext _dbContext;
        private readonly ICDCClient _cdcClient;
        private readonly IIdpLogger _logger;

        public AccountDetailsRetrieverService(SILDbContext dbContext, ICDCClient cdcClient, IIdpLogger logger)
        {
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            _cdcClient = cdcClient ?? throw new ArgumentNullException(nameof(cdcClient));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task PollAndProcess(string instanceId, string specificCaseId, int requiredStatusCode)
        {
            var pollInterval = TimeSpan.FromSeconds(5);

            while (true)
            {
                var todayStart = DateTime.Today;
                var todayEnd = todayStart.AddDays(1);

                using var transaction = await _dbContext.Database.BeginTransactionAsync();
                try
                {
                    var query = from c in _dbContext.SILCases
                                join d in _dbContext.SILCaseDetails on c.CaseId equals d.CaseId
                                where d.CaseStatusCode == requiredStatusCode
                                      && c.ProcessingInstanceId == instanceId
                                      && (string.IsNullOrEmpty(specificCaseId) ? (c.CreationTimestamp >= todayStart && c.CreationTimestamp < todayEnd) : true)
                                select new { Case = c, Details = d };

                    if (!string.IsNullOrEmpty(specificCaseId))
                    {
                        query = query.Where(x => x.Case.CaseId == specificCaseId);
                    }

                    var cases = await query.ToListAsync();

                    foreach (var item in cases)
                    {
                        var silCase = item.Case;
                        var details = item.Details;

                        try
                        {
                            var isins = JsonParser.ExtractIsinsFromCaseData(silCase.CaseData);
                            var accountIds = new HashSet<string>();
                            foreach (var isin in isins)
                            {
                                var accounts = await _dbContext.SILIsinAccountMappings
                                    .Where(m => m.PositionFID == isin)
                                    .Select(m => m.AccountID)
                                    .Distinct()
                                    .ToListAsync();
                                foreach (var acc in accounts) accountIds.Add(acc);
                            }

                            foreach (var accountId in accountIds)
                            {
                                var accDetails = await _dbContext.SILAccountDetails.FirstOrDefaultAsync(a => a.AccountID == accountId);

                                bool needsCall = accDetails == null || !(accDetails.InsertTimestamp >= todayStart && accDetails.InsertTimestamp < todayEnd);

                                if (needsCall)
                                {
                                    var clientEmail = await _cdcClient.GetClientEmail(accountId);
                                    var advisorEmail = await _cdcClient.GetAdvisorEmail(accountId);
                                    var sub = await _cdcClient.GetClientSILSubscription(accountId);
                                    var waiver = await _cdcClient.GetClientEmailWaiver(accountId);
                                    var gpn = await _cdcClient.GetAdvisorGPN(accountId);

                                    if (accDetails == null)
                                    {
                                        accDetails = new SILAccountDetails
                                        {
                                            AccountID = accountId,
                                        };
                                        _dbContext.SILAccountDetails.Add(accDetails);
                                    }

                                    accDetails.ClientEmailAddresses = clientEmail;
                                    accDetails.ClientEmailWaiver = waiver;
                                    accDetails.ClientSILSubscription = sub;
                                    accDetails.CAGPN = gpn;
                                    accDetails.CAEmailAddress = advisorEmail;
                                    accDetails.CABlacklisted = false;
                                    accDetails.InsertTimestamp = DateTime.UtcNow;

                                    await _dbContext.SaveChangesAsync();
                                }
                            }
                            details.CaseStatusCode = 3; // EmailSending
                        }
                        catch (Exception ex)
                        {
                            details.CaseStatusCode = -3; // Failed_Email
                            silCase.Remarks = ex.Message;
                            _logger.LogError($"Failed retrieving details for {silCase.CaseId}", ex);
                        }
                        finally
                        {
                            silCase.ProcessingInstanceId = null;
                            silCase.ProcessingTimestamp = null;
                            await _dbContext.SaveChangesAsync();
                        }
                    }
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    _logger.LogError("Transaction failed", ex);
                }

                await Task.Delay(pollInterval);
            }
        }
    }
}

---------------------------------------------------------------------------------------------------------------------------

SILEmailSender/EmailSenderService.cs

====================================

using Microsoft.EntityFrameworkCore;
using SILCommon.Interfaces;
using SILCommon.Models;
using SILCommon.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SILEmailSender
{
    public class EmailSenderService
    {
        private readonly SILDbContext _dbContext;
        private readonly IMailProcessingClient _mailClient;
        private readonly IIdpLogger _logger;

        public EmailSenderService(SILDbContext dbContext, IMailProcessingClient mailClient, IIdpLogger logger)
        {
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            _mailClient = mailClient ?? throw new ArgumentNullException(nameof(mailClient));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task PollAndProcess(string instanceId, string specificCaseId, int requiredStatusCode)
        {
            var pollInterval = TimeSpan.FromSeconds(5);

            while (true)
            {
                var todayStart = DateTime.Today;
                var todayEnd = todayStart.AddDays(1);

                using var transaction = await _dbContext.Database.BeginTransactionAsync();
                try
                {
                    var query = from c in _dbContext.SILCases
                                join d in _dbContext.SILCaseDetails on c.CaseId equals d.CaseId
                                where d.CaseStatusCode == requiredStatusCode
                                      && c.ProcessingInstanceId == instanceId
                                      && (string.IsNullOrEmpty(specificCaseId) ? (c.CreationTimestamp >= todayStart && c.CreationTimestamp < todayEnd) : true)
                                select new { Case = c, Details = d };

                    if (!string.IsNullOrEmpty(specificCaseId))
                    {
                        query = query.Where(x => x.Case.CaseId == specificCaseId);
                    }

                    var cases = await query.ToListAsync();

                    foreach (var item in cases)
                    {
                        var silCase = item.Case;
                        var details = item.Details;

                        try
                        {
                            var isins = JsonParser.ExtractIsinsFromCaseData(silCase.CaseData);
                            var accountIds = new HashSet<string>();
                            foreach (var isin in isins)
                            {
                                var accounts = await _dbContext.SILIsinAccountMappings
                                    .Where(m => m.PositionFID == isin)
                                    .Select(m => m.AccountID)
                                    .Distinct()
                                    .ToListAsync();
                                foreach (var acc in accounts) accountIds.Add(acc);
                            }

                            var doxIds = await _dbContext.SILCaseDocumentsDetails
                                .Where(d => d.CaseId == silCase.CaseId)
                                .Select(d => d.DoxDocumentExchangeId)
                                .ToListAsync();

                            foreach (var accountId in accountIds)
                            {
                                var accDetails = await _dbContext.SILAccountDetails.FirstOrDefaultAsync(a => a.AccountID == accountId);

                                if (accDetails != null && !accDetails.ClientMailSend.HasValue && accDetails.ClientSILSubscription == true && accDetails.ClientEmailWaiver != true)
                                {
                                    await _mailClient.SendEmail(accDetails.ClientEmailAddresses, "SIL Notification", "Your update is ready", doxIds);
                                    accDetails.ClientMailSend = true;
                                    accDetails.ClientMailSendTimestamp = DateTime.UtcNow;
                                }

                                if (!accDetails?.CAMailSend.HasValue ?? true)
                                {
                                    await _mailClient.SendEmail(accDetails?.CAEmailAddress, "SIL Notification for Advisor", "Client update notification", doxIds);
                                    accDetails.CAMailSend = true;
                                    accDetails.CAMailSendTimestamp = DateTime.UtcNow;
                                }
                                await _dbContext.SaveChangesAsync();
                            }

                            details.CaseStatusCode = 4; // Completed
                        }
                        catch (Exception ex)
                        {
                            details.CaseStatusCode = -4; // Failed_Email
                            silCase.Remarks = ex.Message;
                            _logger.LogError($"Failed sending emails for {silCase.CaseId}", ex);
                        }
                        finally
                        {
                            silCase.ProcessingInstanceId = null;
                            silCase.ProcessingTimestamp = null;
                            await _dbContext.SaveChangesAsync();
                        }
                    }
                    await transaction.CommitAsync();
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    _logger.LogError("Transaction failed", ex);
                }

                await Task.Delay(pollInterval);
            }
        }
    }
}

--------------------------------------------------------------------------------------------------------------------------------------------------------------------

SILCaseRetrigger/CaseRetriggerService.cs

========================================

using Microsoft.EntityFrameworkCore;
using SILCommon.Interfaces;
using SILCommon.Models;
using System;
using System.Threading.Tasks;

namespace SILCaseRetrigger
{
    public class CaseRetriggerService
    {
        private readonly SILDbContext _dbContext;
        private readonly IIdpLogger _logger;

        public CaseRetriggerService(SILDbContext dbContext, IIdpLogger logger)
        {
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task RetriggerCase(string caseId, int targetStatusCode)
        {
            using var transaction = await _dbContext.Database.BeginTransactionAsync();
            try
            {
                var silCase = await _dbContext.SILCases.FindAsync(caseId);
                if (silCase == null)
                {
                    _logger.LogWarning($"Case {caseId} not found for retriggering.");
                    return;
                }

                if (targetStatusCode < 0 || targetStatusCode > 4)
                {
                    throw new ArgumentException($"Invalid target status code {targetStatusCode}. Must be between 0 and 4.");
                }

                silCase.ProcessingInstanceId = null;
                silCase.ProcessingTimestamp = null;
                silCase.AttemptCount = 0;
                silCase.Remarks = null;

                var details = await _dbContext.SILCaseDetails.FirstOrDefaultAsync(d => d.CaseId == caseId);
                if (details != null)
                {
                    details.CaseStatusCode = targetStatusCode;
                }

                await _dbContext.SaveChangesAsync();
                await transaction.CommitAsync();
                _logger.LogInformation($"Case {caseId} retriggered to status {targetStatusCode}.");
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError($"Failed to retrigger case {caseId}", ex);
                throw;
            }
        }
    }
}

------------------------------------------------------------------------------------------------------------------------------------------------------------------

SILISINExtractor/Program.cs

===========================

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SILCommon;
using SILCommon.Interfaces;
using SILCommon.Utilities;
using System;
using System.CommandLine;
using System.Threading.Tasks;

namespace SILISINExtractor
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            var services = new ServiceCollection();
            services.AddDbContext<SILDbContext>(options => options.UseSqlServer(config.GetConnectionString("SILDb")));
            services.AddSILServices();
            var serviceProvider = services.BuildServiceProvider();

            var dbContext = serviceProvider.GetRequiredService<SILDbContext>();
            var logger = serviceProvider.GetRequiredService<IIdpLogger>();

            var caseIdOption = new Option<string>("--caseId", "Specific case ID to process");
            var statusCodeOption = new Option<int>("--statusCode", "Status code to process from (default: 1)") { DefaultValue = 1 };
            var rootCommand = new RootCommand("Extracts ISINs and maps accounts for SIL cases.");
            rootCommand.AddOption(caseIdOption);
            rootCommand.AddOption(statusCodeOption);
            rootCommand.SetHandler(async (caseId, statusCode) =>
            {
                var instanceId = ConcurrencyHelper.GetInstanceId();
                await new ISINExtractorService(dbContext, serviceProvider.GetRequiredService<IAIFClient>(), logger)
                    .PollAndProcess(instanceId, caseId, statusCode);
            }, caseIdOption, statusCodeOption);

            await rootCommand.InvokeAsync(args);
        }
    }
}

----------------------------------------------------------------------------------------------------------------------------------------------------

SILAccountDetailsRetriever/Program.cs

=====================================

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SILCommon;
using SILCommon.Interfaces;
using SILCommon.Utilities;
using System;
using System.CommandLine;
using System.Threading.Tasks;

namespace SILAccountDetailsRetriever
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            var services = new ServiceCollection();
            services.AddDbContext<SILDbContext>(options => options.UseSqlServer(config.GetConnectionString("SILDb")));
            services.AddSILServices();
            var serviceProvider = services.BuildServiceProvider();

            var dbContext = serviceProvider.GetRequiredService<SILDbContext>();
            var logger = serviceProvider.GetRequiredService<IIdpLogger>();

            var caseIdOption = new Option<string>("--caseId", "Specific case ID to process");
            var statusCodeOption = new Option<int>("--statusCode", "Status code to process from (default: 2)") { DefaultValue = 2 };
            var rootCommand = new RootCommand("Retrieves account details for SIL cases.");
            rootCommand.AddOption(caseIdOption);
            rootCommand.AddOption(statusCodeOption);
            rootCommand.SetHandler(async (caseId, statusCode) =>
            {
                var instanceId = ConcurrencyHelper.GetInstanceId();
                await new AccountDetailsRetrieverService(dbContext, serviceProvider.GetRequiredService<ICDCClient>(), logger)
                    .PollAndProcess(instanceId, caseId, statusCode);
            }, caseIdOption, statusCodeOption);

            await rootCommand.InvokeAsync(args);
        }
    }
}

----------------------------------------------------------------------------------------------------------------------------------------------

SILEmailSender/Program.cs

=========================

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SILCommon;
using SILCommon.Interfaces;
using SILCommon.Utilities;
using System;
using System.CommandLine;
using System.Threading.Tasks;

namespace SILEmailSender
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            var services = new ServiceCollection();
            services.AddDbContext<SILDbContext>(options => options.UseSqlServer(config.GetConnectionString("SILDb")));
            services.AddSILServices();
            var serviceProvider = services.BuildServiceProvider();

            var dbContext = serviceProvider.GetRequiredService<SILDbContext>();
            var logger = serviceProvider.GetRequiredService<IIdpLogger>();

            var caseIdOption = new Option<string>("--caseId", "Specific case ID to process");
            var statusCodeOption = new Option<int>("--statusCode", "Status code to process from (default: 3)") { DefaultValue = 3 };
            var rootCommand = new RootCommand("Sends email notifications for SIL cases.");
            rootCommand.AddOption(caseIdOption);
            rootCommand.AddOption(statusCodeOption);
            rootCommand.SetHandler(async (caseId, statusCode) =>
            {
                var instanceId = ConcurrencyHelper.GetInstanceId();
                await new EmailSenderService(dbContext, serviceProvider.GetRequiredService<IMailProcessingClient>(), logger)
                    .PollAndProcess(instanceId, caseId, statusCode);
            }, caseIdOption, statusCodeOption);

            await rootCommand.InvokeAsync(args);
        }
    }
}

------------------------------------------------------------------------------------------------------------------------------------------------------

SILCaseRetrigger/Program.cs

===========================

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SILCommon;
using SILCommon.Interfaces;
using System;
using System.CommandLine;
using System.Threading.Tasks;

namespace SILCaseRetrigger
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            var services = new ServiceCollection();
            services.AddDbContext<SILDbContext>(options => options.UseSqlServer(config.GetConnectionString("SILDb")));
            services.AddSILServices();
            var serviceProvider = services.BuildServiceProvider();

            var dbContext = serviceProvider.GetRequiredService<SILDbContext>();
            var logger = serviceProvider.GetRequiredService<IIdpLogger>();

            var caseIdOption = new Option<string>("--caseId", "Case ID to retrigger") { IsRequired = true };
            var statusCodeOption = new Option<int>("--statusCode", "Target status code to retrigger to") { IsRequired = true };
            var rootCommand = new RootCommand("Retriggers a failed or stalled SIL case to a specified status.");
            rootCommand.AddOption(caseIdOption);
            rootCommand.AddOption(statusCodeOption);
            rootCommand.SetHandler(async (caseId, statusCode) =>
            {
                await new CaseRetriggerService(dbContext, logger).RetriggerCase(caseId, statusCode);
            }, caseIdOption, statusCodeOption);

            await rootCommand.InvokeAsync(args);
        }
    }
}

--------------------------------------------------------------------------------------------------------------------------------------------------------