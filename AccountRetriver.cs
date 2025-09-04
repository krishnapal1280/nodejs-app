using Microsoft.EntityFrameworkCore;
using SILCommon.Interfaces;
using SILCommon.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SILCaseClaimer
{
    public class AccountDetailsRetrieverService
    {
        private readonly SILDbContext _dbContext;
        private readonly IIdpLogger _logger;
        private readonly ICDCClient _cdcClient; // CDC SOAP client

        public AccountDetailsRetrieverService(SILDbContext dbContext, IIdpLogger logger, ICDCClient cdcClient)
        {
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _cdcClient = cdcClient ?? throw new ArgumentNullException(nameof(cdcClient));
        }

        public async Task<Dictionary<string, Dictionary<string, object>>> RetrieveAccountDetails(RequiredStatusCodes statusCode)
        {
            var today = DateTime.Today;
            var accountMappings = new Dictionary<string, Dictionary<string, object>>();

            try
            {
                // Join SILCaseDetails and SILISINAccountMappings, filtering by latest status and InsertDate
                var mappings = await _dbContext.SILCaseDetails
                    .GroupBy(d => d.CaseId)
                    .Select(g => g.OrderByDescending(d => d.UpdateTimestamp).First())
                    .Where(d => d.CaseStatusCode == (int)statusCode)
                    .Join(
                        _dbContext.SILISINAccountMappings.Where(m => m.InsertDate.Date == today),
                        d => d.CaseId,
                        m => m.CaseId,
                        (d, m) => new { d.CaseId, m.AccountId })
                    .GroupBy(x => x.CaseId)
                    .Select(g => new { CaseId = g.Key, AccountIds = g.Select(x => x.AccountId).Distinct().ToList() })
                    .ToListAsync();

                foreach (var mapping in mappings)
                {
                    var caseDetails = new Dictionary<string, object>();
                    foreach (var accountId in mapping.AccountIds)
                    {
                        try
                        {
                            // Perform CDC SOAP call for each AccountId
                            var cdcResponse = await _cdcClient.GetClientDetailsAsync(accountId); // Hypothetical method
                            caseDetails[accountId] = cdcResponse ?? new { Error = "No data from CDC" };
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError($"Failed to retrieve CDC details for AccountId {accountId}: {ex.Message}");
                            caseDetails[accountId] = new { Error = ex.Message };
                        }
                    }
                    accountMappings[mapping.CaseId] = caseDetails;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error in RetrieveAccountDetails: {ex.Message}");
                throw; // Re-throw to be handled by the caller
            }

            return accountMappings;
        }
    }
}
