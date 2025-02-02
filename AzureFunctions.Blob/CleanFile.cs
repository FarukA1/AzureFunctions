using System;
using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using AzureFunctions.Blob.Models;
using AzureFunctions.Common;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace AzureFunctions.Blob
{
    public class CleanFile
    {
        private readonly ILogger _logger;
        private readonly BlobServiceClient _blobServiceClient;
        private readonly string _connectionString = string.Empty;

        public CleanFile(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<CleanFile>();

            _connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

            if (string.IsNullOrEmpty(_connectionString))
            {
                _logger.LogError($"Error updating metadata table: Unable to load connection string");
            }

            _blobServiceClient = new BlobServiceClient(_connectionString);
        }

        [Function("CleanFile")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] MyInfo myTimer)
        {
            string containerName = Utilities.FileContainerName;
            string tableName = Utilities.FileTableName;

            var tableServiceClient = new TableServiceClient(_connectionString);

            if (!await TableExistsAsync(tableServiceClient, tableName))
            {
                _logger.LogWarning($"Table '{tableName}' does not exist. Cleanup aborted.");
                return;
            }

            var tableClient = tableServiceClient.GetTableClient(tableName);

            var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);

            DateTime thresholdDate = DateTime.UtcNow.AddDays(-30);

            var queryResults = tableClient.Query<FileMetadata>(m => m.ProcessedAt < thresholdDate);

            foreach (var metadata in queryResults)
            {
                var blobClient = containerClient.GetBlobClient(metadata.FileName);
                await blobClient.DeleteIfExistsAsync();

                metadata.Action = "Expired";
                await tableClient.UpsertEntityAsync(metadata);

                _logger.LogInformation($"Deleted expired file: {metadata.FileName}");
            }
        }

        private async Task<bool> TableExistsAsync(TableServiceClient serviceClient, string tableName)
        {
            try
            {
                await serviceClient.GetTableClient(tableName).CreateIfNotExistsAsync();
                return true;
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                return false;
            }
        }
    }

    public class MyInfo
    {
        public MyScheduleStatus ScheduleStatus { get; set; }

        public bool IsPastDue { get; set; }
    }

    public class MyScheduleStatus
    {
        public DateTime Last { get; set; }

        public DateTime Next { get; set; }

        public DateTime LastUpdated { get; set; }
    }
}

