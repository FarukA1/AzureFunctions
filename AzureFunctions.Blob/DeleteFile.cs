// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using System.Text.Json;
using Azure.Data.Tables;
using Azure.Messaging.EventGrid;
using AzureFunctions.Blob.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace AzureFunctions.Blob
{
    public class DeleteFile
    {
        private readonly ILogger _logger;
        private readonly string _connectionString = string.Empty;

        public DeleteFile(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<DeleteFile>();

            _connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

            if (string.IsNullOrEmpty(_connectionString))
            {
                _logger.LogError($"Error updating metadata table: Unable to load connection string");
            }
        }

        [Function("DeleteFile")]
        public async Task Run([EventGridTrigger] EventGridEvent input)
        {
            try
            {
                _logger.LogInformation($"Received Event: {JsonSerializer.Serialize(input)}");

                if (input.EventType != "Microsoft.Storage.BlobDeleted")
                {
                    _logger.LogWarning("Event is not a BlobDeleted event. Ignoring.");
                    return;
                }

                var eventData = JsonSerializer.Deserialize<BlobDeletedEventData>(input.Data.ToString());
                if (eventData == null || string.IsNullOrEmpty(eventData.Url))
                {
                    _logger.LogError("Failed to deserialize BlobDeletedEventData.");
                    return;
                }

                string blobUrl = eventData.Url;
                string blobName = Path.GetFileName(blobUrl);

                _logger.LogInformation($"Blob deleted: {blobName}");

                // Update metadata in Azure Table Storage
                string tableName = "FileMetadata";
                var tableClient = new TableClient(_connectionString, tableName);

                var entity = await tableClient.GetEntityIfExistsAsync<FileMetadata>("File", blobName);

                if (!entity.HasValue)
                {
                    _logger.LogWarning($"No metadata found for deleted file: {blobName}");
                    return;
                }

                var metadata = entity.Value;
                metadata.Action = "Deleted";
                metadata.ProcessedAt = DateTime.UtcNow;

                await tableClient.UpsertEntityAsync(metadata);
                _logger.LogInformation($"Metadata updated for deleted file: {blobName}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error handling deleted blob: {ex.Message}");
            }

            _logger.LogInformation(input.Data.ToString());
        }
    }

    public class BlobDeletedEventData
    {
        public string Url { get; set; }
    }
}

