using System;
using System.IO;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using AzureFunctions.Blob.Models;
using AzureFunctions.Common;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace AzureFunctions.Blob
{
    public class ProcessFile
    {
        private readonly ILogger _logger;
        private readonly BlobServiceClient _blobServiceClient;
        private readonly string _connectionString = string.Empty;

        public ProcessFile(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<ProcessFile>();

            _connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

            if (string.IsNullOrEmpty(_connectionString))
            {
                _logger.LogError($"Error updating metadata table: Unable to load connection string");
            }

            _blobServiceClient = new BlobServiceClient(_connectionString);
        }

        [Function("ProcessFile")]
        public async Task Run([BlobTrigger("filecontainer/{name}", Connection = "AzureWebJobsStorage")] string inputBlob, string name)
        {

            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient("filecontainer");
                var blobClient = containerClient.GetBlobClient(name);

                var blobProperties = await blobClient.GetPropertiesAsync();

                if (blobProperties == null || blobProperties.Value == null)
                {
                    _logger.LogError($"Error processing blob: {name}, Exception: {name} does not exist");
                    return;
                }

                long blobSize = blobProperties.Value.ContentLength;
                var processedAt = blobProperties.Value.LastModified;
                
                _logger.LogInformation($"File Uploaded/Updated - Name: {name}, Size: {blobSize} bytes, Processed At: {processedAt}");

                var metadata = new FileMetadata
                {
                    PartitionKey = "File",
                    RowKey = name,
                    FileName = name,
                    Size = blobSize,
                    ProcessedAt = processedAt,
                    Action = "Uploaded",
                    FileUrl = blobClient.Uri.ToString()
                };

                await UpsertMetadataAsync(metadata);

                _logger.LogInformation($"Metadata saved for blob: {name}.");
            }
            catch(Exception ex)
            {
                _logger.LogError($"Error processing blob: {name}, Exception: {ex.Message}");
            }
        }

        private async Task UpsertMetadataAsync(FileMetadata metadata)
        {
            try
            {
                string tableName = Utilities.FileTableName;

                var tableClient = new TableClient(_connectionString, tableName);

                await tableClient.CreateIfNotExistsAsync();

                await tableClient.UpsertEntityAsync(metadata);

                _logger.LogInformation($"Metadata saved/updated for {metadata.FileName} in table {tableName}.");

            }
            catch (Exception ex)
            {
                _logger.LogError($"Error updating metadata table: {ex.Message}");
            }
        }
    }
}

