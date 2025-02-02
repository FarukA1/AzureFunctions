using System;
using Azure;
using Azure.Data.Tables;

namespace AzureFunctions.Blob.Models
{
	public class FileMetadata : ITableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; } 
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }

        public string FileName { get; set; }
        public long Size { get; set; }
        public DateTimeOffset ProcessedAt { get; set; }
        public string Action { get; set; }
        public string FileUrl { get; set; }
    }
}

