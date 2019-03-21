## Azure Cosmos DB - Custom Point in Time Restore
This project provides a custom implementation for point in time restore, for an Azure Cosmos DB container. This solution leverages the Cosmos DB Change Feed Processor library, to constantly poll for changes on the source container and back up these changes to a specified Azure Blob Storage account. These backups are compressed before they are backup to the Blob Storage Account.

When a Restore is triggered, the backed up changes are simply replayed in the order in which they came in, to restore the data as it appeared at a specified point in time. The compressed blobs are decompressed and restored to the specified Cosmos DB restore container.
