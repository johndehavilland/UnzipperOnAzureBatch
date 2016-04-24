using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.IO.Compression;

namespace unzipper
{
    public class UnzipperTask
    {
        public static void TaskMain(string[] args)
        {

            if (args == null || args.Length != 4)
            {
                throw new Exception("Usage: unzipper.exe --Task <blobpath> <storageAccountName> <storageAccountKey>");
            }

            string blobName = args[1];
            string storageAccountName = args[2];
            string storageAccountKey = args[3];

            var storageCred = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudBlockBlob blob = new CloudBlockBlob(new Uri(blobName), storageCred);


            StorageCredentials cred = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(cred, useHttps: true);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer outputContainer = client.GetContainerReference("unzipped");
            outputContainer.CreateIfNotExists();


            using (Stream memoryStream = new MemoryStream())
            {

                blob.DownloadToStream(memoryStream);
                memoryStream.Position = 0; //Reset the stream

                ZipArchive archive = new ZipArchive(memoryStream);
                Console.WriteLine("Extracting {0} which contains {1} files", blobName, archive.Entries.Count);
                foreach (ZipArchiveEntry entry in archive.Entries)
                {
                    CloudBlockBlob blockBlob = outputContainer.GetBlockBlobReference(entry.Name);

                    blockBlob.UploadFromStream(entry.Open());
                    Console.WriteLine("Uploaded {0}", entry.Name);
                }
            }
        }
    }
}
