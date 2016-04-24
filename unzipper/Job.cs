//Copyright (c) Microsoft Corporation

using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Batch.FileStaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace unzipper
{


    /// <summary>
    /// In this class, the Batch Service is used to process a set of input blobs that are zip files in parallel on multiple 
    /// compute nodes. Each task represents a single zip file.
    /// 
    /// A run-once job is created followed by multiple tasks which each task assigned to process a
    /// specific blob. It then waits for each of the tasks to complete where it prints out the results for
    /// each input blob.
    /// </summary>
    public static class Job
    {
        // files that are required on the compute nodes that run the tasks
        private const string UnzipperExeName = "unzipper.exe";
        private const string StorageClientDllName = "Microsoft.WindowsAzure.Storage.dll";

        public static void JobMain(string[] args)
        {
            //Load the configuration
            Settings unzipperSettings = Settings.Default;

            CloudStorageAccount cloudStorageAccount = new CloudStorageAccount(
                new StorageCredentials(
                    unzipperSettings.StorageAccountName,
                    unzipperSettings.StorageAccountKey),
                unzipperSettings.StorageServiceUrl,
                useHttps: true);

            StagingStorageAccount stagingStorageAccount = new StagingStorageAccount(
                unzipperSettings.StorageAccountName,
                unzipperSettings.StorageAccountKey,
                cloudStorageAccount.BlobEndpoint.ToString());

            using (BatchClient client = BatchClient.Open(new BatchSharedKeyCredentials(unzipperSettings.BatchServiceUrl, unzipperSettings.BatchAccountName, unzipperSettings.BatchAccountKey)))
            {
                string stagingContainer = null;
                //create pool
                CloudPool pool = CreatePool(unzipperSettings, client);

                try
                {
                    CreateJob(unzipperSettings, client);

                    List<CloudTask> tasksToRun = CreateTasks(unzipperSettings, stagingStorageAccount);

                    AddTasksToJob(unzipperSettings, client, stagingContainer, tasksToRun);

                    MonitorProgess(unzipperSettings, client);
                }
                finally
                {
                    Cleanup(unzipperSettings, client, stagingContainer);
                }
            }
        }

        private static void Cleanup(Settings unzipperSettings, BatchClient client, string stagingContainer)
        {
            //Delete the pool that we created
            if (unzipperSettings.ShouldDeletePool)
            {
                Console.WriteLine("Deleting pool: {0}", unzipperSettings.PoolId);
                client.PoolOperations.DeletePool(unzipperSettings.PoolId);
            }

            //Delete the job that we created
            if (unzipperSettings.ShouldDeleteJob)
            {
                Console.WriteLine("Deleting job: {0}", unzipperSettings.JobId);
                client.JobOperations.DeleteJob(unzipperSettings.JobId);
            }

            //Delete the containers we created
            if (unzipperSettings.ShouldDeleteContainer)
            {
                DeleteContainers(unzipperSettings, stagingContainer);
            }
        }

        private static void MonitorProgess(Settings unzipperSettings, BatchClient client)
        {
            //Get the job to monitor status.
            CloudJob job = client.JobOperations.GetJob(unzipperSettings.JobId);

            Console.Write("Waiting for tasks to complete ...   ");
            // Wait 120 minutes for all tasks to reach the completed state. The long timeout is necessary for the first
            // time a pool is created in order to allow nodes to be added to the pool and initialized to run tasks.
            IPagedEnumerable<CloudTask> ourTasks = job.ListTasks(new ODATADetailLevel(selectClause: "id"));
            client.Utilities.CreateTaskStateMonitor().WaitAll(ourTasks, TaskState.Completed, TimeSpan.FromMinutes(120));
            Console.WriteLine("tasks are done.");

            foreach (CloudTask t in ourTasks)
            {
                Console.WriteLine("Task " + t.Id);
                Console.WriteLine("stdout:" + Environment.NewLine + t.GetNodeFile(Microsoft.Azure.Batch.Constants.StandardOutFileName).ReadAsString());
                Console.WriteLine();
                Console.WriteLine("stderr:" + Environment.NewLine + t.GetNodeFile(Microsoft.Azure.Batch.Constants.StandardErrorFileName).ReadAsString());
            }
        }

        private static void AddTasksToJob(Settings unzipperSettings, BatchClient client, string stagingContainer, List<CloudTask> tasksToRun)
        {
            // Commit all the tasks to the Batch Service. Ask AddTask to return information about the files that were staged.
            // The container information is used later on to remove these files from Storage.
            ConcurrentBag<ConcurrentDictionary<Type, IFileStagingArtifact>> fsArtifactBag = new ConcurrentBag<ConcurrentDictionary<Type, IFileStagingArtifact>>();
            client.JobOperations.AddTask(unzipperSettings.JobId, tasksToRun, fileStagingArtifacts: fsArtifactBag);

            // loop through the bag of artifacts, looking for the one that matches our staged files. Once there,
            // capture the name of the container holding the files so they can be deleted later on if that option
            // was configured in the settings.
            foreach (var fsBagItem in fsArtifactBag)
            {
                IFileStagingArtifact fsValue;
                if (fsBagItem.TryGetValue(typeof(FileToStage), out fsValue))
                {
                    SequentialFileStagingArtifact stagingArtifact = fsValue as SequentialFileStagingArtifact;
                    if (stagingArtifact != null)
                    {
                        stagingContainer = stagingArtifact.BlobContainerCreated;
                        Console.WriteLine(
                            "Uploaded files to container: {0} -- you will be charged for their storage unless you delete them.",
                            stagingArtifact.BlobContainerCreated);
                    }
                }
            }
        }

        private static List<CloudTask> CreateTasks(Settings unzipperSettings, StagingStorageAccount stagingStorageAccount)
        {
            // create file staging objects that represent the executable and its dependent assembly to run as the task.
            // These files are copied to every node before the corresponding task is scheduled to run on that node.
            FileToStage unzipperExe = new FileToStage(UnzipperExeName, stagingStorageAccount);
            FileToStage storageDll = new FileToStage(StorageClientDllName, stagingStorageAccount);


            //get list of zipped files
            var zipFiles = GetZipFiles(unzipperSettings).ToList();
            Console.WriteLine("found {0} zipped files", zipFiles.Count);


            // initialize a collection to hold the tasks that will be submitted in their entirety. This will be one task per file.
            List<CloudTask> tasksToRun = new List<CloudTask>(zipFiles.Count);
            int i = 0;
            foreach (var zipFile in zipFiles)
            {
                CloudTask task = new CloudTask("task_no_" + i, String.Format("{0} --Task {1} {2} {3}",
                    UnzipperExeName,
                    zipFile.Uri,
                    unzipperSettings.StorageAccountName,
                    unzipperSettings.StorageAccountKey));

                //This is the list of files to stage to a container -- for each job, one container is created and 
                //files all resolve to Azure Blobs by their name (so two tasks with the same named file will create just 1 blob in
                //the container).
                task.FilesToStage = new List<IFileStagingProvider>
                                            {
                                                unzipperExe,
                                                storageDll
                                            };

                tasksToRun.Add(task);
                i++;
            }

            return tasksToRun;
        }

        private static void CreateJob(Settings unzipperSettings, BatchClient client)
        {
            Console.WriteLine("Creating job: " + unzipperSettings.JobId);
            // get an empty unbound Job
            CloudJob unboundJob = client.JobOperations.CreateJob();
            unboundJob.Id = unzipperSettings.JobId;
            unboundJob.PoolInformation = new PoolInformation() { PoolId = unzipperSettings.PoolId };

            // Commit Job to create it in the service
            unboundJob.Commit();
           
        }

        private static CloudPool CreatePool(Settings unzipperSettings, BatchClient client)
        {
            //OSFamily 4 == OS 2012 R2. You can learn more about os families and versions at:
            //http://msdn.microsoft.com/en-us/library/azure/ee924680.aspx
            CloudPool pool = client.PoolOperations.CreatePool(
                poolId: unzipperSettings.PoolId,
                targetDedicated: unzipperSettings.PoolNodeCount,
                virtualMachineSize: unzipperSettings.MachineSize,
                cloudServiceConfiguration: new CloudServiceConfiguration(osFamily: "4"));
            pool.MaxTasksPerComputeNode = unzipperSettings.MaxTasksPerNode;
            Console.WriteLine("Adding pool {0}", unzipperSettings.PoolId);

            try
            {
                pool.Commit();
            }
            catch (AggregateException ae)
            {
                // Go through all exceptions and dump useful information
                ae.Handle(x =>
                {
                    Console.Error.WriteLine("Creating pool ID {0} failed", unzipperSettings.PoolId);
                    if (x is BatchException)
                    {
                        BatchException be = x as BatchException;

                        Console.WriteLine(be.ToString());
                        Console.WriteLine();
                    }
                    else
                    {
                        Console.WriteLine(x);
                    }

                    // can't continue without a pool
                    return false;
                });
            }
            catch (BatchException be)
            {
                if (be.Message.Contains("conflict"))
                {
                    Console.WriteLine("pool already exists");
                }
            }

            return pool;
        }

        /// <summary>
        /// create a client for accessing blob storage
        /// </summary>
        private static CloudBlobClient GetCloudBlobClient(string accountName, string accountKey, string accountUrl)
        {
            StorageCredentials cred = new StorageCredentials(accountName, accountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(cred, accountUrl, useHttps: true);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();

            return client;
        }

         /// <summary>
        /// Delete the containers in Azure Storage which are created by this sample.
        /// </summary>
        private static void DeleteContainers(Settings unzipperSettings, string fileStagingContainer)
        {
            CloudBlobClient client = GetCloudBlobClient(
                unzipperSettings.StorageAccountName,
                unzipperSettings.StorageAccountKey,
                unzipperSettings.StorageServiceUrl);

            //Delete the file staging container
            if (!string.IsNullOrEmpty(fileStagingContainer))
            {
                CloudBlobContainer container = client.GetContainerReference(fileStagingContainer);
                Console.WriteLine("Deleting container: {0}", fileStagingContainer);
                container.DeleteIfExists();
            }
        }

        /// <summary>
        /// Gets all blobs in specified container
        /// </summary>
        /// <param name="unzipperSettings">The account settings.</param>
        /// <returns>The list of blob items blob.</returns>
        private static IEnumerable<IListBlobItem> GetZipFiles(Settings unzipperSettings)
        {
            CloudBlobClient client = GetCloudBlobClient(
                unzipperSettings.StorageAccountName,
                unzipperSettings.StorageAccountKey,
                unzipperSettings.StorageServiceUrl);
            var container = client.GetContainerReference(unzipperSettings.Container);
            var list = container.ListBlobs();

            return list;
        }
    }
}
