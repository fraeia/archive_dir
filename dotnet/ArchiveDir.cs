using System;
using System.IO;
using System.Data.SQLite;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Drawing;
using System.Drawing.Imaging;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using SkiaSharp;
using MediaToolkit;
using MediaToolkit.Model;
using MediaToolkit.Options;

namespace ArchiveDir
{
    class Program
    {
        static void Main(string[] args)
        {
            // Load environment variables from .env file if needed
            // using DotNetEnv;

            // DotNetEnv.Env.Load();

            // Parse command-line arguments
            var parser = new ArgumentParser(args);
            string srcDirectory = parser.GetValue("--src_directory") ?? Environment.GetEnvironmentVariable("SOURCE_DIRECTORY");
            string destDirectory = parser.GetValue("--dest_directory") ?? Environment.GetEnvironmentVariable("DESTINATION_DIRECTORY");
            string dbPath = parser.GetValue("--db_path") ?? Environment.GetEnvironmentVariable("DB_PATH");
            string azureContainer = parser.GetValue("--azure_container") ?? Environment.GetEnvironmentVariable("AZURE_CONTAINER");
            string azureConnectionString = parser.GetValue("--azure_connection_string") ?? Environment.GetEnvironmentVariable("AZURE_CONNECTION_STRING");

            DateTime startTime = DateTime.Now;
            Console.WriteLine($"Process started at: {startTime}");

            long originalSize = GetDirectorySize(srcDirectory);

            // Generate a timestamp
            string timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");

            // Open a single database connection
            string connectionString = $"Data Source={dbPath};Version=3;";
            using (var conn = new SQLiteConnection(connectionString))
            {
                conn.Open();

                // Redirect warnings or logs if necessary
                // Implement custom warning handling if required

                try
                {
                    // Compress files and save metadata to the database
                    CompressFilesAndSaveToDb(srcDirectory, destDirectory, conn, timestamp);

                    long compressedSize = GetDirectorySize(destDirectory);

                    DateTime endTime = DateTime.Now;
                    Console.WriteLine($"Process ended at: {endTime}");
                    Console.WriteLine($"Total duration: {endTime - startTime}");
                    Console.WriteLine($"Total original size: {FormatSize(originalSize)}");
                    Console.WriteLine($"Total compressed size: {FormatSize(compressedSize)}");

                    Console.WriteLine($"Directory tree saved to database: {dbPath}");

                    // Upload to Azure Blob Storage if specified
                    if (!string.IsNullOrEmpty(azureContainer) && !string.IsNullOrEmpty(azureConnectionString))
                    {
                        UploadToAzure(azureContainer, azureConnectionString, destDirectory, timestamp, conn);
                        Console.WriteLine($"Files uploaded to Azure Blob Storage container: {azureContainer}");

                        // Remove the created destination files and directory
                        RemoveDirectory(destDirectory);
                        Console.WriteLine($"Removed destination directory: {destDirectory}");
                    }
                }
                finally
                {
                    conn.Close();
                }
            }
        }

        static long GetDirectorySize(string directory)
        {
            long totalSize = 0;
            foreach (string file in Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories))
            {
                FileInfo fi = new FileInfo(file);
                totalSize += fi.Length;
            }
            return totalSize;
        }

        static string FormatSize(long size)
        {
            string[] units = { "B", "KB", "MB", "GB", "TB" };
            double sizeD = size;
            int unit = 0;
            while (sizeD >= 1024 && unit < units.Length - 1)
            {
                sizeD /= 1024;
                unit++;
            }
            return $"{sizeD:F2} {units[unit]}";
        }

        static void CompressFilesAndSaveToDb(string srcDir, string destDir, SQLiteConnection conn, string timestamp)
        {
            var cursor = conn.CreateCommand();

            // Create tables
            cursor.CommandText = @"
                CREATE TABLE IF NOT EXISTS files (
                    id TEXT PRIMARY KEY,
                    filename TEXT,
                    filepath TEXT,
                    content_type TEXT,
                    size INTEGER,
                    creation_time TEXT,
                    modification_time TEXT,
                    thumbnail BLOB,
                    is_duplicate INTEGER,
                    original_path TEXT,
                    batch TEXT
                );
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    event_type TEXT,
                    file_path TEXT,
                    message TEXT,
                    timestamp TEXT
                );
            ";
            cursor.ExecuteNonQuery();

            var files = Directory.GetFiles(srcDir, "*", SearchOption.AllDirectories);
            int totalItems = files.Length;
            int processedItems = 0;

            foreach (var filePath in files)
            {
                string relativePath = Path.GetRelativePath(srcDir, filePath);
                string destPath = Path.Combine(destDir, relativePath);
                string destDirPath = Path.GetDirectoryName(destPath);

                if (!Directory.Exists(destDirPath))
                {
                    Directory.CreateDirectory(destDirPath);
                }

                try
                {
                    // Check for duplicates in the database
                    FileInfo fileInfo = new FileInfo(filePath);
                    string originalFilename = fileInfo.Name;
                    string contentType = MimeMapping.GetMimeMapping(filePath);
                    long size = fileInfo.Length;
                    string creationTime = fileInfo.CreationTimeUtc.ToString("o");
                    string modificationTime = fileInfo.LastWriteTimeUtc.ToString("o");

                    var checkDuplicateCmd = conn.CreateCommand();
                    checkDuplicateCmd.CommandText = @"
                        SELECT filepath FROM files WHERE filename = @filename AND size = @size AND creation_time = @creation_time AND modification_time = @modification_time
                    ";
                    checkDuplicateCmd.Parameters.AddWithValue("@filename", originalFilename);
                    checkDuplicateCmd.Parameters.AddWithValue("@size", size);
                    checkDuplicateCmd.Parameters.AddWithValue("@creation_time", creationTime);
                    checkDuplicateCmd.Parameters.AddWithValue("@modification_time", modificationTime);
                    var originalRecord = checkDuplicateCmd.ExecuteScalar();
                    bool isDuplicate = originalRecord != null;

                    if (isDuplicate)
                    {
                        string originalPath = originalRecord.ToString();
                        var insertDuplicateCmd = conn.CreateCommand();
                        insertDuplicateCmd.CommandText = @"
                            INSERT INTO files (id, filename, filepath, content_type, size, creation_time, modification_time, thumbnail, is_duplicate, original_path, batch)
                            VALUES (@id, @filename, @filepath, @content_type, @size, @creation_time, @modification_time, @thumbnail, @is_duplicate, @original_path, @batch)
                        ";
                        insertDuplicateCmd.Parameters.AddWithValue("@id", Guid.NewGuid().ToString());
                        insertDuplicateCmd.Parameters.AddWithValue("@filename", originalFilename);
                        insertDuplicateCmd.Parameters.AddWithValue("@filepath", (relativePath + ".7z").Replace("\\", "/"));
                        insertDuplicateCmd.Parameters.AddWithValue("@content_type", contentType);
                        insertDuplicateCmd.Parameters.AddWithValue("@size", size);
                        insertDuplicateCmd.Parameters.AddWithValue("@creation_time", creationTime);
                        insertDuplicateCmd.Parameters.AddWithValue("@modification_time", modificationTime);
                        insertDuplicateCmd.Parameters.AddWithValue("@thumbnail", DBNull.Value);
                        insertDuplicateCmd.Parameters.AddWithValue("@is_duplicate", 1);
                        insertDuplicateCmd.Parameters.AddWithValue("@original_path", originalPath);
                        insertDuplicateCmd.Parameters.AddWithValue("@batch", timestamp);
                        insertDuplicateCmd.ExecuteNonQuery();

                        processedItems++;
                        continue;
                    }

                    // Compress the file with fastest compression
                    string zipFilePath = destPath + ".7z";
                    CompressFile(filePath, zipFilePath);

                    // Generate thumbnail
                    byte[] thumbnail = null;
                    if (contentType.StartsWith("image/") || contentType.StartsWith("video/"))
                    {
                        thumbnail = GenerateThumbnail(filePath, contentType);
                    }

                    // Save file record to database
                    var insertFileCmd = conn.CreateCommand();
                    insertFileCmd.CommandText = @"
                        INSERT INTO files (id, filename, filepath, content_type, size, creation_time, modification_time, thumbnail, is_duplicate, original_path, batch)
                        VALUES (@id, @filename, @filepath, @content_type, @size, @creation_time, @modification_time, @thumbnail, @is_duplicate, @original_path, @batch)
                    ";
                    insertFileCmd.Parameters.AddWithValue("@id", Guid.NewGuid().ToString());
                    insertFileCmd.Parameters.AddWithValue("@filename", originalFilename);
                    insertFileCmd.Parameters.AddWithValue("@filepath", (relativePath + ".7z").Replace("\\", "/"));
                    insertFileCmd.Parameters.AddWithValue("@content_type", contentType);
                    insertFileCmd.Parameters.AddWithValue("@size", size);
                    insertFileCmd.Parameters.AddWithValue("@creation_time", creationTime);
                    insertFileCmd.Parameters.AddWithValue("@modification_time", modificationTime);
                    insertFileCmd.Parameters.AddWithValue("@thumbnail", thumbnail ?? (object)DBNull.Value);
                    insertFileCmd.Parameters.AddWithValue("@is_duplicate", 0);
                    insertFileCmd.Parameters.AddWithValue("@original_path", filePath);
                    insertFileCmd.Parameters.AddWithValue("@batch", timestamp);
                    insertFileCmd.ExecuteNonQuery();

                    processedItems++;
                    Console.WriteLine($"Processed {processedItems}/{totalItems}: {filePath}");
                }
                catch (Exception e)
                {
                    LogEvent("Error processing file", filePath, e.Message, conn);
                }
            }
        }

        static void CompressFile(string filePath, string zipFilePath)
        {
            string compressionLevel = Environment.GetEnvironmentVariable("COMPRESSION_LEVEL") ?? "1";
            var processInfo = new ProcessStartInfo
            {
                FileName = "7z",
                Arguments = $"a -t7z -mx={compressionLevel} \"{zipFilePath}\" \"{filePath}\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
            var process = Process.Start(processInfo);
            process.WaitForExit();
            if (process.ExitCode != 0)
            {
                throw new Exception($"7z compression failed: {process.StandardError.ReadToEnd()}");
            }
        }

        static byte[] GenerateThumbnail(string filePath, string contentType)
        {
            try
            {
                if (contentType.StartsWith("image/"))
                {
                    using (var input = File.OpenRead(filePath))
                    using (var original = SKBitmap.Decode(input))
                    {
                        int width = 128;
                        int height = 128;
                        var resizeInfo = new SKImageInfo(width, height);
                        var resized = original.Resize(resizeInfo, SKFilterQuality.High);
                        using (var image = SKImage.FromBitmap(resized))
                        using (var data = image.Encode(SKEncodedImageFormat.Png, 100))
                        {
                            return data.ToArray();
                        }
                    }
                }
                else if (contentType.StartsWith("video/"))
                {
                    var inputFile = new MediaFile { Filename = filePath };
                    using (var engine = new Engine())
                    {
                        engine.GetMetadata(inputFile);
                        var options = new ConversionOptions { Seek = TimeSpan.FromSeconds(1) };
                        var outputFile = new MediaFile { Filename = Path.GetTempFileName() };
                        engine.GetThumbnail(inputFile, outputFile, options);
                        byte[] thumbnail = File.ReadAllBytes(outputFile.Filename);
                        File.Delete(outputFile.Filename);
                        return thumbnail;
                    }
                }
            }
            catch (Exception)
            {
                // Handle exceptions if needed
            }
            return null;
        }

        static void LogEvent(string eventType, string filePath, string message, SQLiteConnection conn)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                INSERT INTO events (id, event_type, file_path, message, timestamp)
                VALUES (@id, @event_type, @file_path, @message, @timestamp)
            ";
            cmd.Parameters.AddWithValue("@id", Guid.NewGuid().ToString());
            cmd.Parameters.AddWithValue("@event_type", eventType);
            cmd.Parameters.AddWithValue("@file_path", filePath);
            cmd.Parameters.AddWithValue("@message", message);
            cmd.Parameters.AddWithValue("@timestamp", DateTime.Now.ToString("o"));
            cmd.ExecuteNonQuery();
        }

        static void UploadToAzure(string containerName, string connectionString, string destDir, string timestamp, SQLiteConnection conn)
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);

            var files = Directory.GetFiles(destDir, "*", SearchOption.AllDirectories);
            int totalFiles = files.Length;
            int uploadedFiles = 0;

            foreach (var file in files)
            {
                string relativePath = Path.GetRelativePath(destDir, file).Replace("\\", "/");
                string blobPath = $"{timestamp}/{relativePath}";
                BlobClient blobClient = containerClient.GetBlobClient(blobPath);

                try
                {
                    Console.WriteLine($"Uploading: {file} -> {blobPath}");
                    using (FileStream fs = File.OpenRead(file))
                    {
                        blobClient.Upload(fs, true);
                    }
                    uploadedFiles++;
                    Console.WriteLine($"Uploaded {uploadedFiles}/{totalFiles}");
                }
                catch (Exception e)
                {
                    LogEvent("Error uploading file to Azure", file, e.Message, conn);
                }
            }
        }

        static void RemoveDirectory(string directory)
        {
            Directory.Delete(directory, true);
        }
    }

    class ArgumentParser
    {
        private readonly Dictionary<string, string> argsDict = new Dictionary<string, string>();

        public ArgumentParser(string[] args)
        {
            string key = null;
            foreach (var arg in args)
            {
                if (arg.StartsWith("--"))
                {
                    key = arg;
                }
                else if (key != null)
                {
                    argsDict[key] = arg;
                    key = null;
                }
            }
        }

        public string GetValue(string key)
        {
            argsDict.TryGetValue(key, out string value);
            return value;
        }
    }
}