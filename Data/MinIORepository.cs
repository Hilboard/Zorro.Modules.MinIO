using Microsoft.AspNetCore.Http;
using Minio;
using Minio.DataModel;
using Minio.DataModel.Args;
using Minio.Exceptions;
using static Zorro.Services.MinIOService;

namespace Zorro.Data;

public class MinIORepository : BucketRepository<IMinioClient, Bucket, Item>
{
    public MinIORepository(IMinioClient client, string defaultBucket) : base(client, defaultBucket)
    {
        //client.SetTraceOn();

        var listBucketsTask = client.ListBucketsAsync();
        listBucketsTask.Wait();
        if (listBucketsTask.Result.Buckets is null)
            throw new MinIOInitializeException();

        //client.SetTraceOff();
    }

    public override string GetFullPath(string path)
    {
        return $"{client.Config.Endpoint}/{bucket}/{path}";
    }

    public override async Task<bool> UploadAsync(IFormFile file, string path)
    {
        try
        {
            var request = new PutObjectArgs()
                .WithBucket(bucket)
                .WithObject(path)
                .WithObjectSize(file.Length)
                .WithStreamData(file.OpenReadStream())
                .WithContentType(file.ContentType);

            var response = await client.PutObjectAsync(request).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
            throw;
        }
    }

    public override async Task<bool> UploadAsync(Stream stream, string path, long contentLength, string contentType)
    {
        try
        {
            var request = new PutObjectArgs()
                .WithBucket(bucket)
                .WithObject(path)
                .WithObjectSize(contentLength)
                .WithStreamData(stream)
                .WithContentType(contentType);

            var response = await client.PutObjectAsync(request).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
            throw;
        }
    }

    public override async Task<bool> UploadAsync(string streamUri, string path)
    {
        try
        {
            using (var httpClient = new HttpClient())
            using (var httpResponse = await httpClient.GetAsync(streamUri, HttpCompletionOption.ResponseHeadersRead))
            using (var stream = await httpResponse.Content.ReadAsStreamAsync())
            {
                long? contentLength = httpResponse.Content.Headers.ContentLength;
                var contentType = httpResponse.Content.Headers.ContentType;

                if (contentLength.HasValue is false)
                    throw new Exception("Content length is not specified in response header");
                if (contentType is null)
                    throw new Exception("Content type is not specified in response header");

                var request = new PutObjectArgs()
                    .WithBucket(bucket)
                    .WithObject(path)
                    .WithObjectSize(contentLength.Value)
                    .WithStreamData(stream)
                    .WithContentType(contentType.ToString());

                var response = await client.PutObjectAsync(request).ConfigureAwait(false);
                return true;
            }
        }
        catch
        {
            return false;
            throw;
        }
    }

    public override async Task<bool> DeleteAsync(string path)
    {
        try
        {
            var request = new RemoveObjectArgs()
                .WithBucket(bucket)
                .WithObject(path);

            await client.RemoveObjectAsync(request).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
            throw;
        }
    }

    public override async Task<bool?> HasAsync(string path)
    {
        try
        {
            var request = new StatObjectArgs()
                .WithBucket(bucket)
                .WithObject(path);

            var response = await client.StatObjectAsync(request).ConfigureAwait(false);
            return true;
        }
        catch (ObjectNotFoundException)
        {
            return false;
        }
        catch
        {
            return null;
            throw;
        }
    }

    public override async Task<List<Item>?> ListAsync(string path)
    {
        try
        {
            var request = new ListObjectsArgs()
                .WithBucket(bucket)
                .WithPrefix(path);

            var response = client.ListObjectsEnumAsync(request);
            return response.ToBlockingEnumerable().ToList();
        }
        catch
        {
            return await Task.FromResult<List<Item>?>(null);
            throw;
        }
    }

    public override async Task<List<Bucket>?> ListBucketsAsync()
    {
        try
        {
            var response = await client.ListBucketsAsync().ConfigureAwait(false);
            return response.Buckets.ToList();
        }
        catch
        {
            return null;
            throw;
        }
    }


    public override async Task<bool?> BucketExistsAsync(string bucket)
    {
        try
        {
            var request = new BucketExistsArgs()
                .WithBucket(bucket);

            return await client.BucketExistsAsync(request).ConfigureAwait(false);
        }
        catch
        {
            return null;
            throw;
        }
    }
}