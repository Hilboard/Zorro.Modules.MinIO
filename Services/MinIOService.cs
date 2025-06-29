using Microsoft.Extensions.DependencyInjection;
using Minio;
using Zorro.Data;
using Zorro.Modules.MinIO;

namespace Zorro.Services;

public static class MinIOService
{
    public delegate MinIOSettings MinIOSettingsBuilder(MinIOSettings settings);
    public static MinIOSettingsBuilder? SettingsMaster { get; set; }

    public static MinIOSettings DefaultSettings { get; set; } = new();

    public static IServiceCollection AddMinIO(this IServiceCollection services)
    {
        MinIOSettings settings = DefaultSettings;
        if (SettingsMaster is not null)
            SettingsMaster.Invoke(settings);

        Action<IMinioClient> clientFactory = (client) =>
        {
            client.WithEndpoint(settings.endpoint)
                .WithCredentials(settings.accessKey, settings.secretKey)
                .WithSSL(settings.secure)
                .Build();
        };

        var client = new MinioClient();
        clientFactory(client);

        services.AddScoped(factory => new MinIORepository(client, settings.defaultBucket));

        services.AddMinio(clientFactory);

        return services;
    }

    public class MinIOInitializeException : Exception;
}
