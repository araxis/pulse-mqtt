using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.IntegrationTests;

/// <summary>
/// Proves the TLS transport path against a real broker: a Mosquitto container is configured with a
/// TLS listener and a freshly generated self-signed certificate, and the client connects over TLS
/// and round-trips a QoS 1 message.
/// </summary>
public sealed class TlsIntegrationTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task A_qos1_message_round_trips_over_tls()
    {
        using var timeout = new CancellationTokenSource(TestTimeout);
        var (certPem, keyPem) = SelfSignedCertificate();

        const string config = """
            listener 8883
            allow_anonymous true
            certfile /certs/server.crt
            keyfile /certs/server.key
            """;

        var container = new ContainerBuilder("eclipse-mosquitto:2")
            .WithResourceMapping(Encoding.UTF8.GetBytes(config), "/mosquitto/config/tls.conf")
            .WithResourceMapping(Encoding.UTF8.GetBytes(certPem), "/certs/server.crt")
            .WithResourceMapping(Encoding.UTF8.GetBytes(keyPem), "/certs/server.key")
            .WithCommand("mosquitto", "-c", "/mosquitto/config/tls.conf")
            .WithPortBinding(8883, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(8883))
            .Build();

        await container.StartAsync(timeout.Token);
        try
        {
            var topic = $"tls/{Guid.NewGuid():N}";
            var factory = new TcpTransportFactory(new TcpTransportOptions
            {
                Host = container.Hostname,
                Port = container.GetMappedPublicPort(8883),
                UseTls = true,
                // The cert is self-signed and generated per run, so chain validation is bypassed
                // for the test — the point is to exercise the TLS handshake and byte path.
                ServerCertificateValidation = AcceptAnyCertificate,
            });

            await using var subscriber = new RawMqttClient(factory);
            await subscriber.ConnectAsync(new MqttConnectPacket { ClientId = $"tls-sub-{Guid.NewGuid():N}", KeepAliveSeconds = 30 }, timeout.Token);
            await subscriber.SubscribeAsync(
                [new MqttTopicFilter(topic) { MaximumQualityOfService = MqttQualityOfService.AtLeastOnce }],
                timeout.Token);

            await using var publisher = new RawMqttClient(factory);
            await publisher.ConnectAsync(new MqttConnectPacket { ClientId = $"tls-pub-{Guid.NewGuid():N}", KeepAliveSeconds = 30 }, timeout.Token);
            await publisher.PublishAsync(
                new MqttPublishPacket { Topic = topic, Payload = "secure"u8.ToArray(), QualityOfService = MqttQualityOfService.AtLeastOnce },
                timeout.Token);

            var received = await subscriber.Messages.ReadAsync(timeout.Token);
            received.Topic.ShouldBe(topic);
            Encoding.UTF8.GetString(received.Payload.Span).ShouldBe("secure");

            await subscriber.DisconnectAsync(timeout.Token);
            await publisher.DisconnectAsync(timeout.Token);
        }
        finally
        {
            await container.DisposeAsync();
        }
    }

    private static bool AcceptAnyCertificate(object sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors) => true;

    private static (string CertPem, string KeyPem) SelfSignedCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

        var subjectAlternativeNames = new SubjectAlternativeNameBuilder();
        subjectAlternativeNames.AddDnsName("localhost");
        subjectAlternativeNames.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(subjectAlternativeNames.Build());

        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        return (certificate.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }
}
