using System.Buffers;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Runtime.Versioning;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Pulse.Mqtt.Codec;
using Pulse.Mqtt.Connection;
using Pulse.Mqtt.Packets;
using Pulse.Mqtt.Protocol;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

/// <summary>Runs only where QUIC is available; skips (visibly) elsewhere.</summary>
public sealed class QuicFactAttribute : FactAttribute
{
    public QuicFactAttribute()
    {
        if (!QuicTransportFactory.IsSupported)
        {
            Skip = "QUIC is not supported on this machine (OS or msquic missing).";
        }
    }
}

[SupportedOSPlatform("windows")]
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("macos")]
public sealed class QuicTransportTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(15);

    [QuicFact]
    public async Task Packets_round_trip_over_quic()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        using var certificate = CreateServerCertificate();
        await using var listener = await StartListenerAsync(certificate);

        var acceptTask = AcceptAsync(listener, timeout.Token);
        var factory = new QuicTransportFactory(TrustingOptions(listener));
        await using var transport = await factory.ConnectAsync(timeout.Token);

        // client -> server: a PINGREQ. Sent before accepting server-side because the peer only
        // learns about a QUIC stream once bytes flow on it.
        MqttPingCodec.WriteRequest(transport.Output);
        await transport.Output.FlushAsync(timeout.Token);

        var (serverConnection, serverStream) = await acceptTask.WaitAsync(timeout.Token);
        await using var ownedConnection = serverConnection;
        await using var ownedStream = serverStream;

        var received = await ReadExactAsync(serverStream, 2, timeout.Token);
        received.ShouldBe(new byte[] { 0xC0, 0x00 });

        // server -> client: a CONNACK
        var scratch = new ArrayBufferWriter<byte>();
        MqttConnAckCodec.Encode(scratch, new MqttConnAckPacket { SessionPresent = true });
        await serverStream.WriteAsync(scratch.WrittenMemory, timeout.Token);

        var result = await transport.Input.ReadAtLeastAsync(scratch.WrittenCount, timeout.Token);
        var bytes = result.Buffer.ToArray();
        var status = MqttFrameReader.TryReadFrame(bytes, out _, out var body, out _);
        status.ShouldBe(MqttFrameStatus.Complete);
        MqttConnAckCodec.Decode(body, MqttProtocolVersion.V500).SessionPresent.ShouldBeTrue();
        transport.Input.AdvanceTo(result.Buffer.End);
    }

    [QuicFact]
    public async Task The_raw_client_handshakes_over_quic()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        using var certificate = CreateServerCertificate();
        await using var listener = await StartListenerAsync(certificate);

        // The server side must stay open until the client has read the CONNACK; disposing it
        // inside the task can abort the connection before the bytes are consumed.
        var serverTask = Task.Run(async () =>
        {
            var (connection, stream) = await AcceptAsync(listener, timeout.Token);
            try
            {
                var connect = await ReadFrameAsync(stream, timeout.Token);
                var status = MqttFrameReader.TryReadFrame(connect, out _, out var body, out _);
                status.ShouldBe(MqttFrameStatus.Complete);
                MqttConnectCodec.Decode(body).ClientId.ShouldBe("quic-client");

                var scratch = new ArrayBufferWriter<byte>();
                MqttConnAckCodec.Encode(scratch, new MqttConnAckPacket());
                await stream.WriteAsync(scratch.WrittenMemory, timeout.Token);
            }
            catch
            {
                await stream.DisposeAsync();
                await connection.DisposeAsync();
                throw;
            }

            return (connection, stream);
        }, timeout.Token);

        var factory = new QuicTransportFactory(TrustingOptions(listener));
        await using var client = new RawMqttClient(factory);
        var connAck = await client.ConnectAsync(
            new MqttConnectPacket { ClientId = "quic-client", KeepAliveSeconds = 0 }, timeout.Token);

        connAck.ReasonCode.ShouldBe(MqttReasonCode.Success);
        var (serverConnection, serverStream) = await serverTask;
        await using var ownedConnection = serverConnection;
        await using var ownedStream = serverStream;
    }

    [QuicFact]
    public async Task A_rejected_server_certificate_fails_the_connection()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        using var certificate = CreateServerCertificate();
        await using var listener = await StartListenerAsync(certificate);
        DrainFailedAccept(listener, timeout.Token);

        var factory = new QuicTransportFactory(TrustingOptions(listener) with
        {
            ServerCertificateValidation = (_, _, _, _) => false,
        });

        await Should.ThrowAsync<AuthenticationException>(() => factory.ConnectAsync(timeout.Token).AsTask());
    }

    [QuicFact]
    public async Task An_alpn_mismatch_fails_the_connection()
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        using var certificate = CreateServerCertificate();
        await using var listener = await StartListenerAsync(certificate);
        DrainFailedAccept(listener, timeout.Token);

        var factory = new QuicTransportFactory(TrustingOptions(listener) with { Alpn = "not-mqtt" });

        // Proves the ALPN option reaches the handshake: the listener only speaks "mqtt".
        var exception = await Record.ExceptionAsync(() => factory.ConnectAsync(timeout.Token).AsTask());
        (exception is QuicException or AuthenticationException).ShouldBeTrue(exception?.ToString());
    }

    [Fact]
    public void The_default_port_and_alpn_match_broker_conventions()
    {
        var options = new QuicTransportOptions { Host = "broker" };
        options.Port.ShouldBe(14567);
        options.Alpn.ShouldBe("mqtt");
    }

    private static QuicTransportOptions TrustingOptions(QuicListener listener) => new()
    {
        Host = "127.0.0.1",
        Port = ((IPEndPoint)listener.LocalEndPoint).Port,
        // The listener uses a throwaway self-signed certificate, so chain validation must be
        // bypassed here; this also exercises the callback wiring.
        ServerCertificateValidation = (_, _, _, _) => true,
    };

    private static async Task<QuicListener> StartListenerAsync(X509Certificate2 certificate)
        => await QuicListener.ListenAsync(new QuicListenerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [new SslApplicationProtocol("mqtt")],
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(new QuicServerConnectionOptions
            {
                DefaultStreamErrorCode = 0,
                DefaultCloseErrorCode = 0,
                ServerAuthenticationOptions = new SslServerAuthenticationOptions
                {
                    ApplicationProtocols = [new SslApplicationProtocol("mqtt")],
                    ServerCertificate = certificate,
                },
            }),
        });

    private static async Task<(QuicConnection Connection, QuicStream Stream)> AcceptAsync(
        QuicListener listener, CancellationToken cancellationToken)
    {
        var connection = await listener.AcceptConnectionAsync(cancellationToken);
        var stream = await connection.AcceptInboundStreamAsync(cancellationToken);
        return (connection, stream);
    }

    /// <summary>Observes the accept that a failing handshake test triggers, so nothing leaks.</summary>
    private static void DrainFailedAccept(QuicListener listener, CancellationToken cancellationToken)
        => _ = Task.Run(async () =>
        {
            try
            {
                await using var connection = await listener.AcceptConnectionAsync(cancellationToken);
            }
            catch
            {
                // The handshake was rejected or the listener closed; both are the point.
            }
        }, CancellationToken.None);

    private static X509Certificate2 CreateServerCertificate()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1")], critical: false)); // server authentication
        var names = new SubjectAlternativeNameBuilder();
        names.AddIpAddress(IPAddress.Loopback);
        names.AddDnsName("localhost");
        request.CertificateExtensions.Add(names.Build());

        using var certificate = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(2));

        // Round-trip through PKCS#12 so the private key is persisted; Schannel (msquic on
        // Windows) cannot use the ephemeral key CreateSelfSigned produces.
        return X509CertificateLoader.LoadPkcs12(
            certificate.Export(X509ContentType.Pfx), password: null);
    }

    private static async Task<byte[]> ReadExactAsync(QuicStream stream, int count, CancellationToken cancellationToken)
    {
        var buffer = new byte[count];
        await stream.ReadExactlyAsync(buffer, cancellationToken);
        return buffer;
    }

    private static async Task<byte[]> ReadFrameAsync(QuicStream stream, CancellationToken cancellationToken)
    {
        var buffer = new byte[4096];
        var filled = 0;
        while (true)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(filled), cancellationToken);
            read.ShouldBeGreaterThan(0, "the stream closed before a full MQTT frame arrived");
            filled += read;
            if (MqttFrameReader.TryReadFrame(buffer.AsSpan(0, filled), out _, out _, out _) == MqttFrameStatus.Complete)
            {
                return buffer[..filled];
            }
        }
    }
}
