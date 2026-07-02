using System.Buffers;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Pulse.Mqtt.Transport;
using Shouldly;
using Xunit;

namespace Pulse.Mqtt.Client.Tests;

// Liveness regressions for the msquic 30s default idle timeout: each case must outlast a full
// idle window, so they run ~35s and stay out of the fast lane (Category=Soak, run on demand).
[Trait("Category", "Soak")]
[SupportedOSPlatform("windows")]
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("macos")]
public sealed class QuicLivenessTests
{
    [QuicFact]
    public async Task Default_options_survive_a_35s_idle_window()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        using var certificate = CreateServerCertificate();
        await using var listener = await StartListenerAsync(certificate);

        // The effective QUIC idle timeout is the minimum both peers advertise, so the client-side
        // default can only be proven against a server that does not idle out on its own.
        var factory = new QuicTransportFactory(new QuicTransportOptions
        {
            Host = "127.0.0.1",
            Port = ((IPEndPoint)listener.LocalEndPoint).Port,
            ServerCertificateValidation = (_, _, _, _) => true,
        });
        await using var transport = await factory.ConnectAsync(timeout.Token);
        transport.Output.Write("x"u8);
        await transport.Output.FlushAsync(timeout.Token);
        var (connection, stream) = await AcceptAsync(listener, timeout.Token);
        await using var ownedConnection = connection;
        await using var ownedStream = stream;
        var first = new byte[1];
        await stream.ReadExactlyAsync(first, timeout.Token);

        await Task.Delay(TimeSpan.FromSeconds(35), timeout.Token);

        // Past msquic's 30s default idle window, the connection must still carry data both ways.
        transport.Output.Write("y"u8);
        await transport.Output.FlushAsync(timeout.Token);
        var second = new byte[1];
        await stream.ReadExactlyAsync(second, timeout.Token);
        second[0].ShouldBe((byte)'y');

        await stream.WriteAsync(new byte[] { (byte)'z' }, timeout.Token);
        var result = await transport.Input.ReadAtLeastAsync(1, timeout.Token);
        result.Buffer.FirstSpan[0].ShouldBe((byte)'z');
        transport.Input.AdvanceTo(result.Buffer.End);
    }

    [QuicFact]
    public async Task Keep_alive_pings_survive_a_server_side_idle_timeout()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        using var certificate = CreateServerCertificate();
        // The server keeps msquic's default 30s idle timeout here — the client's QUIC-level
        // pings must reset it.
        await using var listener = await StartListenerAsync(certificate, serverIdleTimeout: TimeSpan.Zero);

        var factory = new QuicTransportFactory(new QuicTransportOptions
        {
            Host = "127.0.0.1",
            Port = ((IPEndPoint)listener.LocalEndPoint).Port,
            ServerCertificateValidation = (_, _, _, _) => true,
            KeepAliveInterval = TimeSpan.FromSeconds(10),
        });
        await using var transport = await factory.ConnectAsync(timeout.Token);
        transport.Output.Write("x"u8);
        await transport.Output.FlushAsync(timeout.Token);
        var (connection, stream) = await AcceptAsync(listener, timeout.Token);
        await using var ownedConnection = connection;
        await using var ownedStream = stream;
        var first = new byte[1];
        await stream.ReadExactlyAsync(first, timeout.Token);

        await Task.Delay(TimeSpan.FromSeconds(35), timeout.Token);

        transport.Output.Write("y"u8);
        await transport.Output.FlushAsync(timeout.Token);
        var second = new byte[1];
        await stream.ReadExactlyAsync(second, timeout.Token);
        second[0].ShouldBe((byte)'y');
    }

    private static async Task<QuicListener> StartListenerAsync(X509Certificate2 certificate)
        => await StartListenerAsync(certificate, Timeout.InfiniteTimeSpan);

    private static async Task<QuicListener> StartListenerAsync(X509Certificate2 certificate, TimeSpan serverIdleTimeout)
        => await QuicListener.ListenAsync(new QuicListenerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [new SslApplicationProtocol("mqtt")],
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(new QuicServerConnectionOptions
            {
                DefaultStreamErrorCode = 0,
                DefaultCloseErrorCode = 0,
                IdleTimeout = serverIdleTimeout,
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

    private static X509Certificate2 CreateServerCertificate()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], critical: false));
        var names = new SubjectAlternativeNameBuilder();
        names.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(names.Build());
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(2));
        return X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pfx), password: null);
    }
}
