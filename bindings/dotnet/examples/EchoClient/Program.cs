// C# client for the Rust echo server:
//   cargo run -p netlib --example echo_server
//   dotnet run --project bindings/dotnet/examples/EchoClient -- 127.0.0.1:7777 5
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using NetLib;

var server = args.Length > 0 ? args[0] : "127.0.0.1:7777";
var seconds = args.Length > 1 ? int.Parse(args[1]) : 5;
var config = new NetConfig
{
    ProtocolId = 0x4e455401,
    Channels = new[] { new ChannelConfig(DeliveryMode.Unreliable), new ChannelConfig(DeliveryMode.UnreliableSequenced) },
};
using var net = new NetManager(config, "0.0.0.0:0");
Console.WriteLine($"netlib {NetManager.Version}, connecting to {server}");
var peer = net.Connect(server, System.Text.Encoding.UTF8.GetBytes("echo"));
var clock = Stopwatch.StartNew();
var tick = TimeSpan.FromTicks(TimeSpan.TicksPerSecond / 64);
var next = clock.Elapsed;
var connected = false;
long sent = 0, echoed = 0;
var rtts = new List<double>();
var msg = new byte[48];
while (clock.Elapsed < TimeSpan.FromSeconds(seconds + 5))
{
    net.Poll();
    while (net.TryGetEvent(out var e))
    {
        switch (e.Kind)
        {
            case NetEventKind.Connected:
                Console.WriteLine($"connected in {clock.ElapsedMilliseconds} ms");
                connected = true;
                break;
            case NetEventKind.Disconnected:
                Console.WriteLine($"disconnected: {e.Reason}");
                return 1;
            case NetEventKind.Message when e.Data.Length >= 8:
                var t = BinaryPrimitives.ReadInt64LittleEndian(e.Data);
                rtts.Add((clock.Elapsed.Ticks - t) / (double)TimeSpan.TicksPerMillisecond);
                echoed++;
                break;
        }
    }
    if (connected)
    {
        if (sent >= seconds * 64) break;
        BinaryPrimitives.WriteInt64LittleEndian(msg, clock.Elapsed.Ticks);
        if (net.Send(peer, 0, msg) == NetError.Ok) sent++;
    }
    net.Flush();
    next += tick;
    var wait = next - clock.Elapsed;
    if (wait > TimeSpan.Zero) Thread.Sleep(wait);
}
// Collect the last echoes.
for (var i = 0; i < 32; i++)
{
    net.Poll();
    while (net.TryGetEvent(out var e)) if (e.Kind == NetEventKind.Message) echoed++;
    net.Flush();
    Thread.Sleep(tick);
}
rtts.Sort();
double P(int p) => rtts.Count == 0 ? 0 : rtts[(rtts.Count - 1) * p / 100];
net.TryGetStats(peer, out var stats);
Console.WriteLine($"sent {sent}, echoed {echoed}");
Console.WriteLine($"echo rtt p50 {P(50):F2} ms, p99 {P(99):F2} ms; transport rtt {stats.RttMicros / 1000.0:F2} ms");
Console.WriteLine($"packets sent {stats.PacketsSent}, received {stats.PacketsReceived}, bytes sent {stats.BytesSent}");
net.Disconnect(peer);
return echoed >= sent * 9 / 10 ? 0 : 2;
