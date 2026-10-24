using System;
using System.Collections.Generic;
using System.Threading;
using NetLib;
using Xunit;

public class ManagerTests
{
    private static readonly ChannelConfig[] Channels =
    {
        new ChannelConfig(DeliveryMode.Unreliable),
        new ChannelConfig(DeliveryMode.UnreliableSequenced, 200),
        new ChannelConfig(DeliveryMode.ReliableUnordered),
        new ChannelConfig(DeliveryMode.ReliableOrdered),
        new ChannelConfig(DeliveryMode.ReliableSequenced),
    };

    private static NetManager Create(bool server) =>
        new NetManager(new NetConfig { ProtocolId = 99, AcceptIncoming = server, Channels = Channels }, "127.0.0.1:0");

    /// <summary>A connected server/client pair.</summary>
    private sealed class Pair : IDisposable
    {
        public readonly NetManager Server = Create(true);
        public readonly NetManager Client = Create(false);
        public readonly PeerId ClientPeer;
        public PeerId ServerPeer;
        public bool ClientConnected;
        public bool ServerConnected;
        public readonly List<(byte Channel, byte[] Data)> ServerReceived = new List<(byte, byte[])>();

        public Pair()
        {
            ClientPeer = Client.Connect($"127.0.0.1:{Server.LocalAddress.Port}", new byte[] { 1, 2, 3 });
            var connected = false;
            for (var i = 0; i < 400 && !connected; i++)
            {
                Tick(collect: true);
                connected = ServerConnected && ClientConnected;
            }
            Assert.True(connected, "connected");
        }

        public void Tick(bool collect)
        {
            Server.Poll();
            while (Server.TryGetEvent(out var e))
            {
                switch (e.Kind)
                {
                    case NetEventKind.ConnectRequest:
                        Assert.Equal(new byte[] { 1, 2, 3 }, e.Data.ToArray());
                        Server.Accept(e.Peer);
                        break;
                    case NetEventKind.Connected:
                        ServerPeer = e.Peer;
                        ServerConnected = true;
                        break;
                    case NetEventKind.Message when collect:
                        ServerReceived.Add((e.Channel, e.Data.ToArray()));
                        break;
                }
            }
            Server.Flush();
            Client.Poll();
            while (Client.TryGetEvent(out var e))
            {
                if (e.Kind == NetEventKind.Connected) ClientConnected = true;
            }
            Client.Flush();
            Thread.Sleep(2);
        }

        public void Dispose()
        {
            Client.Dispose();
            Server.Dispose();
        }
    }

    [Fact]
    public void X3_VersionAndDefaults()
    {
        Assert.Equal("0.0.1", NetManager.Version);
        var c = new NetConfig();
        Assert.Equal(1200u, c.Mtu);
        Assert.True(c.ReceiveThread);
    }

    [Fact]
    public void X3_MessagesOnAllChannels()
    {
        using var p = new Pair();
        for (byte ch = 0; ch < 5; ch++)
        {
            Assert.Equal(NetError.Ok, p.Client.Send(p.ClientPeer, ch, new[] { ch, (byte)42 }));
        }
        var big = new byte[20_000];
        new Random(1).NextBytes(big);
        Assert.Equal(NetError.Ok, p.Client.Send(p.ClientPeer, 3, big));
        Assert.Equal(NetError.InvalidChannel, p.Client.Send(p.ClientPeer, 9, new byte[1]));
        Assert.Equal(NetError.TooLarge, p.Client.Send(p.ClientPeer, 0, new byte[5000]));
        Assert.Equal(NetError.InvalidOptions, p.Client.Send(p.ClientPeer, 3, new byte[1], SendOptions.Replace(1)));
        for (var i = 0; i < 100 && p.ServerReceived.Count < 6; i++) p.Tick(collect: true);
        Assert.Equal(6, p.ServerReceived.Count);
        for (byte ch = 0; ch < 5; ch++) Assert.Contains(p.ServerReceived, m => m.Channel == ch && m.Data[0] == ch);
        Assert.Contains(p.ServerReceived, m => m.Channel == 3 && m.Data.AsSpan().SequenceEqual(big));
        Assert.True(p.Client.TryGetStats(p.ClientPeer, out var stats));
        Assert.True(stats.RttMicros > 0 && stats.BytesSent > 20_000);
    }

    [Fact]
    public void X3_DisconnectReachesTheServer()
    {
        using var p = new Pair();
        p.Client.Disconnect(p.ClientPeer, new byte[] { 9 });
        var reason = DisconnectReason.None;
        byte[]? data = null;
        for (var i = 0; i < 100 && reason == DisconnectReason.None; i++)
        {
            p.Server.Poll();
            while (p.Server.TryGetEvent(out var e))
            {
                if (e.Kind == NetEventKind.Disconnected)
                {
                    reason = e.Reason;
                    data = e.Data.ToArray();
                }
            }
            Thread.Sleep(2);
        }
        Assert.Equal(DisconnectReason.Remote, reason);
        Assert.Equal(new byte[] { 9 }, data);
    }

    [Fact]
    public void X3_ErrorsAreReported()
    {
        var bad = new NetConfig { Mtu = 10 };
        var ex = Assert.Throws<NetException>(() => new NetManager(bad, "127.0.0.1:0"));
        Assert.Equal(NetError.Config, ex.Error);
        using var m = Create(false);
        Assert.Equal(NetError.NotConnected, m.Send(new PeerId(12345), 0, new byte[1]));
        m.Dispose();
        Assert.Throws<ObjectDisposedException>(() => m.Poll());
    }

    [Fact]
    public void P5_SteadyStateDoesNotAllocate()
    {
        using var p = new Pair();
        var payload = new byte[40];
        var state = new byte[24];
        long received = 0;
        void Step()
        {
            for (uint entity = 0; entity < 8; entity++)
            {
                p.Client.Send(p.ClientPeer, 1, state, SendOptions.Replace(entity));
            }
            p.Client.Send(p.ClientPeer, 0, payload);
            p.Client.Send(p.ClientPeer, 3, payload);
            p.Client.Flush();
            p.Server.Poll();
            while (p.Server.TryGetEvent(out var e))
            {
                if (e.Kind == NetEventKind.Message) received += e.Data.Length;
            }
            p.Server.Send(p.ServerPeer, 1, state);
            p.Server.Flush();
            p.Client.Poll();
            while (p.Client.TryGetEvent(out _)) { }
        }
        for (var i = 0; i < 500; i++) Step();
        Thread.Sleep(20);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 2000; i++) Step();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(received > 0);
        Assert.Equal(0, allocated);
    }
}
