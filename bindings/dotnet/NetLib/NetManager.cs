using System;
using System.Text;

namespace NetLib
{
    /// <summary>
    /// A connection manager bound to a UDP socket. Call <see cref="Poll"/> at the start of a
    /// tick, drain <see cref="TryGetEvent"/>, queue messages with <see cref="Send(PeerId, byte, ReadOnlySpan{byte})"/>
    /// and call <see cref="Flush"/> at the end of the tick. Not thread-safe: use one thread.
    /// </summary>
    /// <remarks>Polling, sending and receiving do not allocate managed memory.</remarks>
    public sealed unsafe class NetManager : IDisposable
    {
        private IntPtr _handle;

        /// <summary>Binds a socket; use port 0 for clients, e.g. "0.0.0.0:0".</summary>
        public NetManager(NetConfig config, string bindAddress)
        {
            var channels = new Native.ChannelConfig[config.Channels.Length];
            for (var i = 0; i < channels.Length; i++)
            {
                var c = config.Channels[i];
                channels[i] = new Native.ChannelConfig { Mode = (byte)c.Mode, Priority = c.Priority, TtlMs = c.TtlMs };
            }
            var addr = Utf8(bindAddress);
            fixed (Native.ChannelConfig* ch = channels)
            fixed (byte* a = addr)
            {
                var nc = new Native.Config
                {
                    ProtocolId = config.ProtocolId,
                    Mtu = config.Mtu,
                    AcceptIncoming = config.AcceptIncoming ? (byte)1 : (byte)0,
                    ReceiveThread = config.ReceiveThread ? (byte)1 : (byte)0,
                    MaxIncoming = config.MaxIncoming,
                    Channels = ch,
                    ChannelCount = (uint)channels.Length,
                    TimeoutMs = config.TimeoutMs,
                    PingIntervalMs = config.PingIntervalMs,
                    ConnectTimeoutMs = config.ConnectTimeoutMs,
                    Bandwidth = config.Bandwidth,
                    StarvationTimeMs = config.StarvationTimeMs,
                    MaxReliableMessage = config.MaxReliableMessage,
                    MaxReliableQueue = config.MaxReliableQueue,
                };
                IntPtr handle;
                NetException.Check(Native.ManagerCreate(&nc, a, &handle));
                _handle = handle;
            }
        }

        /// <summary>Version of the native library.</summary>
        public static string Version
        {
            get
            {
                var p = Native.Version();
                var n = 0;
                while (p[n] != 0) n++;
                return Encoding.UTF8.GetString(p, n);
            }
        }

        private static byte[] Utf8(string s)
        {
            var bytes = new byte[Encoding.UTF8.GetByteCount(s) + 1];
            Encoding.UTF8.GetBytes(s, 0, s.Length, bytes, 0);
            return bytes;
        }

        private IntPtr Handle => _handle != IntPtr.Zero ? _handle : throw new ObjectDisposedException(nameof(NetManager));

        /// <summary>The bound address.</summary>
        public NetAddress LocalAddress
        {
            get
            {
                NetAddress a;
                NetException.Check(Native.LocalAddress(Handle, &a));
                return a;
            }
        }

        /// <summary>Receives, runs timers and collects events.</summary>
        public void Poll() => NetException.Check(Native.Poll(Handle));

        /// <summary>Takes the next event collected by <see cref="Poll"/>.</summary>
        public bool TryGetEvent(out NetEvent ev)
        {
            Native.Event e;
            var r = Native.NextEvent(Handle, &e);
            NetException.Check(r);
            ev = r == 1 ? new NetEvent(e) : default;
            return r == 1;
        }

        /// <summary>Packs queued messages into datagrams and sends them.</summary>
        public void Flush() => NetException.Check(Native.Flush(Handle));

        /// <summary>Starts connecting to an address such as "203.0.113.5:7777".</summary>
        public PeerId Connect(string address, ReadOnlySpan<byte> data = default)
        {
            var a = Utf8(address);
            ulong peer;
            fixed (byte* ap = a)
            fixed (byte* d = data)
            {
                NetException.Check(Native.Connect(Handle, ap, d, (uint)data.Length, &peer));
            }
            return new PeerId(peer);
        }

        /// <summary>Accepts a pending connection.</summary>
        public void Accept(PeerId peer) => NetException.Check(Native.Accept(Handle, peer.Value));

        /// <summary>Rejects a pending connection.</summary>
        public void Reject(PeerId peer, ReadOnlySpan<byte> data = default)
        {
            fixed (byte* d = data) NetException.Check(Native.Reject(Handle, peer.Value, d, (uint)data.Length));
        }

        /// <summary>Closes a connection.</summary>
        public void Disconnect(PeerId peer, ReadOnlySpan<byte> data = default)
        {
            fixed (byte* d = data) NetException.Check(Native.Disconnect(Handle, peer.Value, d, (uint)data.Length));
        }

        /// <summary>Queues a message with the channel defaults. Returns an error instead of throwing.</summary>
        public NetError Send(PeerId peer, byte channel, ReadOnlySpan<byte> data)
        {
            fixed (byte* d = data) return (NetError)Native.SendWith(Handle, peer.Value, channel, d, (uint)data.Length, null);
        }

        /// <summary>Queues a message with per-message options. Returns an error instead of throwing.</summary>
        public NetError Send(PeerId peer, byte channel, ReadOnlySpan<byte> data, in SendOptions options)
        {
            var o = new Native.SendOptions
            {
                Priority = options.Priority,
                TtlMs = options.TtlMs,
                HasReplaceKey = options.HasReplaceKey ? (byte)1 : (byte)0,
                ReplaceKey = options.ReplaceKey,
            };
            fixed (byte* d = data) return (NetError)Native.SendWith(Handle, peer.Value, channel, d, (uint)data.Length, &o);
        }

        /// <summary>Counters of a peer; false if the peer does not exist.</summary>
        public bool TryGetStats(PeerId peer, out PeerStats stats)
        {
            fixed (PeerStats* s = &stats) return Native.PeerStats(Handle, peer.Value, s) == 0;
        }

        /// <summary>Recreates the socket, e.g. after returning from background; connections continue.</summary>
        public void Rebind() => NetException.Check(Native.Rebind(Handle));

        /// <summary>Destroys the native manager.</summary>
        public void Dispose()
        {
            if (_handle != IntPtr.Zero)
            {
                Native.ManagerDestroy(_handle);
                _handle = IntPtr.Zero;
            }
            GC.SuppressFinalize(this);
        }

        /// <summary>Releases the native manager if Dispose was not called.</summary>
        ~NetManager() => Dispose();
    }
}
