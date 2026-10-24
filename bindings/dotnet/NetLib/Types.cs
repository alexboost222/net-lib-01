#nullable enable
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Runtime.InteropServices;

namespace NetLib
{
    /// <summary>Delivery guarantees of a channel.</summary>
    public enum DeliveryMode : byte
    {
        /// <summary>No guarantees; duplicates are filtered.</summary>
        Unreliable = 0,
        /// <summary>Unreliable; messages older than one already delivered are dropped.</summary>
        UnreliableSequenced = 1,
        /// <summary>Every message exactly once, in any order.</summary>
        ReliableUnordered = 2,
        /// <summary>Every message exactly once, in send order.</summary>
        ReliableOrdered = 3,
        /// <summary>The latest message is guaranteed; older ones may be skipped.</summary>
        ReliableSequenced = 4,
    }

    /// <summary>Result codes of the native library.</summary>
    public enum NetError
    {
        /// <summary>Success.</summary>
        Ok = 0,
        /// <summary>A pointer was null, a string invalid or a value out of range.</summary>
        InvalidArgument = -1,
        /// <summary>A socket operation failed.</summary>
        Io = -2,
        /// <summary>The configuration is invalid.</summary>
        Config = -3,
        /// <summary>The peer does not exist or is not connected.</summary>
        NotConnected = -4,
        /// <summary>No channel with this index.</summary>
        InvalidChannel = -5,
        /// <summary>The message is larger than the channel allows.</summary>
        TooLarge = -6,
        /// <summary>The reliable queue of the channel is full.</summary>
        QueueFull = -7,
        /// <summary>Send options do not apply to this channel.</summary>
        InvalidOptions = -8,
        /// <summary>The peer does not exist.</summary>
        UnknownPeer = -9,
        /// <summary>The peer is not in a state that allows the operation.</summary>
        InvalidState = -10,
        /// <summary>Connect, reject or disconnect data is too large.</summary>
        DataTooLarge = -11,
        /// <summary>A peer with this address already exists.</summary>
        AlreadyExists = -12,
        /// <summary>An internal error.</summary>
        Internal = -13,
    }

    /// <summary>An error returned by the native library.</summary>
    public sealed class NetException : Exception
    {
        /// <summary>The error code.</summary>
        public NetError Error { get; }

        internal NetException(NetError error) : base($"netlib error: {error}") => Error = error;

        internal static void Check(int code)
        {
            if (code < 0) throw new NetException((NetError)code);
        }
    }

    /// <summary>Kind of a <see cref="NetEvent"/>.</summary>
    public enum NetEventKind : uint
    {
        /// <summary>A client passed address validation; call Accept or Reject.</summary>
        ConnectRequest = 1,
        /// <summary>A connection was established.</summary>
        Connected = 2,
        /// <summary>A connection ended; see <see cref="NetEvent.Reason"/>.</summary>
        Disconnected = 3,
        /// <summary>A message arrived.</summary>
        Message = 4,
    }

    /// <summary>Why a peer went away.</summary>
    public enum DisconnectReason : uint
    {
        /// <summary>Not a disconnect event.</summary>
        None = 0,
        /// <summary>No packets for the configured timeout.</summary>
        Timeout = 1,
        /// <summary>The remote side disconnected; see the event data.</summary>
        Remote = 2,
        /// <summary>Disconnect was called locally.</summary>
        Local = 3,
        /// <summary>The connection attempt was not answered.</summary>
        ConnectTimeout = 4,
        /// <summary>The server rejected the connection; see the event data.</summary>
        Rejected = 5,
        /// <summary>The server has no free slots.</summary>
        ServerFull = 6,
        /// <summary>The server speaks another wire format version.</summary>
        VersionMismatch = 7,
        /// <summary>The client reconnected from the same address.</summary>
        Reconnected = 8,
    }

    /// <summary>Identifies a peer of a <see cref="NetManager"/>.</summary>
    public readonly struct PeerId : IEquatable<PeerId>
    {
        /// <summary>The raw id.</summary>
        public readonly ulong Value;

        /// <summary>Wraps a raw id.</summary>
        public PeerId(ulong value) => Value = value;

        /// <inheritdoc />
        public bool Equals(PeerId other) => Value == other.Value;

        /// <inheritdoc />
        public override bool Equals(object? obj) => obj is PeerId p && Equals(p);

        /// <inheritdoc />
        public override int GetHashCode() => Value.GetHashCode();

        /// <inheritdoc />
        public override string ToString() => $"Peer({Value:x})";

        /// <summary>Equality.</summary>
        public static bool operator ==(PeerId a, PeerId b) => a.Value == b.Value;

        /// <summary>Inequality.</summary>
        public static bool operator !=(PeerId a, PeerId b) => a.Value != b.Value;
    }

    /// <summary>A socket address in the native layout.</summary>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NetAddress
    {
        /// <summary>4 or 6; 0 if absent.</summary>
        public byte Family;
        /// <summary>IPv4 in the first 4 bytes, or IPv6.</summary>
        public fixed byte Ip[16];
        /// <summary>Port.</summary>
        public ushort Port;

        /// <summary>Converts to an <see cref="IPEndPoint"/> (allocates).</summary>
        public IPEndPoint? ToIPEndPoint()
        {
            if (Family == 0) return null;
            var bytes = new byte[Family == 4 ? 4 : 16];
            for (var i = 0; i < bytes.Length; i++) bytes[i] = Ip[i];
            return new IPEndPoint(new IPAddress(bytes), Port);
        }
    }

    /// <summary>Configuration of one channel.</summary>
    public struct ChannelConfig
    {
        /// <summary>Delivery guarantees.</summary>
        public DeliveryMode Mode;
        /// <summary>Base priority; higher goes first.</summary>
        public byte Priority;
        /// <summary>Unreliable channels: TTL of queued messages in ms; 0 means none.</summary>
        public uint TtlMs;

        /// <summary>A channel with priority 128 and a 1 s TTL for unreliable modes.</summary>
        public ChannelConfig(DeliveryMode mode, byte priority = 128)
        {
            Mode = mode;
            Priority = priority;
            TtlMs = mode <= DeliveryMode.UnreliableSequenced ? 1000u : 0u;
        }
    }

    /// <summary>Manager configuration; defaults come from the native library.</summary>
    public sealed class NetConfig
    {
        /// <summary>Application id; peers with different ids cannot connect.</summary>
        public uint ProtocolId;
        /// <summary>Maximum UDP payload in bytes.</summary>
        public uint Mtu;
        /// <summary>Accept incoming connections (server).</summary>
        public bool AcceptIncoming;
        /// <summary>Receive on a background native thread (exact arrival times).</summary>
        public bool ReceiveThread;
        /// <summary>Maximum number of incoming peers.</summary>
        public uint MaxIncoming;
        /// <summary>Channels, at most 16; empty means one unreliable channel.</summary>
        public ChannelConfig[] Channels = Array.Empty<ChannelConfig>();
        /// <summary>Time without packets until a connection drops, ms.</summary>
        public uint TimeoutMs;
        /// <summary>Keepalive and ping interval, ms.</summary>
        public uint PingIntervalMs;
        /// <summary>Time until an unanswered connection attempt fails, ms.</summary>
        public uint ConnectTimeoutMs;
        /// <summary>Bandwidth budget per peer in bytes per second; 0 is unlimited.</summary>
        public uint Bandwidth;
        /// <summary>Waiting time after which a queued message reaches the top priority, ms.</summary>
        public uint StarvationTimeMs;
        /// <summary>Largest reliable message in bytes.</summary>
        public uint MaxReliableMessage;
        /// <summary>Unacknowledged reliable messages per channel before sends fail.</summary>
        public uint MaxReliableQueue;

        /// <summary>Creates a configuration with the native defaults.</summary>
        public unsafe NetConfig()
        {
            Native.Config c;
            NetException.Check(Native.ConfigDefault(&c));
            ProtocolId = c.ProtocolId;
            Mtu = c.Mtu;
            AcceptIncoming = c.AcceptIncoming != 0;
            ReceiveThread = c.ReceiveThread != 0;
            MaxIncoming = c.MaxIncoming;
            TimeoutMs = c.TimeoutMs;
            PingIntervalMs = c.PingIntervalMs;
            ConnectTimeoutMs = c.ConnectTimeoutMs;
            Bandwidth = c.Bandwidth;
            StarvationTimeMs = c.StarvationTimeMs;
            MaxReliableMessage = c.MaxReliableMessage;
            MaxReliableQueue = c.MaxReliableQueue;
        }
    }

    /// <summary>Per-message send options.</summary>
    public struct SendOptions
    {
        /// <summary>Priority override; negative uses the channel priority.</summary>
        public short Priority;
        /// <summary>Unreliable only: TTL override in ms; 0 uses the channel TTL.</summary>
        public uint TtlMs;
        /// <summary>Unreliable only: replace a queued message with the same key.</summary>
        public bool HasReplaceKey;
        /// <summary>See <see cref="HasReplaceKey"/>.</summary>
        public uint ReplaceKey;

        /// <summary>Channel defaults.</summary>
        public static SendOptions Default => new SendOptions { Priority = -1 };

        /// <summary>Replace the queued message with the same key (latest state of an entity).</summary>
        public static SendOptions Replace(uint key) => new SendOptions { Priority = -1, HasReplaceKey = true, ReplaceKey = key };
    }

    /// <summary>Counters of a peer.</summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct PeerStats
    {
        /// <summary>Smoothed network RTT in microseconds; 0 until measured.</summary>
        public ulong RttMicros;
        /// <summary>Datagrams sent.</summary>
        public ulong PacketsSent;
        /// <summary>Datagrams received.</summary>
        public ulong PacketsReceived;
        /// <summary>Bytes sent.</summary>
        public ulong BytesSent;
        /// <summary>Bytes received.</summary>
        public ulong BytesReceived;
        /// <summary>Sent packets declared lost.</summary>
        public ulong PacketsLost;
        /// <summary>Reliable messages or pieces sent again.</summary>
        public ulong MessagesRetransmitted;
        /// <summary>Messages delivered to the application.</summary>
        public ulong MessagesReceived;
    }

    /// <summary>
    /// An event. <see cref="Data"/> points into native memory and is valid until the next
    /// <see cref="NetManager.Poll"/>; copy it if you need it longer.
    /// </summary>
    public readonly unsafe ref struct NetEvent
    {
        /// <summary>The kind.</summary>
        public readonly NetEventKind Kind;
        /// <summary>The peer.</summary>
        public readonly PeerId Peer;
        /// <summary>For messages: the channel.</summary>
        public readonly byte Channel;
        /// <summary>For disconnects: why.</summary>
        public readonly DisconnectReason Reason;
        /// <summary>Message, connect, reject or disconnect data.</summary>
        public readonly ReadOnlySpan<byte> Data;
        /// <summary>For connect requests and connections: the remote address.</summary>
        public readonly NetAddress Address;

        internal NetEvent(in Native.Event e)
        {
            Kind = (NetEventKind)e.Kind;
            Peer = new PeerId(e.Peer);
            Channel = e.Channel;
            Reason = Kind == NetEventKind.Disconnected ? (DisconnectReason)e.Reason : DisconnectReason.None;
            Data = e.Data == null ? ReadOnlySpan<byte>.Empty : new ReadOnlySpan<byte>(e.Data, (int)e.DataLen);
            Address = e.Addr;
        }
    }
}

namespace NetLib
{
    /// <summary>Severity of a log record.</summary>
    public enum LogLevel
    {
        /// <summary>Logging disabled.</summary>
        Off = -1,
        /// <summary>Per-datagram detail.</summary>
        Trace = 0,
        /// <summary>Drops, retransmissions, scheduler decisions.</summary>
        Debug = 1,
        /// <summary>Connection lifecycle and periodic statistics.</summary>
        Info = 2,
        /// <summary>Problems the connection survives or that have external causes.</summary>
        Warn = 3,
        /// <summary>Failures.</summary>
        Error = 4,
    }

    /// <summary>Type of a <see cref="LogProperty"/> value.</summary>
    public enum LogValueKind : byte
    {
        /// <summary><see cref="LogProperty.UInt64"/>.</summary>
        UInt64 = 0,
        /// <summary><see cref="LogProperty.Int64"/>.</summary>
        Int64 = 1,
        /// <summary><see cref="LogProperty.Double"/>; durations are milliseconds.</summary>
        Double = 2,
        /// <summary><see cref="LogProperty.Bool"/>.</summary>
        Bool = 3,
        /// <summary><see cref="LogProperty.Utf8"/>: text and addresses.</summary>
        String = 4,
    }

    /// <summary>A named, typed property of a log record. Valid while its record is.</summary>
    public readonly unsafe ref struct LogProperty
    {
        /// <summary>PascalCase name as UTF-8.</summary>
        public readonly ReadOnlySpan<byte> NameUtf8;
        /// <summary>Type of the value.</summary>
        public readonly LogValueKind Kind;
        /// <summary>Value for <see cref="LogValueKind.UInt64"/>.</summary>
        public readonly ulong UInt64;
        /// <summary>Value for <see cref="LogValueKind.Int64"/>.</summary>
        public readonly long Int64;
        /// <summary>Value for <see cref="LogValueKind.Double"/>.</summary>
        public readonly double Double;
        /// <summary>Value for <see cref="LogValueKind.String"/> as UTF-8.</summary>
        public readonly ReadOnlySpan<byte> Utf8;

        internal LogProperty(in Native.LogProperty p)
        {
            NameUtf8 = new ReadOnlySpan<byte>(p.Name, (int)p.NameLen);
            Kind = (LogValueKind)p.Kind;
            UInt64 = p.U;
            Int64 = p.I;
            Double = p.F;
            Utf8 = p.Str == null ? ReadOnlySpan<byte>.Empty : new ReadOnlySpan<byte>(p.Str, (int)p.StrLen);
        }

        /// <summary>Value for <see cref="LogValueKind.Bool"/>.</summary>
        public bool Bool => UInt64 != 0;

        /// <summary>The name (allocates).</summary>
        public string Name => Encoding.UTF8.GetString(NameUtf8);

        /// <summary>The value as text (allocates).</summary>
        public override string ToString() => Kind switch
        {
            LogValueKind.UInt64 => UInt64.ToString(System.Globalization.CultureInfo.InvariantCulture),
            LogValueKind.Int64 => Int64.ToString(System.Globalization.CultureInfo.InvariantCulture),
            LogValueKind.Double => Double.ToString("R", System.Globalization.CultureInfo.InvariantCulture),
            LogValueKind.Bool => Bool ? "true" : "false",
            _ => Encoding.UTF8.GetString(Utf8),
        };
    }

    /// <summary>
    /// A structured log record: a message template (https://messagetemplates.org/), a stable
    /// event id and typed properties in template order. Nothing is formatted on the native side.
    /// </summary>
    public readonly unsafe ref struct LogRecord
    {
        private static readonly Dictionary<uint, string> Templates = new Dictionary<uint, string>();
        private readonly Native.LogProperty* _props;

        /// <summary>Severity.</summary>
        public readonly LogLevel Level;
        /// <summary>Stable id of the template (docs/LOG_EVENTS.md).</summary>
        public readonly uint EventId;
        /// <summary>Microseconds since the manager was created.</summary>
        public readonly ulong TimeMicros;
        /// <summary>The template as UTF-8.</summary>
        public readonly ReadOnlySpan<byte> TemplateUtf8;
        /// <summary>Number of properties.</summary>
        public readonly int PropertyCount;

        internal LogRecord(in Native.LogRecord r)
        {
            Level = (LogLevel)r.Level;
            EventId = r.EventId;
            TimeMicros = r.TimeUs;
            TemplateUtf8 = new ReadOnlySpan<byte>(r.Template, (int)r.TemplateLen);
            PropertyCount = (int)r.PropertyCount;
            _props = r.Properties;
        }

        /// <summary>Property <paramref name="index"/> in template order.</summary>
        public LogProperty GetProperty(int index)
        {
            if ((uint)index >= (uint)PropertyCount) throw new ArgumentOutOfRangeException(nameof(index));
            return new LogProperty(_props[index]);
        }

        /// <summary>The template text; cached per event id, so it allocates once per event type.</summary>
        public string Template
        {
            get
            {
                lock (Templates)
                {
                    if (!Templates.TryGetValue(EventId, out var t))
                    {
                        t = Encoding.UTF8.GetString(TemplateUtf8);
                        Templates[EventId] = t;
                    }
                    return t;
                }
            }
        }

        /// <summary>The template with values in place of its holes (allocates).</summary>
        public string Render()
        {
            var t = Template;
            var sb = new StringBuilder(t.Length + 32);
            var hole = 0;
            for (var i = 0; i < t.Length; i++)
            {
                var c = t[i];
                if ((c == '{' || c == '}') && i + 1 < t.Length && t[i + 1] == c)
                {
                    sb.Append(c);
                    i++;
                    continue;
                }
                if (c != '{')
                {
                    sb.Append(c);
                    continue;
                }
                var close = t.IndexOf('}', i);
                if (close < 0)
                {
                    sb.Append(t, i, t.Length - i);
                    break;
                }
                if (hole < PropertyCount) sb.Append(GetProperty(hole).ToString());
                else sb.Append(t, i, close - i + 1);
                hole++;
                i = close;
            }
            return sb.ToString();
        }
    }
}
