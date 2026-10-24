#nullable enable
using System;
using System.Runtime.InteropServices;

namespace NetLib
{
    /// <summary>Raw P/Invoke declarations of the netlib C ABI (bindings/c/netlib.h).</summary>
    /// <remarks>
    /// Define <c>NETLIB_STATIC</c> when the native library is linked statically into the
    /// executable (for example on iOS); the functions are then resolved from the process itself.
    /// </remarks>
    internal static unsafe class Native
    {
#if NETLIB_STATIC
        private const string Lib = "__Internal";
#else
        private const string Lib = "netlib_ffi";
#endif

        [StructLayout(LayoutKind.Sequential)]
        internal struct ChannelConfig
        {
            public byte Mode;
            public byte Priority;
            public uint TtlMs;
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct Config
        {
            public uint ProtocolId;
            public uint Mtu;
            public byte AcceptIncoming;
            public byte ReceiveThread;
            public uint MaxIncoming;
            public ChannelConfig* Channels;
            public uint ChannelCount;
            public uint TimeoutMs;
            public uint PingIntervalMs;
            public uint ConnectTimeoutMs;
            public uint Bandwidth;
            public uint StarvationTimeMs;
            public uint MaxReliableMessage;
            public uint MaxReliableQueue;
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct SendOptions
        {
            public short Priority;
            public uint TtlMs;
            public byte HasReplaceKey;
            public uint ReplaceKey;
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct Event
        {
            public uint Kind;
            public uint Reason;
            public ulong Peer;
            public byte Channel;
            public byte* Data;
            public uint DataLen;
            public NetAddress Addr;
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct LogProperty
        {
            public byte* Name;
            public uint NameLen;
            public byte Kind;
            public ulong U;
            public long I;
            public double F;
            public byte* Str;
            public uint StrLen;
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct LogRecord
        {
            public byte Level;
            public uint EventId;
            public ulong TimeUs;
            public byte* Template;
            public uint TemplateLen;
            public LogProperty* Properties;
            public uint PropertyCount;
        }

        [DllImport(Lib, EntryPoint = "netlib_set_log_level")]
        internal static extern int SetLogLevel(IntPtr manager, int level);

        [DllImport(Lib, EntryPoint = "netlib_next_log")]
        internal static extern int NextLog(IntPtr manager, LogRecord* record);

        [DllImport(Lib, EntryPoint = "netlib_config_default")]
        internal static extern int ConfigDefault(Config* config);

        [DllImport(Lib, EntryPoint = "netlib_manager_create")]
        internal static extern int ManagerCreate(Config* config, byte* bindAddr, IntPtr* manager);

        [DllImport(Lib, EntryPoint = "netlib_manager_destroy")]
        internal static extern void ManagerDestroy(IntPtr manager);

        [DllImport(Lib, EntryPoint = "netlib_poll")]
        internal static extern int Poll(IntPtr manager);

        [DllImport(Lib, EntryPoint = "netlib_next_event")]
        internal static extern int NextEvent(IntPtr manager, Event* ev);

        [DllImport(Lib, EntryPoint = "netlib_flush")]
        internal static extern int Flush(IntPtr manager);

        [DllImport(Lib, EntryPoint = "netlib_connect")]
        internal static extern int Connect(IntPtr manager, byte* addr, byte* data, uint len, ulong* peer);

        [DllImport(Lib, EntryPoint = "netlib_accept")]
        internal static extern int Accept(IntPtr manager, ulong peer);

        [DllImport(Lib, EntryPoint = "netlib_reject")]
        internal static extern int Reject(IntPtr manager, ulong peer, byte* data, uint len);

        [DllImport(Lib, EntryPoint = "netlib_disconnect")]
        internal static extern int Disconnect(IntPtr manager, ulong peer, byte* data, uint len);

        [DllImport(Lib, EntryPoint = "netlib_send_with")]
        internal static extern int SendWith(IntPtr manager, ulong peer, byte channel, byte* data, uint len, SendOptions* options);

        [DllImport(Lib, EntryPoint = "netlib_peer_stats")]
        internal static extern int PeerStats(IntPtr manager, ulong peer, PeerStats* stats);

        [DllImport(Lib, EntryPoint = "netlib_local_address")]
        internal static extern int LocalAddress(IntPtr manager, NetAddress* addr);

        [DllImport(Lib, EntryPoint = "netlib_rebind")]
        internal static extern int Rebind(IntPtr manager);

        [DllImport(Lib, EntryPoint = "netlib_version")]
        internal static extern byte* Version();
    }
}
