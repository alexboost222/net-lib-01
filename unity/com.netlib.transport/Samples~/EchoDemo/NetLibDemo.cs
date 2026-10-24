// netlib Echo Demo: one component that runs either a server or a client.
//
// Server: accepts clients, receives each player's position and sends every client the positions
// of all players. Run it in the Editor, as a desktop build or as a Linux Dedicated Server build
// (it starts as a server automatically in batch mode).
// Client: moves a cube with WASD / arrow keys and draws the other players as cubes.
using System;
using System.Collections.Generic;
using NetLib;
using UnityEngine;

public sealed class NetLibDemo : MonoBehaviour
{
    public enum Mode { Server, Client }

    [Tooltip("Server listens on this port; client connects to Address:Port.")]
    public Mode mode = Mode.Client;
    public string address = "127.0.0.1";
    public ushort port = 7777;
    [Tooltip("Ticks per second for Poll/Flush.")]
    public int tickRate = 64;
    public LogLevel logLevel = LogLevel.Info;

    private const uint ProtocolId = 0x4e4c4430; // "NLD0"
    private const byte ChannelState = 0;        // positions: UnreliableSequenced, latest only
    private const byte ChannelEvents = 1;       // join/leave: ReliableOrdered

    private NetManager _net;
    private PeerId _server;
    private bool _connected;
    private float _accumulator;
    private readonly byte[] _buffer = new byte[16];
    private readonly byte[] _state = new byte[20];
    private readonly Dictionary<ulong, Vector3> _positions = new Dictionary<ulong, Vector3>();
    private readonly Dictionary<ulong, GameObject> _cubes = new Dictionary<ulong, GameObject>();
    private readonly List<PeerId> _clients = new List<PeerId>();
    private GameObject _self;
    private ulong _selfId;
    private string _status = "";

    private static NetConfig Config(bool server) => new NetConfig
    {
        ProtocolId = ProtocolId,
        AcceptIncoming = server,
        Channels = new[]
        {
            new ChannelConfig(DeliveryMode.UnreliableSequenced, priority: 200),
            new ChannelConfig(DeliveryMode.ReliableOrdered),
        },
    };

    private void Start()
    {
        if (Application.isBatchMode) mode = Mode.Server;
        if (mode == Mode.Server)
        {
            _net = new NetManager(Config(true), $"0.0.0.0:{port}");
            _status = $"server on port {_net.LocalAddress.Port}";
        }
        else
        {
            _net = new NetManager(Config(false), "0.0.0.0:0");
            _server = _net.Connect($"{address}:{port}");
            _self = GameObject.CreatePrimitive(PrimitiveType.Cube);
            _self.GetComponent<Renderer>().material.color = Color.green;
            _status = $"connecting to {address}:{port}";
        }
        _net.SetLogLevel(logLevel);
        Debug.Log($"netlib {NetManager.Version}: {_status}");
    }

    private void Update()
    {
        if (_net == null) return;
        if (_self != null)
        {
            var input = new Vector3(Input.GetAxis("Horizontal"), 0, Input.GetAxis("Vertical"));
            _self.transform.position += input * (5f * Time.deltaTime);
        }
        _accumulator += Time.deltaTime;
        var step = 1f / tickRate;
        // One network tick per step; at most a few per frame after a hitch.
        for (var i = 0; i < 4 && _accumulator >= step; i++)
        {
            _accumulator -= step;
            Tick();
        }
        foreach (var kv in _positions)
        {
            if (kv.Key == _selfId && mode == Mode.Client) continue;
            if (!_cubes.TryGetValue(kv.Key, out var cube))
            {
                cube = GameObject.CreatePrimitive(PrimitiveType.Cube);
                _cubes[kv.Key] = cube;
            }
            cube.transform.position = Vector3.Lerp(cube.transform.position, kv.Value, 0.5f);
        }
    }

    private void Tick()
    {
        _net.Poll();
        while (_net.TryGetEvent(out var e))
        {
            switch (e.Kind)
            {
                case NetEventKind.ConnectRequest:
                    _net.Accept(e.Peer);
                    break;
                case NetEventKind.Connected:
                    if (mode == Mode.Server)
                    {
                        _clients.Add(e.Peer);
                        // Tell the client its player id (reliable).
                        BitConverter.TryWriteBytes(_state, e.Peer.Value);
                        _net.Send(e.Peer, ChannelEvents, new ReadOnlySpan<byte>(_state, 0, 8));
                    }
                    else
                    {
                        _connected = true;
                    }
                    break;
                case NetEventKind.Disconnected:
                    _clients.Remove(e.Peer);
                    if (_positions.Remove(e.Peer.Value) && _cubes.TryGetValue(e.Peer.Value, out var c)) Destroy(c);
                    if (mode == Mode.Client)
                    {
                        _connected = false;
                        _status = $"disconnected: {e.Reason}";
                    }
                    break;
                case NetEventKind.Message when e.Channel == ChannelState:
                    OnState(e.Peer, e.Data);
                    break;
                case NetEventKind.Message when e.Channel == ChannelEvents && e.Data.Length == 8:
                    _selfId = BitConverter.ToUInt64(e.Data);
                    break;
            }
        }
        while (_net.TryGetLog(out var log))
        {
            var text = $"[netlib {log.EventId:x8}] {log.Render()}";
            if (log.Level >= LogLevel.Warn) Debug.LogWarning(text);
            else Debug.Log(text);
        }
        if (mode == Mode.Server) ServerTick();
        else ClientTick();
        _net.Flush();
    }

    private void OnState(PeerId from, ReadOnlySpan<byte> data)
    {
        if (mode == Mode.Server)
        {
            if (data.Length < 12) return;
            _positions[from.Value] = new Vector3(Read(data, 0), Read(data, 4), Read(data, 8));
            return;
        }
        // Client: [player id u64][x][y][z]
        if (data.Length < 20) return;
        var id = BitConverter.ToUInt64(data.Slice(0, 8));
        _positions[id] = new Vector3(Read(data, 8), Read(data, 12), Read(data, 16));
    }

    private void ServerTick()
    {
        foreach (var client in _clients)
        {
            foreach (var kv in _positions)
            {
                BitConverter.TryWriteBytes(_state, kv.Key);
                Write(_state, 8, kv.Value);
                // The latest position of each player replaces an unsent older one.
                _net.Send(client, ChannelState, _state, SendOptions.Replace((uint)kv.Key));
            }
        }
        _status = $"server: {_clients.Count} clients";
    }

    private void ClientTick()
    {
        if (!_connected) return;
        Write(_buffer, 0, _self.transform.position);
        _net.Send(_server, ChannelState, new ReadOnlySpan<byte>(_buffer, 0, 12), SendOptions.Replace(0));
        if (_net.TryGetStats(_server, out var s)) _status = $"RTT {s.RttMicros / 1000.0:F1} ms, lost {s.PacketsLost}";
    }

    private static float Read(ReadOnlySpan<byte> d, int offset) => BitConverter.ToSingle(d.Slice(offset, 4));

    private static void Write(byte[] d, int offset, Vector3 v)
    {
        BitConverter.TryWriteBytes(new Span<byte>(d, offset, 4), v.x);
        BitConverter.TryWriteBytes(new Span<byte>(d, offset + 4, 4), v.y);
        BitConverter.TryWriteBytes(new Span<byte>(d, offset + 8, 4), v.z);
    }

    private void OnGUI() => GUI.Label(new Rect(10, 10, 600, 24), _status);

    private void OnApplicationPause(bool paused)
    {
        // Mobile: the OS may close the socket in background; a fresh one keeps the connection.
        if (!paused && _net != null && mode == Mode.Client) _net.Rebind();
    }

    private void OnDestroy()
    {
        if (_net == null) return;
        if (mode == Mode.Client && _connected) _net.Disconnect(_server);
        _net.Dispose();
        _net = null;
    }
}
