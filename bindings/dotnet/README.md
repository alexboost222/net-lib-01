# NetLib for .NET

Биндинг netlib для .NET Standard 2.1: работает в .NET 8, Mono и Unity (Mono и IL2CPP) и ни от одного из них не зависит.
Отправка, приём и `Poll` не выделяют управляемую память.

```csharp
var config = new NetConfig
{
    ProtocolId = 0x1234,
    Channels = new[]
    {
        new ChannelConfig(DeliveryMode.UnreliableSequenced, priority: 200), // позиции
        new ChannelConfig(DeliveryMode.ReliableOrdered),                    // события
    },
};
using var net = new NetManager(config, "0.0.0.0:0");
var server = net.Connect("203.0.113.5:7777");

// каждый тик:
net.Poll();
while (net.TryGetEvent(out var e))
{
    if (e.Kind == NetEventKind.Message) Handle(e.Channel, e.Data); // Data валидна до следующего Poll
}
net.Send(server, 0, state, SendOptions.Replace(entityId));         // только последняя версия
net.Flush();
```

## Нативная библиотека

`cargo build -p netlib-ffi --release` собирает:

- `libnetlib_ffi.so` (Linux, Android);
- `libnetlib_ffi.dylib` (macOS);
- `netlib_ffi.dll` (Windows);
- `libnetlib_ffi.a` (iOS, статическая линковка).

При статической линковке (iOS) задайте define `NETLIB_STATIC`: функции будут искаться в самом процессе (`__Internal`).

C-заголовок лежит в `bindings/c/netlib.h`, он генерируется командой `cargo xtask header`.

## Проверка

```sh
cargo build -p netlib-ffi
dotnet test bindings/dotnet/NetLib.Tests
cargo run -p netlib --example echo_server &
dotnet run --project bindings/dotnet/examples/EchoClient -- 127.0.0.1:7777 5
```
