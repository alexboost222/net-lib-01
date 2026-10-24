# netlib Transport for Unity

UDP-транспорт для fast-paced мультиплеера: reliable и unreliable каналы, плотная упаковка пакетов с приоритетами, переживание смены сети, структурированные логи.
Ядро написано на Rust. C# API без управляемых аллокаций в рабочем цикле.

Требования: Unity 6 (6000.0+), Api Compatibility Level **.NET Standard 2.1** (по умолчанию). Работает и с Mono, и с IL2CPP.

## Установка

1. Возьмите `com.netlib.transport-<версия>.tgz`:
   - **CI:** артефакт `unity-package` workflow «Unity package» (GitHub → Actions → последний зелёный прогон). Внутри нативные библиотеки для всех платформ.
   - **Локально:** `cargo xtask unity-package`. Соберёт библиотеку только для текущей ОС; остальные платформы передаются через `--natives DIR`.
2. Unity → **Window → Package Manager → + → Add package from tarball…** → выберите `.tgz`.
3. **Project Settings → Player → Allow 'unsafe' Code** включать не нужно: unsafe-код живёт в сборке `NetLib` пакета, у неё своя настройка.

Пакет можно положить и распакованным в `Packages/com.netlib.transport` (embedded package): тогда исходники видны в проекте.

## Платформы

| Платформа | Плагин | Примечания |
|-----------|--------|------------|
| Windows x86_64 (Editor, Standalone) | `Plugins/Windows/x86_64/netlib_ffi.dll` | |
| macOS (Editor, Standalone) | `Plugins/macOS/libnetlib_ffi.dylib` | universal: arm64 + x86_64 |
| Linux x86_64 (Editor, Standalone, Dedicated Server) | `Plugins/Linux/x86_64/libnetlib_ffi.so` | |
| Android arm64-v8a, armeabi-v7a, x86_64 | `Plugins/Android/<abi>/libnetlib_ffi.so` | API 23+ (минимум Unity 6) |
| iOS arm64 | `Plugins/iOS/libnetlib_ffi.a` | статическая линковка, вызовы через `__Internal` |

Настройки импорта плагинов (платформа и CPU) уже прописаны в `.meta`-файлах, ничего настраивать не нужно.

## Использование

```csharp
using NetLib;

var config = new NetConfig
{
    ProtocolId = 0x12345678,              // одинаковый у клиента и сервера
    AcceptIncoming = isServer,
    Channels = new[]
    {
        new ChannelConfig(DeliveryMode.UnreliableSequenced, priority: 200), // 0: состояние
        new ChannelConfig(DeliveryMode.ReliableOrdered),                    // 1: события
    },
};
var net = new NetManager(config, isServer ? "0.0.0.0:7777" : "0.0.0.0:0");
var server = isServer ? default : net.Connect("203.0.113.5:7777");

// Каждый сетевой тик (например, 64 раза в секунду):
net.Poll();
while (net.TryGetEvent(out var e))
{
    switch (e.Kind)
    {
        case NetEventKind.ConnectRequest: net.Accept(e.Peer); break;
        case NetEventKind.Message: Handle(e.Peer, e.Channel, e.Data); break; // Data валидна до следующего Poll
    }
}
net.Send(server, 0, state, SendOptions.Replace(entityId)); // уходит только последнее состояние сущности
net.Flush();
```

- **Тики:** вызывайте `Poll` и `Flush` из своего цикла сетевых тиков с фиксированной частотой, а не обязательно раз в кадр. Пример — `Samples~/EchoDemo/NetLibDemo.cs`.
- **Потоки:** по умолчанию нативная библиотека принимает пакеты в своём фоновом потоке. Так время прихода точное, а RTT не зависит от FPS. Обработка событий при этом остаётся в главном потоке, колбэков нет.
- **Мобильные платформы:** при возврате из background вызовите `net.Rebind()` в `OnApplicationPause(false)`. Соединение продолжится без дисконнекта даже при смене сети Wi-Fi ↔ LTE.
- **Логи:** вызовите `net.SetLogLevel(LogLevel.Info)` и после каждого `Poll` выводите записи:
  ```csharp
  while (net.TryGetLog(out var r)) Debug.Log($"[{r.EventId:x8}] {r.Render()}");
  ```
  Каталог событий — `docs/LOG_EVENTS.md` в репозитории.

## Сэмпл

**Package Manager → netlib Transport → Samples → Echo Demo → Import.** Затем:

1. **Сервер:** создайте пустой GameObject, добавьте `NetLibDemo`, Mode = Server. Можно и сборкой: Linux **Dedicated Server**; в batch mode она сама запускается как сервер.
2. **Клиент:** в другом экземпляре (сборка под Windows, macOS, Android или iOS) Mode = Client, Address = адрес сервера. Кубик двигается WASD; кубики других игроков видны у всех. В углу экрана выводятся RTT и потери.

## Устранение проблем

- **`DllNotFoundException: netlib_ffi`:** в пакете нет плагина для этой платформы. Соберите пакет с `--natives` из CI.
- **iOS, `EntryPointNotFoundException`:** проверьте, что `libnetlib_ffi.a` включён для iOS в инспекторе плагина. В плеере iOS сборка `NetLib` использует `__Internal` автоматически.
- **Android, нет соединения:** в Player Settings → Android → Internet Access выберите Require.
