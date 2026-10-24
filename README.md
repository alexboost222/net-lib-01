# netlib

UDP-транспорт на Rust для fast-paced мультиплеера (FPS) в духе LiteNetLib. Главные цели:

- минимум накладных байт на проводе;
- детерминированное sans-IO ядро;
- C ABI и .NET-биндинг для Unity, при этом сама библиотека от Unity не зависит.

Документы:

- [docs/PLAN.md](docs/PLAN.md) — критерии приёмки и вехи;
- [docs/WIRE_FORMAT.md](docs/WIRE_FORMAT.md) — формат пакетов.

## Статус

**M2**: все пять режимов доставки (`Unreliable`, `UnreliableSequenced`, `ReliableUnordered`, `ReliableOrdered`, `ReliableSequenced`).
Повторная отправка по порогу пакетов и по RTO с backoff, фрагментация до 64 КиБ, сетевой RTT без задержки подтверждения (`ack_delay`, защита как в QUIC).
**M8**: Unity-пакет — нативные сборки под все платформы в CI, `.meta` с настройками импорта, iOS через `__Internal`, сэмпл.

**M7**: покрытие тестами — `cargo xtask coverage` (строки, ветвления, .NET, базовая линия), ночной mutation testing.

**M6**: структурированное логирование — макрос `netlog!` с проверкой шаблонов при компиляции, CLEF, ограничение частоты, проброс в C#, `logview`.

**M5**: C ABI (`crates/netlib-ffi`, заголовок `bindings/c/netlib.h`) и .NET-биндинг (`bindings/dotnet`, .NET Standard 2.1, ноль GC-аллокаций).

**M4**: миграция соединения при смене адреса и пересоздании сокета без дисконнекта.

**M3**: планировщик — упаковка best-fit decreasing, приоритеты с защитой от голодания, бюджет полосы, TTL, `replace_key`, ноль аллокаций в рабочем цикле.

## Структура

| Крейт | Назначение |
|-------|------------|
| `crates/netlib-core` | sans-IO ядро (`no_std` + `alloc`, без зависимостей) |
| `crates/netlib` | драйвер на `std::net::UdpSocket`; по умолчанию с потоком приёма (точное время прихода, немедленные ответы на PING), отключается через `DriverConfig::receive_thread` |
| `crates/netlib-sim` | детерминированный эмулятор сети: потери, задержка, джиттер, дубли, полоса |
| `crates/netlib-ffi` | C ABI: cdylib + staticlib, poll-модель, blittable-структуры |
| `bindings/c` | сгенерированный `netlib.h` и C smoke-тест |
| `bindings/dotnet` | .NET Standard 2.1 биндинг, xunit-тесты, C#-клиент для echo-сервера |
| `unity/com.netlib.transport` | шаблон Unity-пакета (asmdef, сэмпл, README); собирается `cargo xtask unity-package` |
| `xtask` | `report` — критерии; `coverage` — покрытие; `unity-package`; `header`; `log-catalog`; `logview` |

## Быстрый старт

```sh
cargo test --workspace                     # все тесты
cargo xtask report                         # критерии текущей вехи: ✅/❌
cargo run -p netlib --example echo_server  # терминал 1
cargo run -p netlib --example echo_client  # терминал 2
cargo run -p netlib-sim --example sim -- --loss 20 --latency 100 --jitter 20 --clients 8
```

Использование:

```rust
use netlib::{ChannelConfig, Config, DeliveryMode, Event, Manager};

let config = Config {
    protocol_id: 0x1234,
    accept_incoming: true,
    channels: vec![ChannelConfig::new(DeliveryMode::UnreliableSequenced)],
    ..Config::default()
};
let mut server = Manager::bind("0.0.0.0:7777", config)?;
loop {
    server.poll()?;                       // начало тика
    while let Some(e) = server.poll_event() {
        match e {
            Event::ConnectRequest { peer, .. } => { server.accept(peer)?; }
            Event::Message { peer, channel, data } => { /* ... */ }
            _ => {}
        }
    }
    // server.send(peer, 0, &state)?;
    server.flush();                       // конец тика: упаковка и отправка
}
```

## Unity

Пакет UPM `com.netlib.transport` для Unity 6: Windows, macOS, Linux (включая Dedicated Server), Android, iOS. Mono и IL2CPP.

1. Скачайте `com.netlib.transport-*.tgz` — артефакт `unity-package` workflow «Unity package». Для текущей ОС можно собрать локально: `cargo xtask unity-package`.
2. **Package Manager → + → Add package from tarball.**
3. Сэмпл **Echo Demo** — в Package Manager.

Подробности: [unity/com.netlib.transport/README.md](unity/com.netlib.transport/README.md); что проверено автоматически и что нужно проверить руками — [docs/UNITY.md](docs/UNITY.md).

## Логирование

События пишутся по [Message Templates](https://messagetemplates.org/): у каждого константный шаблон, стабильный `EventId` и типизированные свойства (`PeerId`, `RemoteEndpoint`, `Seq`…).

- **Каталог событий:** [docs/LOG_EVENTS.md](docs/LOG_EVENTS.md).
- **Rust:** `manager.set_log_sink(Some(Box::new(netlib::logging::ClefSink::new(...))))` или свой `LogSink`.
- **C#:** `net.SetLogLevel(LogLevel.Info)`, затем после каждого `Poll()` цикл `while (net.TryGetLog(out var r)) ...`. Доступны `r.Template`, `r.EventId`, `r.GetProperty(i)`, `r.Render()`.
- **Примеры:** `NETLIB_LOG=debug cargo run -p netlib --example echo_server`. Для вывода в CLEF (Seq, Serilog, `jq`) добавьте `NETLIB_LOG_FORMAT=clef`.
- **Разбор:** `cargo xtask logview FILE --summary | --peer ID | --event ID | --min-level Warning`.

## Лицензия

MIT OR Apache-2.0.
