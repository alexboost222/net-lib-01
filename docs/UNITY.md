# Unity: проверка интеграции

Что проверяется автоматически:

| Критерий | Проверка | Где |
|----------|----------|-----|
| X1 | Сборка нативных библиотек для Windows x86_64, macOS universal, Linux x86_64 (+ arm64 для серверов), Android arm64-v8a / armeabi-v7a / x86_64, iOS arm64 | workflow `Unity package`, job `native` |
| P4 | Динамические библиотеки < 1 МиБ | там же, шаг «Size of dynamic libraries» |
| X4 (частично) | Исходники пакета компилируются как в Unity 6: C# 9, .NET Standard 2.1, warnings as errors, обе ветки — обычная и iOS (`__Internal`); Runtime не ссылается на UnityEngine (`noEngineReferences`) | job `package`, `unity/ci` |
| X6 (частично) | Пакет полный: плагин для каждой платформы, `.meta` у каждого файла и папки, уникальные GUID, настройки импорта по платформам, Runtime совпадает с биндингом | `cargo test -p xtask x6_` |

Что нельзя проверить без Unity и устройств. Ручной чек-лист перед релизом:

- [ ] Unity 6, новый проект → Add package from tarball → нет ошибок импорта и предупреждений компилятора.
- [ ] Editor (Windows, macOS, Linux): Echo Demo, сервер и клиент в двух экземплярах редактора или редактор + сборка.
- [ ] Linux Dedicated Server, IL2CPP: запускается в batch mode, принимает клиентов.
- [ ] Windows Standalone, **IL2CPP**: клиент подключается, RTT отображается.
- [ ] macOS Standalone, IL2CPP, на Apple Silicon и Intel.
- [ ] Android, IL2CPP, arm64 и armv7: подключение через Wi-Fi; переключение Wi-Fi ↔ LTE без дисконнекта (смотреть лог `moved from … to …` на сервере).
- [ ] Android и iOS: уход в background на 5 с и возврат → `Rebind()` → соединение продолжается.
- [ ] iOS, IL2CPP: сборка Xcode-проекта линкуется (`libnetlib_ffi.a`), подключение работает; проверка в сети NAT64 (Network Link Conditioner / Mac Internet Sharing с NAT64) — X5.
- [ ] Логи доходят до Console (`SetLogLevel`, `TryGetLog`).
