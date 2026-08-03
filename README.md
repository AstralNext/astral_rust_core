# astral_rust_core

Flutter Rust Bridge 插件（Astral / Astral Game 共用）。

```dart
final p2p = P2PService();
await p2p.ensureInitialized();
final id = await p2p.createInstance(configToml: toml, watchEvent: true);
```

重新生成 FRB：

```bash
flutter_rust_bridge_codegen generate
```
