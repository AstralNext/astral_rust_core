# astral_rust_core

Flutter Rust Bridge 插件（Astral / Astral Game 共用）。

```dart
final p2p = P2PService();
await p2p.ensureInitialized();
final id = await p2p.createInstance(configToml: toml, watchEvent: true);
```

`rust/` 变更会触发预编译，产物挂在 GitHub Release（`precompiled_<crateHash>`）。客户端仓库放 `cargokit_options.yaml`（`use_precompiled_binaries: true`）即可下载，不必每次现编。

重新生成 FRB：

```bash
flutter_rust_bridge_codegen generate
```
