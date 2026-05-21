import 'dart:convert';
import 'dart:io';

import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart';

import 'src/rust/api/p2p.dart' as p2p;
import 'src/rust/frb_generated.dart' show RustLib;

ExternalLibrary? _resolvePluginDll() {
  if (!Platform.isWindows) return null;

  final execDir = File(Platform.resolvedExecutable).parent.path;

  final possiblePaths = [
    '$execDir/astral_rust_core.dll',
    '$execDir/../plugins/astral_rust_core/Release/astral_rust_core.dll',
    '$execDir/../plugins/astral_rust_core/Debug/astral_rust_core.dll',
    '$execDir/plugins/astral_rust_core/astral_rust_core.dll',
    '$execDir/data/flutter_assets/packages/astral_rust_core/windows/astral_rust_core.dll',
  ];

  for (final path in possiblePaths) {
    if (File(path).existsSync()) {
      return ExternalLibrary.open(path);
    }
  }

  return null;
}

/// P2P 服务封装：对外提供更易用的 Dart API，并统一处理 FRB 初始化。
class P2PService {
  P2PService();

  /// 缓存初始化 Future，避免重复初始化。
  Future<void>? _initFuture;

  /// 确保 FRB 已初始化。
  ///
  /// 通常在应用启动时调用一次即可；此方法会缓存初始化结果。
  Future<void> ensureInitialized({bool forceSameCodegenVersion = true}) {
    return _initFuture ??= RustLib.init(
      forceSameCodegenVersion: forceSameCodegenVersion,
      externalLibrary: _resolvePluginDll(),
    );
  }

  /// 释放 FRB 资源（可选）。
  void dispose() => RustLib.dispose();

  /// 内部统一初始化入口，保证调用 Rust API 前已完成初始化。
  Future<T> _withInit<T>(Future<T> Function() action) async {
    await ensureInitialized();
    return action();
  }

  /// 获取 easytier 版本号。
  Future<String> easytierVersion() => _withInit(p2p.easytierVersion);

  /// 判断指定实例是否仍在运行。
  Future<bool> isEasytierRunning(String instanceId) =>
      _withInit(() => p2p.isEasytierRunning(instanceId: instanceId));

  /// 获取指定实例的 IP 列表。
  Future<List<String>> getIps(String instanceId) =>
      _withInit(() => p2p.getIps(instanceId: instanceId));

  /// 设置 tun 设备文件描述符。
  Future<void> setTunFd(String instanceId, int fd) =>
      _withInit(() => p2p.setTunFd(instanceId: instanceId, fd: fd));

  /// 获取运行信息的原始 JSON 字符串。
  Future<String> getRunningInfo(String instanceId) =>
      _withInit(() => p2p.getRunningInfo(instanceId: instanceId));

  /// 解析运行信息为 Map；如果返回为 null/解析失败则返回 null。
  Future<Map<String, dynamic>?> getRunningInfoJson(String instanceId) async {
    final raw = await getRunningInfo(instanceId);
    if (raw.isEmpty || raw == 'null') {
      return null;
    }
    try {
      final decoded = jsonDecode(raw);
      if (decoded is Map<String, dynamic>) {
        return decoded;
      }
    } catch (_) {
      return null;
    }
    return null;
  }

  /// 使用 TOML 配置创建服务实例，返回实例 ID。
  Future<String> createServer({
    required String configToml,
    required bool watchEvent,
  }) async {
    await ensureInitialized();
    final handle = await p2p.createServer(configToml: configToml, watchEvent: watchEvent);
    return p2p.joinHandleResult(handle: handle);
  }

  /// 使用参数旗标创建服务实例。
  Future<p2p.JoinHandleResultStringString> createServerWithFlags({
    required String username,
    required bool enableDhcp,
    required String specifiedIp,
    required String roomName,
    required String roomPassword,
    required List<String> severurl,
    required List<String> onurl,
    required List<String> cidrs,
    required List<p2p.Forward> forwards,
    required p2p.FlagsC flag,
  }) => _withInit(
    () => p2p.createServerWithFlags(
      username: username,
      enableDhcp: enableDhcp,
      specifiedIp: specifiedIp,
      roomName: roomName,
      roomPassword: roomPassword,
      severurl: severurl,
      onurl: onurl,
      cidrs: cidrs,
      forwards: forwards,
      flag: flag,
    ),
  );

  /// 关闭指定实例。
  Future<void> closeServer(String instanceId) =>
      _withInit(() => p2p.closeServer(instanceId: instanceId));

  /// 获取节点路由对列表。
  Future<List<p2p.PeerRoutePair>> getPeerRoutePairs(String instanceId) =>
      _withInit(() => p2p.getPeerRoutePairs(instanceId: instanceId));

  /// 获取网络状态汇总信息。
  Future<p2p.KVNetworkStatus> getNetworkStatus(String instanceId) =>
      _withInit(() => p2p.getNetworkStatus(instanceId: instanceId));
}
