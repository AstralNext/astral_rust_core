import 'dart:io';

import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart';

import 'src/rust/api/p2p.dart' as p2p;
import 'src/rust/frb_generated.dart' show RustLib;

export 'src/rust/api/p2p.dart'
    show
        AppCallResultC,
        AppInboundEventC,
        AppInboundKindC,
        CoreLogEventC,
        KVNetworkStatus,
        KVNodeInfo;

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
  Future<void> ensureInitialized({bool forceSameCodegenVersion = true}) {
    return _initFuture ??= RustLib.init(
      forceSameCodegenVersion: forceSameCodegenVersion,
      externalLibrary: _resolvePluginDll(),
    );
  }

  /// 释放 FRB 资源（可选）。
  void dispose() => RustLib.dispose();

  Future<T> _withInit<T>(Future<T> Function() action) async {
    await ensureInitialized();
    return action();
  }

  /// 获取 easytier 版本号。
  Future<String> easytierVersion() => _withInit(p2p.easytierVersion);

  /// 判断指定实例是否仍在运行。
  Future<bool> isEasytierRunning(String instanceId) =>
      _withInit(() => p2p.isEasytierRunning(instanceId: instanceId));

  /// 设置 tun 设备文件描述符（移动端 VPN）。
  Future<void> setTunFd(String instanceId, int fd) =>
      _withInit(() => p2p.setTunFd(instanceId: instanceId, fd: fd));

  /// 使用 TOML 配置创建服务实例，返回实例 ID。
  Future<String> createInstance({
    required String configToml,
    required bool watchEvent,
  }) async {
    await ensureInitialized();
    return p2p.createInstance(
      configToml: configToml,
      watchEvent: watchEvent,
    );
  }

  /// 关闭指定实例。
  Future<void> closeInstance(String instanceId) =>
      _withInit(() => p2p.closeInstance(instanceId: instanceId));

  /// 获取网络状态汇总信息。
  Future<p2p.KVNetworkStatus> getNetworkStatus(String instanceId) =>
      _withInit(() => p2p.getNetworkStatus(instanceId: instanceId));

  /// 内核日志流（替代 UDP 9999）。需先 [ensureInitialized]。
  Stream<p2p.CoreLogEventC> subscribeCoreLogs() {
    return Stream.fromFuture(ensureInitialized()).asyncExpand((_) {
      return p2p.subscribeCoreLogs();
    });
  }

  /// 请求-响应 App RPC。
  Future<p2p.AppCallResultC> appCall({
    required String instanceId,
    required int dstPeerId,
    required String channel,
    required BigInt requestId,
    required List<int> payload,
    required int flags,
    required int timeoutMs,
  }) =>
      _withInit(
        () => p2p.appCall(
          instanceId: instanceId,
          dstPeerId: dstPeerId,
          channel: channel,
          requestId: requestId,
          payload: payload,
          flags: flags,
          timeoutMs: timeoutMs,
        ),
      );

  /// Fire-and-forget 通知（仍等路由 ack）。
  Future<void> appNotify({
    required String instanceId,
    required int dstPeerId,
    required String channel,
    required List<int> payload,
    required int timeoutMs,
  }) =>
      _withInit(
        () => p2p.appNotify(
          instanceId: instanceId,
          dstPeerId: dstPeerId,
          channel: channel,
          payload: payload,
          timeoutMs: timeoutMs,
        ),
      );

  /// Peer RTT（毫秒）。
  Future<PlatformInt64> peerPing({
    required String instanceId,
    required int dstPeerId,
    required int timeoutMs,
  }) =>
      _withInit(
        () => p2p.peerPing(
          instanceId: instanceId,
          dstPeerId: dstPeerId,
          timeoutMs: timeoutMs,
        ),
      );

  /// 入站 Call/Notify 事件流。
  Stream<p2p.AppInboundEventC> subscribeAppInbound(String instanceId) {
    return Stream.fromFuture(ensureInitialized()).asyncExpand((_) {
      return p2p.subscribeAppInbound(instanceId: instanceId);
    });
  }

  /// 回复入站 Call。
  Future<bool> appCallReply({
    required String instanceId,
    required BigInt token,
    required int status,
    required String errorMsg,
    required List<int> payload,
  }) =>
      _withInit(
        () => p2p.appCallReply(
          instanceId: instanceId,
          token: token,
          status: status,
          errorMsg: errorMsg,
          payload: payload,
        ),
      );

  /// 本机 peer id。
  Future<int> myPeerId(String instanceId) =>
      _withInit(() => p2p.myPeerId(instanceId: instanceId));
}
