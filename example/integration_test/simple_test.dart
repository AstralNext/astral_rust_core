import 'package:integration_test/integration_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:astral_rust_core/p2p_service.dart';
import 'package:astral_rust_core/src/rust/frb_generated.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  setUpAll(() async => await RustLib.init());
  test('Can call easytierVersion', () async {
    final version = await P2PService().easytierVersion();
    expect(version, isNotEmpty);
  });
}
