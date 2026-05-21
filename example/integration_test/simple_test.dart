import 'package:integration_test/integration_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:astral_rust_core/astral_rust_core.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  setUpAll(() async => await RustLib.init());
  test('Can call rust function', () async {
    expect(greet(name: "Tom"), "Hello, Tom!");
  });
}
