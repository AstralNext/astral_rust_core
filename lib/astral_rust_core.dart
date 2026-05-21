library;

export 'src/rust/api/simple.dart';
export 'src/rust/api/firewall.dart';
export 'src/rust/frb_generated.dart' show RustLib;
export 'p2p_service.dart';

class AstralRustCore {
   static Future<int> add(int a, int b) async {
    return (a + b);
  }
}