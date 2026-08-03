import 'package:flutter/material.dart';
import 'package:astral_rust_core/p2p_service.dart';
import 'package:astral_rust_core/src/rust/frb_generated.dart';

Future<void> main() async {
  await RustLib.init();
  final version = await P2PService().easytierVersion();
  runApp(MyApp(easytierVersion: version));
}

class MyApp extends StatelessWidget {
  const MyApp({super.key, required this.easytierVersion});

  final String easytierVersion;

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: const Text('astral_rust_core')),
        body: Center(
          child: Text('EasyTier: $easytierVersion'),
        ),
      ),
    );
  }
}
