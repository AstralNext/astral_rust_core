use easytier::common::config::{ConfigFileControl, PortForwardConfig};
pub use easytier::common::config::{ConfigLoader, NetworkIdentity, PeerConfig, TomlConfigLoader};
pub use easytier::common::global_ctx::{EventBusSubscriber, GlobalCtxEvent};
pub use easytier::instance_manager::NetworkInstanceManager;
pub use easytier::proto;
pub use easytier::proto::api::instance::{PeerRoutePair, Route};
pub use easytier::proto::common::NatType;
use lazy_static::lazy_static;
use serde_json::json;
use tokio::runtime::Runtime;
pub use tokio::task::JoinHandle;
use uuid::Uuid;

pub static DEFAULT_ET_DNS_ZONE: &str = "as.net.";
const LOCAL_SYNTHETIC_PEER_ID: u32 = 0;

lazy_static! {
    static ref RT: Runtime = Runtime::new().expect("failed to create tokio runtime");
    static ref MANAGER: NetworkInstanceManager = NetworkInstanceManager::new();
}

fn parse_instance_id(instance_id: &str) -> Result<Uuid, String> {
    Uuid::parse_str(instance_id).map_err(|e| format!("invalid instance_id: {}", e))
}

async fn get_instance_info(
    instance_id: &str,
) -> Result<easytier::launcher::NetworkInstanceRunningInfo, String> {
    let id = parse_instance_id(instance_id)?;
    MANAGER
        .get_network_info(&id)
        .await
        .ok_or_else(|| "instance not found".to_string())
}

fn peer_conn_info_to_string(p: proto::api::instance::PeerConnInfo) -> String {
    format!(
        "my_peer_id: {}, dst_peer_id: {}, tunnel_info: {:?}",
        p.my_peer_id, p.peer_id, p.tunnel
    )
}

fn send_udp_to_localhost_with_instance_id(instance_id: &str, message: &str) -> Result<(), String> {
    use std::net::UdpSocket;
    use serde_json::json;

    let socket = match UdpSocket::bind("0.0.0.0:0") {
        Ok(s) => s,
        Err(e) => return Err(format!("udp bind failed: {}", e)),
    };

    let json_msg = json!({
        "instance_id": instance_id,
        "message": message,
    });

    let json_str = serde_json::to_string(&json_msg).map_err(|e| format!("json serialize failed: {}", e))?;

    match socket.send_to(json_str.as_bytes(), "127.0.0.1:9999") {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("udp send failed: {}", e)),
    }
}

pub fn send_udp_to_localhost(message: &str) -> Result<(), String> {
    use std::net::UdpSocket;

    let socket = match UdpSocket::bind("0.0.0.0:0") {
        Ok(s) => s,
        Err(e) => return Err(format!("udp bind failed: {}", e)),
    };

    match socket.send_to(message.as_bytes(), "127.0.0.1:9999") {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("udp send failed: {}", e)),
    }
}

fn handle_event_with_instance_id(mut events: EventBusSubscriber, instance_id: String) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(e) => match e {
                    GlobalCtxEvent::PeerAdded(p) => {
                        let msg = format!("peer added. peer_id: {}", p);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::PeerRemoved(p) => {
                        let msg = format!("peer removed. peer_id: {}", p);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::PeerConnAdded(p) => {
                        let conn_info = peer_conn_info_to_string(p);
                        let msg = format!("peer connection added. conn_info: {}", conn_info);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::PeerConnRemoved(p) => {
                        let msg = format!(
                            "peer connection removed. conn_info: {}",
                            peer_conn_info_to_string(p)
                        );
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::ListenerAddFailed(p, msg) => {
                        let msg = format!("listener add failed. listener: {}, msg: {}", p, msg);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::ListenerAcceptFailed(p, msg) => {
                        let msg =
                            format!("listener accept failed. listener: {}, msg: {}", p, msg);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::ListenerAdded(p) => {
                        if p.scheme() == "ring" {
                            continue;
                        }
                        let msg = format!("listener added. listener: {}", p);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::ConnectionAccepted(local, remote) => {
                        let msg =
                            format!("connection accepted. local: {}, remote: {}", local, remote);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::ConnectionError(local, remote, err) => {
                        let msg = format!(
                            "connection error. local: {}, remote: {}, err: {}",
                            local, remote, err
                        );
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::TunDeviceReady(dev) => {
                        let msg = format!("tun device ready. dev: {}", dev);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::TunDeviceError(err) => {
                        let msg = format!("tun device error. err: {}", err);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::Connecting(dst) => {
                        let msg = format!("connecting to peer. dst: {}", dst);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::ConnectError(dst, ip_version, err) => {
                        let msg = format!(
                            "connect error. dst: {}, ip_version: {}, err: {}",
                            dst, ip_version, err
                        );
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::VpnPortalStarted(portal) => {
                        let msg = format!("vpn portal started. portal: {}", portal);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::VpnPortalClientConnected(portal, client_addr) => {
                        let msg = format!(
                            "vpn portal client connected. portal: {}, client_addr: {}",
                            portal, client_addr
                        );
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::VpnPortalClientDisconnected(portal, client_addr) => {
                        let msg = format!(
                            "vpn portal client disconnected. portal: {}, client_addr: {}",
                            portal, client_addr
                        );
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::DhcpIpv4Changed(old, new) => {
                        let msg = format!("dhcp ip changed. old: {:?}, new: {:?}", old, new);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::DhcpIpv4Conflicted(ip) => {
                        let msg = format!("dhcp ip conflict. ip: {:?}", ip);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::PortForwardAdded(cfg) => {
                        let msg = format!("port forward added. cfg: {:?}", cfg);
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::ListenerPortMappingEstablished {
                        local_listener,
                        mapped_listener,
                        backend,
                    } => {
                        let msg = format!(
                            "listener port mapping established. local: {}, mapped: {}, backend: {}",
                            local_listener, mapped_listener, backend
                        );
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::PublicIpv6Changed(old, new) => {
                        let msg = format!(
                            "public ipv6 changed. old: {:?}, new: {:?}",
                            old, new
                        );
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::PublicIpv6RoutesUpdated(added, removed) => {
                        let msg = format!(
                            "public ipv6 routes updated. added: {:?}, removed: {:?}",
                            added, removed
                        );
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::UdpBroadcastRelayStartResult {
                        capture_backend,
                        error,
                    } => {
                        let msg = format!(
                            "udp broadcast relay start result. backend: {:?}, error: {:?}",
                            capture_backend, error
                        );
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                    }
                    GlobalCtxEvent::CredentialChanged => {
                        let msg = "credential changed";
                        let _ = send_udp_to_localhost_with_instance_id(&instance_id, msg);
                    }
                    GlobalCtxEvent::ConfigPatched(_) => {}
                    GlobalCtxEvent::ProxyCidrsUpdated(_, _) => {}
                },
                Err(err) => {
                    eprintln!("event receive error: {:?}", err);
                    match err {
                        tokio::sync::broadcast::error::RecvError::Closed => {
                            let msg = "event channel closed; stop handling events";
                            let _ = send_udp_to_localhost_with_instance_id(&instance_id, msg);
                            break;
                        }
                        tokio::sync::broadcast::error::RecvError::Lagged(n) => {
                            let msg = format!("event lagged, dropped {} events", n);
                            eprintln!("{}", msg);
                            let _ = send_udp_to_localhost_with_instance_id(&instance_id, &msg);
                        }
                    }
                }
            }
        }
    })
}

pub fn handle_event(mut events: EventBusSubscriber) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(e) => match e {
                    GlobalCtxEvent::PeerAdded(p) => {
                        let msg = format!("peer added. peer_id: {}", p);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::PeerRemoved(p) => {
                        let msg = format!("peer removed. peer_id: {}", p);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::PeerConnAdded(p) => {
                        let conn_info = peer_conn_info_to_string(p);
                        let msg = format!("peer connection added. conn_info: {}", conn_info);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::PeerConnRemoved(p) => {
                        let msg = format!(
                            "peer connection removed. conn_info: {}",
                            peer_conn_info_to_string(p)
                        );
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::ListenerAddFailed(p, msg) => {
                        let msg = format!("listener add failed. listener: {}, msg: {}", p, msg);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::ListenerAcceptFailed(p, msg) => {
                        let msg =
                            format!("listener accept failed. listener: {}, msg: {}", p, msg);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::ListenerAdded(p) => {
                        if p.scheme() == "ring" {
                            continue;
                        }
                        let msg = format!("listener added. listener: {}", p);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::ConnectionAccepted(local, remote) => {
                        let msg =
                            format!("connection accepted. local: {}, remote: {}", local, remote);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::ConnectionError(local, remote, err) => {
                        let msg = format!(
                            "connection error. local: {}, remote: {}, err: {}",
                            local, remote, err
                        );
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::TunDeviceReady(dev) => {
                        let msg = format!("tun device ready. dev: {}", dev);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::TunDeviceError(err) => {
                        let msg = format!("tun device error. err: {}", err);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::Connecting(dst) => {
                        let msg = format!("connecting to peer. dst: {}", dst);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::ConnectError(dst, ip_version, err) => {
                        let msg = format!(
                            "connect error. dst: {}, ip_version: {}, err: {}",
                            dst, ip_version, err
                        );
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::VpnPortalStarted(portal) => {
                        let msg = format!("vpn portal started. portal: {}", portal);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::VpnPortalClientConnected(portal, client_addr) => {
                        let msg = format!(
                            "vpn portal client connected. portal: {}, client_addr: {}",
                            portal, client_addr
                        );
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::VpnPortalClientDisconnected(portal, client_addr) => {
                        let msg = format!(
                            "vpn portal client disconnected. portal: {}, client_addr: {}",
                            portal, client_addr
                        );
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::DhcpIpv4Changed(old, new) => {
                        let msg = format!("dhcp ip changed. old: {:?}, new: {:?}", old, new);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::DhcpIpv4Conflicted(ip) => {
                        let msg = format!("dhcp ip conflict. ip: {:?}", ip);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::PortForwardAdded(cfg) => {
                        let msg = format!("port forward added. cfg: {:?}", cfg);
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::ListenerPortMappingEstablished {
                        local_listener,
                        mapped_listener,
                        backend,
                    } => {
                        let msg = format!(
                            "listener port mapping established. local: {}, mapped: {}, backend: {}",
                            local_listener, mapped_listener, backend
                        );
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::PublicIpv6Changed(old, new) => {
                        let msg = format!(
                            "public ipv6 changed. old: {:?}, new: {:?}",
                            old, new
                        );
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::PublicIpv6RoutesUpdated(added, removed) => {
                        let msg = format!(
                            "public ipv6 routes updated. added: {:?}, removed: {:?}",
                            added, removed
                        );
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::UdpBroadcastRelayStartResult {
                        capture_backend,
                        error,
                    } => {
                        let msg = format!(
                            "udp broadcast relay start result. backend: {:?}, error: {:?}",
                            capture_backend, error
                        );
                        let _ = send_udp_to_localhost(&msg);
                    }
                    GlobalCtxEvent::CredentialChanged => {
                        let _ = send_udp_to_localhost("credential changed");
                    }
                    GlobalCtxEvent::ConfigPatched(_) => {}
                    GlobalCtxEvent::ProxyCidrsUpdated(_, _) => {}
                },
                Err(err) => {
                    eprintln!("event receive error: {:?}", err);
                    match err {
                        tokio::sync::broadcast::error::RecvError::Closed => {
                            let msg = "event channel closed; stop handling events";
                            let _ = send_udp_to_localhost(msg);
                            break;
                        }
                        tokio::sync::broadcast::error::RecvError::Lagged(n) => {
                            let msg = format!("event lagged, dropped {} events", n);
                            eprintln!("{}", msg);
                            let _ = send_udp_to_localhost(&msg);
                        }
                    }
                }
            }
        }
    })
}

pub fn easytier_version() -> Result<String, String> {
    Ok(easytier::VERSION.to_string())
}

pub async fn is_easytier_running(instance_id: String) -> bool {
    let Ok(id) = parse_instance_id(&instance_id) else {
        return false;
    };
    MANAGER.list_network_instance_ids().contains(&id)
}

#[derive(Debug)]
pub struct NodeHopStats {
    pub peer_id: u32,
    pub target_ip: String,
    pub latency_ms: f64,
    pub packet_loss: f32,
    pub node_name: String,
}

#[derive(Debug)]
pub struct KVNodeConnectionStats {
    pub conn_type: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_packets: u64,
    pub tx_packets: u64,
}

#[derive(Debug)]
pub struct KVNodeInfo {
    pub peer_id: u32,
    pub hostname: String,
    pub ipv4: String,
    /// 虚拟网 IPv6（含前缀长度），与 `Route.ipv6_addr` 一致；无分配时为空串。
    pub ipv6: String,
    pub latency_ms: f64,
    pub nat: String,
    pub hops: Vec<NodeHopStats>,
    pub loss_rate: f32,
    pub connections: Vec<KVNodeConnectionStats>,
    pub tunnel_proto: String,
    pub conn_type: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub version: String,
    pub cost: i32,
}

#[derive(Debug)]
pub struct KVNetworkStatus {
    pub total_nodes: usize,
    pub nodes: Vec<KVNodeInfo>,
}

pub async fn get_ips(instance_id: String) -> Vec<String> {
    let info = match get_instance_info(&instance_id).await {
        Ok(info) => info,
        Err(_) => return Vec::new(),
    };

    let mut result = Vec::new();

    // 本机虚拟 IPv4 在 `my_node_info`；仅凭 routes（其它节点）在单人/DHCP 早期常为
    // 空，漏掉本机会误导上层「永远没 IP」。
    if let Some(my) = &info.my_node_info {
        if let Some(ipv4_addr) = &my.virtual_ipv4 {
            if let Some(addr) = &ipv4_addr.address {
                let ip = format!(
                    "{}.{}.{}.{}/{}",
                    (addr.addr >> 24) & 0xFF,
                    (addr.addr >> 16) & 0xFF,
                    (addr.addr >> 8) & 0xFF,
                    addr.addr & 0xFF,
                    ipv4_addr.network_length
                );
                if !result.contains(&ip) {
                    result.push(ip);
                }
            }
        }
    }

    for route in &info.routes {
        if let Some(ipv4_addr) = &route.ipv4_addr {
            if let Some(addr) = &ipv4_addr.address {
                let ip = format!(
                    "{}.{}.{}.{}/{}",
                    (addr.addr >> 24) & 0xFF,
                    (addr.addr >> 16) & 0xFF,
                    (addr.addr >> 8) & 0xFF,
                    addr.addr & 0xFF,
                    ipv4_addr.network_length
                );
                if !result.contains(&ip) {
                    result.push(ip);
                }
            }
        }
    }

    result
}

pub async fn set_tun_fd(instance_id: String, fd: i32) -> Result<(), String> {
    let id = parse_instance_id(&instance_id)?;
    MANAGER
        .set_tun_fd(&id, fd)
        .map_err(|e| format!("set_tun_fd failed: {}", e))
}

pub async fn get_running_info(instance_id: String) -> String {
    let info = match get_instance_info(&instance_id).await {
        Ok(info) => info,
        Err(_) => return "null".to_string(),
    };

    serde_json::to_string(&json!({
        "dev_name": info.dev_name,
        "my_node_info": info.my_node_info.as_ref().map(|node| json!({
            "virtual_ipv4": node.virtual_ipv4.as_ref().map(|addr| json!({
                "address": addr.address.as_ref().map(|a| json!({ "addr": a.addr })),
            })),
        })),
        "routes": info.routes,
        "peer_route_pairs": info.peer_route_pairs,
    }))
    .unwrap_or_else(|_| "null".to_string())
}

pub struct FlagsC {
    pub default_protocol: String,
    pub dev_name: String,
    pub enable_encryption: bool,
    pub enable_ipv6: bool,
    pub mtu: u32,
    pub latency_first: bool,
    pub enable_exit_node: bool,
    pub no_tun: bool,
    pub use_smoltcp: bool,
    pub relay_network_whitelist: String,
    pub disable_p2p: bool,
    pub relay_all_peer_rpc: bool,
    pub disable_udp_hole_punching: bool,
    pub disable_tcp_hole_punching: bool,
    pub multi_thread: bool,
    pub data_compress_algo: i32,
    pub bind_device: bool,
    pub enable_kcp_proxy: bool,
    pub disable_kcp_input: bool,
    pub disable_relay_kcp: bool,
    pub proxy_forward_by_system: bool,
    pub accept_dns: bool,
    pub private_mode: bool,
    pub enable_quic_proxy: bool,
    pub disable_quic_input: bool,
    pub disable_sym_hole_punching: bool,
    pub tcp_whitelist: String,
    pub udp_whitelist: String,
}

pub struct Forward {
    pub bind_addr: String,
    pub dst_addr: String,
    pub proto: String,
}
pub fn create_server(config_toml: String, watch_event: bool) -> JoinHandle<Result<String, String>> {
    RT.spawn(async move {
        let cfg = TomlConfigLoader::new_from_str(&config_toml)
            .map_err(|e| format!("invalid config toml: {}", e))?;
        let instance_id = cfg.get_id();
        let instance_id_str = instance_id.to_string();

        let network_identity = cfg.get_network_identity();
        let hostname = cfg.get_hostname();
        let dhcp = cfg.get_dhcp();
        let ipv4 = cfg.get_ipv4().map(|ip| ip.to_string()).unwrap_or_else(|| "none".to_string());
        let listeners = cfg.get_listeners()
            .map(|l| l.iter().map(|u| u.to_string()).collect::<Vec<_>>().join(", "))
            .unwrap_or_else(|| "none".to_string());
        let peers = cfg.get_peers()
            .iter()
            .map(|p| p.uri.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let config_msg = format!(
            "instance starting. instance_id: {}, network_name: {}, network_secret: {}, hostname: {}, dhcp: {}, ipv4: {}, listeners: [{}], peers: [{}]",
            instance_id,
            network_identity.network_name,
            network_identity.network_secret.as_deref().unwrap_or("none"),
            hostname,
            dhcp,
            ipv4,
            listeners,
            peers
        );
        let _ = send_udp_to_localhost_with_instance_id(&instance_id_str, &config_msg);

        MANAGER
            .run_network_instance(cfg, false, ConfigFileControl::STATIC_CONFIG)
            .map_err(|e| format!("start instance failed: {}", e))?;

        // EasyTier 的 NetworkInstance::start 是 spawn-thread 异步的，`run_network_instance`
        // 返回时 Instance::run 还没跑到 `astral_app_rpc::install`。如果直接把 instance_id
        // 交回给 dart，紧跟着的 `subscribeAppInbound` / `myPeerId` 会拿到
        // "astral app rpc service not found" 报错（broadcast 流立刻 onDone）。
        wait_for_app_rpc_service(&instance_id, std::time::Duration::from_secs(5)).await;

        if watch_event {
            if let Some(instance) = MANAGER.iter().find(|item| *item.key() == instance_id) {
                if let Some(subscriber) = instance.subscribe_event() {
                    handle_event_with_instance_id(subscriber, instance_id_str.clone());
                }
            }
        }

        Ok(instance_id_str)
    })
}

pub async fn join_handle_result(handle: JoinHandle<Result<String, String>>) -> Result<String, String> {
    handle.await.map_err(|e| format!("join handle error: {}", e))?
}

pub fn create_server_with_flags(
    username: String,
    enable_dhcp: bool,
    specified_ip: String,
    room_name: String,
    room_password: String,
    severurl: Vec<String>,
    onurl: Vec<String>,
    cidrs: Vec<String>,
    forwards: Vec<Forward>,
    flag: FlagsC,
) -> JoinHandle<Result<String, String>> {
    RT.spawn(async move {
        let cfg = TomlConfigLoader::default();

        let mut listeners = Vec::new();
        for url in onurl {
            match url.parse() {
                Ok(parsed) => listeners.push(parsed),
                Err(e) => return Err(format!("invalid listener url: {}, error: {}", url, e)),
            }
        }
        cfg.set_listeners(listeners);

        cfg.set_hostname(Some(username));
        cfg.set_dhcp(enable_dhcp);
        for c in cidrs {
            let _ = cfg.add_proxy_cidr(c.parse().unwrap(), None);
        }
        let mut old = cfg.get_port_forwards();

        for c in forwards {
            let port_forward_item = PortForwardConfig {
                bind_addr: c.bind_addr.parse().unwrap(),
                dst_addr: c.dst_addr.parse().unwrap(),
                proto: c.proto,
            };
            old.push(port_forward_item);
        }

        cfg.set_port_forwards(old);
        let mut flags = cfg.get_flags();
        flags.default_protocol = flag.default_protocol;
        flags.dev_name = "astral".to_string();
        flags.enable_encryption = flag.enable_encryption;
        flags.enable_ipv6 = flag.enable_ipv6;
        flags.mtu = flag.mtu;
        flags.latency_first = flag.latency_first;
        flags.enable_exit_node = flag.enable_exit_node;
        flags.no_tun = flag.no_tun;
        flags.use_smoltcp = flag.use_smoltcp;
        flags.relay_network_whitelist = flag.relay_network_whitelist;
        flags.disable_p2p = flag.disable_p2p;
        flags.relay_all_peer_rpc = flag.relay_all_peer_rpc;
        flags.disable_udp_hole_punching = flag.disable_udp_hole_punching;
        flags.disable_tcp_hole_punching = flag.disable_tcp_hole_punching;
        flags.multi_thread = flag.multi_thread;
        flags.data_compress_algo = flag.data_compress_algo;
        flags.bind_device = flag.bind_device;
        flags.enable_kcp_proxy = flag.enable_kcp_proxy;
        flags.disable_kcp_input = flag.disable_kcp_input;
        flags.disable_relay_kcp = flag.disable_relay_kcp;
        flags.proxy_forward_by_system = flag.proxy_forward_by_system;
        flags.accept_dns = flag.accept_dns;
        flags.private_mode = flag.private_mode;
        flags.enable_quic_proxy = flag.enable_quic_proxy;
        flags.disable_quic_input = flag.disable_quic_input;
        flags.disable_sym_hole_punching = flag.disable_sym_hole_punching;
        cfg.set_flags(flags);

        if !flag.tcp_whitelist.is_empty() {
            let tcp_ports: Vec<String> = flag
                .tcp_whitelist
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            cfg.set_tcp_whitelist(tcp_ports);
        }
        if !flag.udp_whitelist.is_empty() {
            let udp_ports: Vec<String> = flag
                .udp_whitelist
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            cfg.set_udp_whitelist(udp_ports);
        }

        let mut peer_configs = Vec::new();
        for url in severurl {
            match url.parse() {
                Ok(uri) => peer_configs.push(PeerConfig {
                    uri,
                    peer_public_key: None,
                }),
                Err(e) => return Err(format!("invalid server url: {}, error: {}", url, e)),
            }
        }
        cfg.set_peers(peer_configs);

        if !enable_dhcp && !specified_ip.is_empty() {
            let ip_str = format!("{}/24", specified_ip);
            match ip_str.parse() {
                Ok(ip) => cfg.set_ipv4(Some(ip)),
                Err(e) => {
                    return Err(format!("invalid ip address: {}, error: {}", specified_ip, e))
                }
            }
        }

        cfg.set_network_identity(NetworkIdentity::new(room_name, room_password));
        let instance_id = cfg.get_id();
        let instance_id_str = instance_id.to_string();

        MANAGER
            .run_network_instance(cfg, false, ConfigFileControl::STATIC_CONFIG)
            .map_err(|e| format!("start instance failed: {}", e))?;

        wait_for_app_rpc_service(&instance_id, std::time::Duration::from_secs(5)).await;

        if let Some(instance) = MANAGER.iter().find(|item| *item.key() == instance_id) {
            if let Some(subscriber) = instance.subscribe_event() {
                handle_event_with_instance_id(subscriber, instance_id_str.clone());
            }
        }

        Ok(instance_id_str)
    })
}

/// 等 `astral_app_rpc::install` 在 `Instance::run` 跑完后把 service 写进 REGISTRY。
/// `NetworkInstance::start` 是 spawn-thread 异步的，调用方不能在 instance_id 一拿到
/// 手就立刻去 `subscribe_app_inbound` / `my_peer_id`，否则会拿到
/// "astral app rpc service not found"。
async fn wait_for_app_rpc_service(instance_id: &uuid::Uuid, timeout: std::time::Duration) {
    let step = std::time::Duration::from_millis(50);
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if app_rpc::get_service(instance_id).is_some() {
            return;
        }
        if std::time::Instant::now() >= deadline {
            eprintln!(
                "[astral_rust_core] WARN: astral_app_rpc service did not become ready within {:?} for instance {}",
                timeout, instance_id,
            );
            return;
        }
        tokio::time::sleep(step).await;
    }
}

pub fn close_server(instance_id: String) -> Result<(), String> {
    let id = parse_instance_id(&instance_id)?;
    MANAGER
        .delete_network_instance(vec![id])
        .map_err(|e| format!("delete instance failed: {}", e))?;
    Ok(())
}
pub async fn get_peer_route_pairs(instance_id: String) -> Result<Vec<PeerRoutePair>, String> {
    let info = get_instance_info(&instance_id).await?;

    let mut pairs = if info.peer_route_pairs.is_empty() {
        use easytier::proto::api::instance::list_peer_route_pair;
        list_peer_route_pair(info.peers.clone(), info.routes.clone())
    } else {
        info.peer_route_pairs
    };

    let mut route_peer_ids: std::collections::HashSet<u32> = pairs
        .iter()
        .filter_map(|p| p.route.as_ref().map(|r| r.peer_id))
        .collect();

    for peer in &info.peers {
        if !route_peer_ids.contains(&peer.peer_id) {
            pairs.push(PeerRoutePair {
                route: None,
                peer: Some(peer.clone()),
            });
            route_peer_ids.insert(peer.peer_id);
        }
    }

    if let Some(my_node_info) = &info.my_node_info {
        // 本机补齐节点使用稳定哨兵 peer_id，避免因连接角色变化导致 peer_id 抖动后被去重吞掉。
        let my_peer_id = LOCAL_SYNTHETIC_PEER_ID;

        let my_ipv6_from_routes = info
            .routes
            .iter()
            .find(|r| r.peer_id == my_node_info.peer_id)
            .and_then(|r| r.ipv6_addr.clone());

        let my_route = Route {
            peer_id: my_peer_id,
            ipv4_addr: my_node_info.virtual_ipv4.clone(),
            ipv6_addr: my_ipv6_from_routes,
            next_hop_peer_id: my_peer_id,
            cost: 0,
            path_latency: 0,
            proxy_cidrs: vec![],
            hostname: my_node_info.hostname.clone(),
            stun_info: my_node_info.stun_info.clone(),
            inst_id: "local".to_string(),
            version: my_node_info.version.clone(),
            feature_flag: None,
            next_hop_peer_id_latency_first: None,
            cost_latency_first: None,
            path_latency_latency_first: None,
            public_ipv6_addr: None,
            ipv6_public_addr_prefix: None,
        };

        let my_pair = PeerRoutePair {
            route: Some(my_route),
            peer: None,
        };

        pairs.push(my_pair);
    }

    Ok(pairs)
}

pub async fn get_network_status(instance_id: String) -> KVNetworkStatus {
    // 对齐旧版 Astral：先用 get_peer_route_pairs() 组装（其中包含本机 pair 补齐逻辑），
    // 再统一映射为 KVNodeInfo，避免某些时刻本机节点在列表里丢失。
    let pairs = get_peer_route_pairs(instance_id.clone())
        .await
        .unwrap_or_default();

    let mut nodes: Vec<KVNodeInfo> = Vec::new();
    for p in pairs {
        let Some(route) = p.route.clone() else {
            continue;
        };

        let lat_ms = if route.cost == 1 {
            p.get_latency_ms().unwrap_or(0.0)
        } else {
            route.path_latency_latency_first() as f64
        };

        let loss_percent = p.get_loss_rate().unwrap_or(0.0) * 100.0;
        let ipv4 = route
            .ipv4_addr
            .as_ref()
            .and_then(|ip| ip.address.clone())
            .map(|ip| ip.to_string())
            .unwrap_or_default();

        let ipv6 = route
            .ipv6_addr
            .as_ref()
            .map(|addr| addr.to_string())
            .unwrap_or_default();

        let mut node_info = KVNodeInfo {
            peer_id: route.peer_id,
            hostname: route.hostname.clone(),
            ipv4,
            ipv6,
            latency_ms: lat_ms,
            nat: p.get_udp_nat_type(),
            hops: vec![],
            loss_rate: loss_percent as f32,
            connections: vec![],
            tunnel_proto: p.get_conn_protos().unwrap_or_default().join(","),
            conn_type: p.get_udp_nat_type(),
            rx_bytes: p.get_rx_bytes().unwrap_or(0),
            tx_bytes: p.get_tx_bytes().unwrap_or(0),
            version: if route.version.is_empty() {
                "unknown".to_string()
            } else {
                route.version
            },
            cost: route.cost,
        };

        if route.inst_id == "local" || route.peer_id == LOCAL_SYNTHETIC_PEER_ID {
            node_info.conn_type = "Local".to_string();
            if node_info.tunnel_proto.is_empty() {
                node_info.tunnel_proto = "-".to_string();
            }
        }

        if let Some(peer) = &p.peer {
            for conn in &peer.conns {
                if let Some(stats) = &conn.stats {
                    let conn_type = conn
                        .tunnel
                        .as_ref()
                        .map(|t| t.tunnel_type.clone())
                        .unwrap_or_else(|| "unknown".to_string());
                    node_info.connections.push(KVNodeConnectionStats {
                        conn_type,
                        rx_bytes: stats.rx_bytes,
                        tx_bytes: stats.tx_bytes,
                        rx_packets: stats.rx_packets,
                        tx_packets: stats.tx_packets,
                    });
                }
            }
        }

        nodes.push(node_info);
    }

    // 避免同一 peer 在 pair 合并阶段出现重复条目。
    nodes.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
    nodes.dedup_by(|a, b| a.peer_id == b.peer_id);

    KVNetworkStatus {
        total_nodes: nodes.len(),
        nodes,
    }
}

pub fn init_app() {
    lazy_static::initialize(&RT);
}

// ============================================================================
// Astral application-level peer RPC bindings.
//
// Thin Dart-friendly wrappers around `easytier::peers::astral_app_rpc`. The
// underlying RPC surface is intentionally tiny (Call / Notify / Ping) and
// dispatches business flows by `channel` + opaque `payload` bytes; see
// `easytier/src/proto/astral_rpc.proto` for the wire contract.
//
// Multi-instance: every method takes the instance UUID string so several
// running networks can be addressed independently.
// ============================================================================

use easytier::peers::astral_app_rpc as app_rpc;
use crate::frb_generated::StreamSink;

/// Mirrors `easytier::peers::astral_app_rpc::status` so callers don't have to
/// pull the underlying crate just to read constants.
pub mod app_rpc_status {
    pub const OK: i32 = 0;
    pub const NO_SUBSCRIBER: i32 = -1;
    pub const REPLY_TIMEOUT: i32 = -2;
    pub const SERVICE_DROPPED: i32 = -3;
}

/// Result of [`app_call`] — directly maps `AppCallResponse` to a Dart record.
#[derive(Debug, Clone)]
pub struct AppCallResultC {
    pub status: i32,
    pub error_msg: String,
    pub payload: Vec<u8>,
}

/// Discriminator for [`AppInboundEventC`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppInboundKindC {
    /// Request expecting a reply. Receiver MUST call [`app_call_reply`] with
    /// the carried `token`, otherwise the remote caller observes
    /// `app_rpc_status::REPLY_TIMEOUT` after the receiver-side timeout
    /// (default 30s, configured in EasyTier).
    Call,
    /// Fire-and-forget notification (the sender already received an RPC-layer
    /// ack; this event is informational on the receiver side). For `Notify`
    /// events `request_id` and `token` are always 0.
    Notify,
}

/// Inbound event delivered through [`subscribe_app_inbound`].
///
/// Modelled as a flat struct (rather than a Rust enum with payload variants)
/// so that the Dart binding stays a plain `dart class`, no `freezed` dep.
#[derive(Debug, Clone)]
pub struct AppInboundEventC {
    pub kind: AppInboundKindC,
    pub from_peer_id: u32,
    pub channel: String,
    /// `request_id` echoed from the caller (0 for `Notify`).
    pub request_id: u64,
    /// Reply correlation token (0 for `Notify`). Pass to [`app_call_reply`].
    pub token: u64,
    pub payload: Vec<u8>,
}

fn lookup_app_rpc(
    instance_id: &str,
) -> Result<std::sync::Arc<app_rpc::AstralAppRpcService>, String> {
    let id = parse_instance_id(instance_id)?;
    app_rpc::get_service(&id)
        .ok_or_else(|| format!("astral app rpc service not found for instance {}", id))
}

/// Send a request-response RPC to `dst_peer_id` and await the typed reply.
///
/// 不要用 `RT.spawn(...).await` 把这次调用搬到 `RT` —— `RT` 既不是 FRB 的
/// runtime，也不是 EasyTier 每个 instance 自己的 runtime（见 `EasyTierLauncher::start`
/// 里的 `std::thread::spawn` + 独立 `tokio::runtime::Runtime`）。跨三个 runtime
/// 调度时，`tokio::time::timeout` 注册在 RT 的 timer driver、mpsc 唤醒发生在
/// EasyTier runtime、JoinHandle 唤醒落在 FRB executor，时序上很容易错过 wake，
/// 表现为 Dart 侧 `appCall` 永久 Pending、5s 超时也不触发。直接在 FRB executor
/// 上 `.await svc.call()` 反而是稳的（参考 commit a0fb25e）。
pub async fn app_call(
    instance_id: String,
    dst_peer_id: u32,
    channel: String,
    request_id: u64,
    payload: Vec<u8>,
    flags: u32,
    timeout_ms: i32,
) -> Result<AppCallResultC, String> {
    let svc = lookup_app_rpc(&instance_id)?;
    let resp = svc
        .call(dst_peer_id, channel, request_id, payload, flags, timeout_ms)
        .await
        .map_err(|e| e.to_string())?;
    Ok(AppCallResultC {
        status: resp.status,
        error_msg: resp.error_msg,
        payload: resp.payload,
    })
}

/// Send a fire-and-forget notification to `dst_peer_id`. The RPC ack is still
/// awaited so the caller can detect routing failures within `timeout_ms`.
pub async fn app_notify(
    instance_id: String,
    dst_peer_id: u32,
    channel: String,
    payload: Vec<u8>,
    timeout_ms: i32,
) -> Result<(), String> {
    let svc = lookup_app_rpc(&instance_id)?;
    svc.notify(dst_peer_id, channel, payload, timeout_ms)
        .await
        .map_err(|e| e.to_string())
}

/// Round-trip ping. Returns the measured RTT in milliseconds.
pub async fn peer_ping(
    instance_id: String,
    dst_peer_id: u32,
    timeout_ms: i32,
) -> Result<i64, String> {
    let svc = lookup_app_rpc(&instance_id)?;
    svc.ping(dst_peer_id, timeout_ms)
        .await
        .map_err(|e| e.to_string())
}

/// Stream inbound `Call` and `Notify` events from a running instance into
/// Dart. The future resolves once the EasyTier instance shuts down (the
/// underlying broadcast channel is closed); Dart can re-subscribe after a
/// subsequent `create_server` call.
pub async fn subscribe_app_inbound(
    instance_id: String,
    sink: StreamSink<AppInboundEventC>,
) -> Result<(), String> {
    let svc = lookup_app_rpc(&instance_id)?;
    let mut rx = svc.subscribe_inbound();
    drop(svc);
    loop {
        match rx.recv().await {
            Ok(evt) => {
                let mapped = match evt {
                    app_rpc::AppInboundEvent::Call {
                        from_peer_id,
                        channel,
                        request_id,
                        token,
                        payload,
                    } => AppInboundEventC {
                        kind: AppInboundKindC::Call,
                        from_peer_id,
                        channel,
                        request_id,
                        token,
                        payload,
                    },
                    app_rpc::AppInboundEvent::Notify {
                        from_peer_id,
                        channel,
                        payload,
                    } => AppInboundEventC {
                        kind: AppInboundKindC::Notify,
                        from_peer_id,
                        channel,
                        request_id: 0,
                        token: 0,
                        payload,
                    },
                };
                if sink.add(mapped).is_err() {
                    // Dart cancelled the stream.
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                tracing_log_lagged(&instance_id, skipped);
                // Slow consumer; continue draining.
                continue;
            }
        }
    }
    Ok(())
}

/// Reply to an inbound `Call` identified by `token`. Returns `true` if the
/// reply was delivered to the awaiting RPC task, `false` if the token was
/// already replied to / timed out / never existed.
///
/// `status == 0` is convention for "ok"; positive values are application
/// defined; negative values are reserved for transport-level codes (see
/// [`app_rpc_status`]).
pub async fn app_call_reply(
    instance_id: String,
    token: u64,
    status: i32,
    error_msg: String,
    payload: Vec<u8>,
) -> Result<bool, String> {
    let svc = lookup_app_rpc(&instance_id)?;
    Ok(svc.reply_call(token, status, error_msg, payload))
}

/// Number of `Call` events currently awaiting application replies for the
/// given instance. Useful for diagnostics / liveness checks from Dart.
pub async fn pending_app_call_count(instance_id: String) -> Result<usize, String> {
    let svc = lookup_app_rpc(&instance_id)?;
    Ok(svc.pending_call_count())
}

/// Local peer id for the given instance, exposed so Dart can label outgoing
/// traffic (the EasyTier route table uses the same `peer_id` space).
pub async fn my_peer_id(instance_id: String) -> Result<u32, String> {
    let svc = lookup_app_rpc(&instance_id)?;
    Ok(svc.my_peer_id())
}

fn tracing_log_lagged(instance_id: &str, skipped: u64) {
    // We don't pull `tracing` into AstralNext; just write to stderr at debug
    // verbosity since this is a slow-consumer signal and not a hard error.
    eprintln!(
        "[astral_app_rpc] inbound stream lagged for instance {} (skipped {} events)",
        instance_id, skipped
    );
}
