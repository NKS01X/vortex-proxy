use async_trait::async_trait;
use pingora::prelude::*;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::RwLock;

use kind::kind_service_client::KindServiceClient;
use kind::{PutRequest, WatchRequest};
use serde::{Deserialize, Serialize};

pub mod kind {
    tonic::include_proto!("kind");
}

pub type RoutingTable = Arc<RwLock<HashMap<String, Vec<String>>>>;
pub type MetricsTable = Arc<RwLock<HashMap<String, AtomicUsize>>>;

pub struct VortexRouter {
    pub routing_table: RoutingTable,
    pub metrics_table: MetricsTable,
    pub request_counter: AtomicUsize,
}

#[async_trait]
impl ProxyHttp for VortexRouter {
    type CTX = ();
    
    fn new_ctx(&self) -> () {
        ()
    }

    async fn upstream_peer(&self, session: &mut Session, _ctx: &mut ()) -> Result<Box<HttpPeer>> {
        let host_header = session.get_header("host");
        let host = host_header.and_then(|v| v.to_str().ok()).unwrap_or("");
        
        let client_id = host.split('.').next().unwrap_or("");
        println!("[PROXY] Incoming request for Host: '{}' | Extracted Client ID: '{}'", host, client_id);
        
        if client_id.is_empty() {
            let _ = session.respond_error(502).await;
            return Err(pingora::Error::explain(
                pingora::ErrorType::Custom("No client ID"),
                "No client ID provided in Host header",
            ));
        }

        let ips = {
            let table = self.routing_table.read().await;
            table.get(client_id).cloned()
        };
        
        println!("[PROXY] Routing table lookup for '{}' found: {:?}", client_id, ips);

        let ip = match ips {
            Some(list) if !list.is_empty() => {
                let idx = self.request_counter.fetch_add(1, Ordering::Relaxed) % list.len();
                let selected_ip = list[idx].clone();
                println!("[PROXY] Routing to backend IP: {}", selected_ip);
                selected_ip
            }
            _ => {
                println!("[PROXY] FATAL: No backends available! Returning 502.");
                let _ = session.respond_error(502).await;
                return Err(pingora::Error::explain(
                    pingora::ErrorType::Custom("No backends"),
                    "No backends available for this client",
                ));
            }
        };

        // ... (Metrics section stays exactly the same)
        {
            let table = self.metrics_table.read().await;
            if let Some(counter) = table.get(client_id) {
                counter.fetch_add(1, Ordering::Relaxed);
            } else {
                drop(table); 
                let mut write_table = self.metrics_table.write().await;
                write_table
                    .entry(client_id.to_string())
                    .or_insert_with(|| AtomicUsize::new(0))
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        let peer = HttpPeer::new(ip, false, "".to_string());
        Ok(Box::new(peer))
    }
}

#[derive(Deserialize)]
struct RouteUpdate {
    client_id: String,
    ips: Vec<String>,
}

#[derive(Serialize)]
struct MetricUpdate<'a> {
    client_id: &'a str,
    current_rps: usize,
}

pub async fn run_kind_db_watcher(table: RoutingTable) {
    let db_url = std::env::var("KIND_DB_URL").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());

    loop {
        println!("[WATCHER] Attempting to connect to Kind DB at: {}", db_url);
        match KindServiceClient::connect(db_url.clone()).await {
            Ok(mut client) => {
                println!("[WATCHER] Connected to Kind DB! Opening Watch stream...");
                let req = WatchRequest { prefix: "router:".to_string() };
                
                if let Ok(response) = client.watch(tonic::Request::new(req)).await {
                    let mut stream = response.into_inner();
                    
                    while let Ok(Some(res)) = stream.message().await {
                        // Because of the proto tag shift:
                        // res.operation_type holds the DB Key (e.g., "router:myapp")
                        // res.key holds the JSON String
                        println!("[WATCHER] Received DB Event! DB_Key: {}, Payload: {}", res.operation_type, res.key);
                        
                        // 1. Check if the DB key starts with "router:"
                        if res.operation_type.starts_with("router:") {
                            
                            // 2. Parse the JSON payload directly from the res.key string
                            match serde_json::from_str::<RouteUpdate>(&res.key) {
                                Ok(update) => {
                                    println!("[WATCHER] SUCCESS! Parsed route update for '{}': {:?}", update.client_id, update.ips);
                                    let mut write_guard = table.write().await;
                                    write_guard.insert(update.client_id, update.ips);
                                }
                                Err(e) => {
                                    println!("[WATCHER] ERROR: Failed to parse JSON! {}", e);
                                }
                            }
                        }
                    }
                    println!("[WATCHER] Stream disconnected.");
                }
            }
            Err(e) => println!("[WATCHER] Connection failed: {}. Retrying in 2s...", e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}
pub async fn run_metrics_publisher(metrics: MetricsTable) {
    let db_url = std::env::var("KIND_DB_URL").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());

    loop {
        if let Ok(mut client) = KindServiceClient::connect(db_url.clone()).await {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                let mut updates = Vec::new();
                {
                    let table = metrics.read().await;
                    for (client_id, counter) in table.iter() {
                        let rps = counter.swap(0, Ordering::Relaxed);
                        if rps > 0 {
                            updates.push((client_id.clone(), rps));
                        }
                    }
                }

                let mut connection_alive = true;
                for (client_id, current_rps) in updates {
                    let payload = MetricUpdate {
                        client_id: &client_id,
                        current_rps,
                    };
                    if let Ok(value) = serde_json::to_vec(&payload) {
                        let req = PutRequest {
                            key: format!("vortex:metrics:{}", client_id),
                            value,
                        };
                        // If the put fails (DB crashed/restarted), break inner loop to reconnect
                        if client.put(tonic::Request::new(req)).await.is_err() {
                            connection_alive = false;
                            break;
                        }
                    }
                }
                
                if !connection_alive {
                    break;
                }
            }
        }
        // Wait before trying to reconnect
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

fn main() {
    let mut server = Server::new(None).unwrap();

    // Prevent Docker from killing the container by forcing Pingora to stay in the foreground
    if let Some(conf) = std::sync::Arc::get_mut(&mut server.configuration) {
        conf.daemon = false;
    }
    
    server.bootstrap();

    let routing_table: RoutingTable = Arc::new(RwLock::new(HashMap::new()));
    let metrics_table: MetricsTable = Arc::new(RwLock::new(HashMap::new()));

    let rt_clone = routing_table.clone();
    let mt_clone = metrics_table.clone();
    
    // Spawn a dedicated OS thread to run a background Tokio runtime
    // This prevents "no reactor running" panics from Pingora's synchronous main setup
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            tokio::spawn(run_kind_db_watcher(rt_clone));
            tokio::spawn(run_metrics_publisher(mt_clone));
            
            // Keep this runtime alive indefinitely to process the background loops
            std::future::pending::<()>().await;
        });
    });

    let router = VortexRouter {
        routing_table,
        metrics_table,
        request_counter: AtomicUsize::new(0),
    };

    let mut proxy_service = pingora::proxy::http_proxy_service(&server.configuration, router);
    proxy_service.add_tcp("0.0.0.0:8000");

    server.add_service(proxy_service);
    
    // Start Pingora's worker threads
    server.run_forever();
}
