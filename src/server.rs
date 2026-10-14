use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::dsl::{parse_command, DslCommand};
use crate::engine::{ChronoEngine, EpistemicState};

pub fn start_http_server(engine: Arc<Mutex<ChronoEngine>>, addr: &str) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    println!("=========================================================================");
    println!("  ChronoLSM-KV HTTP/1.1 Server listening on http://{}", addr);
    println!("  Endpoints: /v1/query | /v1/batch | /v1/status | /v1/dag");
    println!("=========================================================================
");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let engine_ref = Arc::clone(&engine);
                thread::spawn(move || {
                    let _ = handle_connection(stream, engine_ref);
                });
            }
            Err(e) => eprintln!("[TCP Accept Error] {}", e),
        }
    }
    Ok(())
}

fn handle_connection(mut stream: TcpStream, engine: Arc<Mutex<ChronoEngine>>) -> std::io::Result<()> {
    let mut buf = [0u8; 8192];
    let bytes_read = stream.read(&mut buf)?;
    if bytes_read == 0 {
        return Ok(());
    }

    let raw_req = String::from_utf8_lossy(&buf[..bytes_read]);
    let (headers_part, body_part) = match raw_req.split_once("\r\n\r\n") {
        Some((h, b)) => (h, b.to_string()),
        None => return send_json(&mut stream, 400, r#"{"ok":false,"error":"Malformed HTTP request"}"#),
    };

    let mut lines = headers_part.lines();
    let request_line = lines.next().unwrap_or("");
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return send_json(&mut stream, 400, r#"{"ok":false,"error":"Invalid request line"}"#);
    }

    let method = parts[0];
    let path = parts[1];

    if method == "OPTIONS" {
        return send_json(&mut stream, 200, r#"{"ok":true}"#);
    }

    let body_str = body_part.trim().to_string();

    match (method, path) {
        ("GET", "/v1/status") => {
            let guard = engine.lock().unwrap();
            let json = ExecuteResult::status_json(&guard);
            send_json(&mut stream, 200, &json)
        }
        ("GET", "/v1/dag") => {
            let guard = engine.lock().unwrap();
            let json = ExecuteResult::dag_json(&guard);
            send_json(&mut stream, 200, &json)
        }
        ("POST", "/v1/query") => {
            let dsl_query = extract_dsl_from_body(&body_str);
            let mut guard = engine.lock().unwrap();
            let (status, json) = ExecuteResult::run_single(&mut guard, &dsl_query);
            send_json(&mut stream, status, &json)
        }
        ("POST", "/v1/batch") => {
            let mut guard = engine.lock().unwrap();
            let mut results = Vec::new();
            for line in body_str.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    continue;
                }
                let (_, res_json) = ExecuteResult::run_single(&mut guard, trimmed);
                results.push(res_json);
            }
            let batch_json = format!(r#"{{"ok":true,"batch_results":[{}]}}"#, results.join(","));
            send_json(&mut stream, 200, &batch_json)
        }
        _ => send_json(&mut stream, 404, r#"{"ok":false,"error":"Not found"}"#),
    }
}

fn extract_dsl_from_body(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        if let Some(idx) = trimmed.find("\"query\"") {
            let after_key = &trimmed[idx + 7..];
            if let Some(colon) = after_key.find(':') {
                let val_part = after_key[colon + 1..].trim();
                if val_part.starts_with('"') {
                    let mut out = String::new();
                    let mut chars = val_part[1..].chars();
                    while let Some(c) = chars.next() {
                        if c == '\\' {
                            if let Some(next_c) = chars.next() {
                                out.push(next_c);
                            }
                        } else if c == '"' {
                            break;
                        } else {
                            out.push(c);
                        }
                    }
                    return out;
                }
            }
        }
    }
    trimmed.to_string()
}

struct ExecuteResult;

impl ExecuteResult {
    fn status_json(engine: &ChronoEngine) -> String {
        let inv: Vec<String> = engine.invalidated_txs.iter().map(|t| t.0.to_string()).collect();
        format!(
            r#"{{"ok":true,"l0_memtable_count":{},"l1_sstable_count":{},"active_branches":{},"invalidated_txs":[{}]}}"#,
            engine.memtable.len(),
            engine.l1_sstable.len(),
            engine.branches.len(),
            inv.join(",")
        )
    }

    fn dag_json(engine: &ChronoEngine) -> String {
        let mut branch_list: Vec<_> = engine.branches.iter().collect();
        branch_list.sort_by_key(|(b_id, _)| b_id.0);
        let mut branches_str = Vec::new();
        for (b_id, meta) in branch_list {
            let parent_str = match meta.parent_branch {
                Some(p) => p.0.to_string(),
                None => "null".to_string(),
            };
            branches_str.push(format!(
                r#"{{"id":{},"parent":{},"fork_ts":{}}}"#,
                b_id.0, parent_str, meta.fork_timestamp.0
            ));
        }

        let mut all_recs: Vec<(&str, &crate::engine::CognitiveRecord)> = Vec::new();
        for (_, rec) in &engine.l1_sstable {
            all_recs.push(("L1", rec));
        }
        for (_, rec) in &engine.memtable {
            all_recs.push(("L0", rec));
        }
        all_recs.sort_by_key(|(_, r)| r.tx_id.0);

        let mut nodes_str = Vec::new();
        for (tier, rec) in all_recs {
            let is_inv = engine.invalidated_txs.contains(&rec.tx_id)
                || matches!(rec.state, EpistemicState::RefutedTombstone { .. });
            let st = match &rec.state {
                EpistemicState::VerifiedFact => "FACT".to_string(),
                EpistemicState::Hypothesis { confidence } => format!("HYP({:.2})", confidence),
                EpistemicState::RefutedTombstone { .. } => "TOMBSTONE".to_string(),
            };
            let deps: Vec<String> = rec.dependencies.iter().map(|d| d.0.to_string()).collect();
            nodes_str.push(format!(
                r#"{{"tx_id":{},"branch":{},"tier":"{}","state":"{}","invalidated":{},"kv_bytes":{},"deps":[{}],"payload":"{}"}}"#,
                rec.tx_id.0,
                rec.branch_id.0,
                tier,
                st,
                is_inv,
                rec.kv_tensor_bytes.len(),
                deps.join(","),
                rec.payload.replace('"', "\\\"")
            ));
        }

        format!(
            r#"{{"ok":true,"branches":[{}],"nodes":[{}]}}"#,
            branches_str.join(","),
            nodes_str.join(",")
        )
    }

    fn run_single(engine: &mut ChronoEngine, raw_cmd: &str) -> (u16, String) {
        match parse_command(raw_cmd) {
            Ok(DslCommand::Fork { new_branch, parent }) => match engine.fork_branch(parent, new_branch) {
                Ok(ts) => (
                    200,
                    format!(
                        r#"{{"ok":true,"op":"FORK","new_branch":{},"parent_branch":{},"fork_tx":{}}}"#,
                        new_branch.0, parent.0, ts.0
                    ),
                ),
                Err(e) => (500, format!(r#"{{"ok":false,"error":"{}"}}"#, e)),
            },
            Ok(DslCommand::Commit { branch, state, deps, payload }) => {
                match engine.commit_thought(branch, state, deps, &payload) {
                    Ok(ts) => (200, format!(r#"{{"ok":true,"op":"COMMIT","tx_id":{},"branch":{}}}"#, ts.0, branch.0)),
                    Err(e) => (500, format!(r#"{{"ok":false,"error":"{}"}}"#, e)),
                }
            }
            Ok(DslCommand::Refute { branch, target_deps, reason }) => {
                let state = EpistemicState::RefutedTombstone { reason: reason.clone() };
                match engine.commit_thought(branch, state, target_deps, &reason) {
                    Ok(ts) => (200, format!(r#"{{"ok":true,"op":"REFUTE","tombstone_tx":{}}}"#, ts.0)),
                    Err(e) => (500, format!(r#"{{"ok":false,"error":"{}"}}"#, e)),
                }
            }
            Ok(DslCommand::Recall { branch, top_k, query }) => {
                let hits = engine.query_context(branch, &query, top_k);
                let mut items = Vec::new();
                for (sim, rec) in hits {
                    items.push(format!(
                        r#"{{"tx_id":{},"branch":{},"similarity":{:.4},"kv_cache_bytes":{},"payload":"{}"}}"#,
                        rec.tx_id.0,
                        rec.branch_id.0,
                        sim,
                        rec.kv_tensor_bytes.len(),
                        rec.payload.replace('"', "\\\"")
                    ));
                }
                (200, format!(r#"{{"ok":true,"op":"RECALL","results":[{}]}}"#, items.join(",")))
            }
            Ok(DslCommand::Merge { src_branch, dst_branch }) => {
                match engine.merge_branch(src_branch, dst_branch) {
                    Ok(txs) => {
                        let list: Vec<String> = txs.iter().map(|t| t.0.to_string()).collect();
                        (200, format!(r#"{{"ok":true,"op":"MERGE","src_branch":{},"dst_branch":{},"merged_txs":[{}]}}"#, src_branch.0, dst_branch.0, list.join(",")))
                    }
                    Err(e) => (500, format!(r#"{{"ok":false,"error":"{}"}}"#, e)),
                }
            }
            Ok(DslCommand::Benchmark { count }) => {
                let (c_sec, q_sec, e_sec) = engine.run_benchmark(count);
                (200, format!(r#"{{"ok":true,"op":"BENCHMARK","nodes_tested":{},"commits_per_sec":{:.0},"lsh_queries_per_sec":{:.0},"dag_evictions_per_sec":{:.0}}}"#, count, c_sec, q_sec, e_sec))
            }
            Ok(DslCommand::Compact { threshold }) => {
                let (purged, merged) = engine.compact_l0_to_l1(threshold);
                (200, format!(r#"{{"ok":true,"op":"COMPACT","purged":{},"merged":{}}}"#, purged, merged))
            }
            Ok(DslCommand::Status) => (200, Self::status_json(engine)),
            Ok(DslCommand::Help) => (200, r#"{"ok":true}"#.to_string()),
            Err(e) => (400, format!(r#"{{"ok":false,"error":"{}"}}"#, e)),
        }
    }
}

fn send_json(stream: &mut TcpStream, status_code: u16, json_body: &str) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status_code,
        json_body.len(),
        json_body
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()
}
