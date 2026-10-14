mod dsl;
mod engine;
mod lsh;
mod server;
mod wal;

use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use dsl::{parse_command, DslCommand};
use engine::{ChronoEngine, EpistemicState};

const WAL_FILE: &str = "chrono_lsm.wal";

fn execute_dsl(engine: &mut ChronoEngine, raw_cmd: &str) {
    println!("dsl> {}", raw_cmd);
    match parse_command(raw_cmd) {
        Ok(DslCommand::Fork { new_branch, parent }) => {
            let ts = engine.fork_branch(parent, new_branch).expect("WAL write failed");
            println!("  [OK] Forked Branch {:?} from {:?} at {:?}", new_branch, parent, ts);
        }
        Ok(DslCommand::Commit { branch, state, deps, payload }) => {
            let ts = engine
                .commit_thought(branch, state.clone(), deps.clone(), &payload)
                .expect("WAL write failed");
            println!("  [OK] Committed {:?} on {:?} (deps: {:?}) -> {:?}", state, branch, deps, ts);
        }
        Ok(DslCommand::Refute { branch, target_deps, reason }) => {
            let ts = engine
                .commit_thought(
                    branch,
                    EpistemicState::RefutedTombstone { reason: reason.clone() },
                    target_deps.clone(),
                    &reason,
                )
                .expect("WAL write failed");
            println!(
                "  [TOMBSTONE] Committed Refutation {:?} -> Cascaded invalidation across DAG targets {:?}",
                ts, target_deps
            );
        }
        Ok(DslCommand::Recall { branch, top_k, query }) => {
            let hits = engine.query_context(branch, &query, top_k);
            println!("  [RECALL] Found {} visible memories on {:?} for \"{}\":", hits.len(), branch, query);
            for (sim, rec) in hits {
                println!(
                    "    * [Sim: {:.3}] {:?} ({:?}) | KV-Cache: {}B | \"{}\"",
                    sim,
                    rec.tx_id,
                    rec.state,
                    rec.kv_tensor_bytes.len(),
                    rec.payload
                );
            }
        }
        Ok(DslCommand::Merge { src_branch, dst_branch }) => {
            let txs = engine.merge_branch(src_branch, dst_branch).expect("Merge failed");
            println!("  [MERGE] Cherry-picked {} verified thoughts from {:?} into {:?}: {:?}", txs.len(), src_branch, dst_branch, txs);
        }
        Ok(DslCommand::Benchmark { count }) => {
            let (c, q, e) = engine.run_benchmark(count);
            println!("  [BENCHMARK {} nodes] Commits: {:.0}/s | LSH Queries: {:.0}/s | DAG Evictions: {:.0}/s", count, c, q, e);
        }
        Ok(DslCommand::Compact { threshold }) => {
            let (purged, merged) = engine.compact_l0_to_l1(threshold);
            println!(
                "  [COMPACT L0->L1] Purged {} invalidated/tombstoned nodes | Synthesized {} redundant memories.",
                purged, merged
            );
        }
        Ok(DslCommand::Status) => {
            println!(
                "  [STATUS] L0 MemTable: {} | L1 SSTable: {} | Active Branches: {} | Invalidated Txs: {:?}",
                engine.memtable.len(),
                engine.l1_sstable.len(),
                engine.branches.len(),
                engine.invalidated_txs
            );
        }
        Ok(DslCommand::Help) => {
            println!("Commands: FORK, COMMIT, REFUTE, RECALL, COMPACT, STATUS, SERVE, EXIT");
        }
        Err(e) => println!("  [Syntax Error] {}", e),
    }
    println!();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let serve_mode = args.iter().any(|a| a == "--serve" || a == "-s");

    if !serve_mode {
        let _ = std::fs::remove_file(WAL_FILE);
    }

    let mut engine = ChronoEngine::open(WAL_FILE, 64).expect("Failed to open engine");

    if serve_mode {
        let shared_engine = Arc::new(Mutex::new(engine));
        server::start_http_server(shared_engine, "127.0.0.1:7878").expect("HTTP Server crashed");
        return;
    }

    println!("=========================================================================");
    println!("   ChronoLSM-KV v0.1 PoC — Neuro-Symbolic Time-Travel Storage Engine");
    println!("=========================================================================\n");

    execute_dsl(&mut engine, "COMMIT FACT ON 0 DEPS [] \"Rust storage engine uses write ahead log WAL with CRC32 checksums\"");
    execute_dsl(&mut engine, "COMMIT FACT ON 0 DEPS [1] \"Write ahead log WAL segments flush to disk via CRC32 checksums\"");
    execute_dsl(&mut engine, "FORK 1 FROM 0");
    execute_dsl(&mut engine, "COMMIT HYP(0.65) ON 1 DEPS [1] \"Latency spike is caused by mutex lock contention in WAL writer\"");
    execute_dsl(&mut engine, "COMMIT HYP(0.60) ON 1 DEPS [4] \"Replace WAL writer mutex lock with lock-free ringbuffer\"");
    execute_dsl(&mut engine, "REFUTE [4] ON 1 \"Flamegraph proves WAL mutex lock takes 0.1% CPU; latency is in L0 compaction\"");
    execute_dsl(&mut engine, "COMPACT 0.45");

    println!("=========================================================================");
    println!("Interactive REPL Ready! Type commands below (do NOT type 'chrono>'):");
    println!("Try typing: STATUS   or   SERVE   or   EXIT");
    println!("=========================================================================");

    let stdin = io::stdin();
    loop {
        print!("chrono> ");
        let _ = io::stdout().flush();
        let mut line = String::new();
        if stdin.read_line(&mut line).is_err() || line.trim().eq_ignore_ascii_case("EXIT") {
            break;
        }
        if line.trim().eq_ignore_ascii_case("SERVE") {
            let shared = Arc::new(Mutex::new(engine));
            server::start_http_server(shared, "127.0.0.1:7878").expect("Server error");
            break;
        }
        if !line.trim().is_empty() {
            execute_dsl(&mut engine, line.trim());
        }
    }
}
