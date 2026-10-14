use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::lsh::{cosine_similarity, LshRouter};
use crate::wal::{WalOp, WriteAheadLog};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxTimestamp(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CognitiveKey {
    pub semantic_bucket: u16,
    pub branch_id: BranchId,
    pub rev_timestamp: u64,
}

impl CognitiveKey {
    pub fn new(bucket: u16, branch: BranchId, ts: TxTimestamp) -> Self {
        Self {
            semantic_bucket: bucket,
            branch_id: branch,
            rev_timestamp: u64::MAX - ts.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum EpistemicState {
    VerifiedFact,
    Hypothesis { confidence: f32 },
    RefutedTombstone { reason: String },
}

#[derive(Debug, Clone)]
pub struct CognitiveRecord {
    pub tx_id: TxTimestamp,
    pub branch_id: BranchId,
    pub embedding: Vec<f32>,
    pub kv_tensor_bytes: Arc<[u8]>,
    pub payload: String,
    pub state: EpistemicState,
    pub dependencies: Vec<TxTimestamp>,
}

#[derive(Debug, Clone)]
pub struct BranchMeta {
    pub parent_branch: Option<BranchId>,
    pub fork_timestamp: TxTimestamp,
}

pub struct ChronoEngine {
    clock: u64,
    lsh: LshRouter,
    wal: WriteAheadLog,
    pub memtable: BTreeMap<CognitiveKey, CognitiveRecord>,
    pub l1_sstable: BTreeMap<CognitiveKey, CognitiveRecord>,
    pub branches: HashMap<BranchId, BranchMeta>,
    pub causal_dependents: HashMap<TxTimestamp, Vec<TxTimestamp>>,
    pub invalidated_txs: HashSet<TxTimestamp>,
}

impl ChronoEngine {
    pub fn open<P: AsRef<Path>>(wal_path: P, dim: usize) -> io::Result<Self> {
        let past_ops = WriteAheadLog::recover(&wal_path)?;
        let wal = WriteAheadLog::open(&wal_path)?;

        let mut branches = HashMap::new();
        branches.insert(
            BranchId(0),
            BranchMeta {
                parent_branch: None,
                fork_timestamp: TxTimestamp(0),
            },
        );

        let mut engine = Self {
            clock: 0,
            lsh: LshRouter::new(dim),
            wal,
            memtable: BTreeMap::new(),
            l1_sstable: BTreeMap::new(),
            branches,
            causal_dependents: HashMap::new(),
            invalidated_txs: HashSet::new(),
        };

        for op in past_ops {
            engine.apply_wal_op(op);
        }

        Ok(engine)
    }

    fn apply_wal_op(&mut self, op: WalOp) {
        match op {
            WalOp::Fork { parent, new_branch, ts } => {
                self.clock = self.clock.max(ts);
                self.branches.insert(
                    BranchId(new_branch),
                    BranchMeta {
                        parent_branch: Some(BranchId(parent)),
                        fork_timestamp: TxTimestamp(ts),
                    },
                );
            }
            WalOp::Commit {
                tx_id,
                branch_id,
                state_tag,
                confidence_bits,
                deps,
                payload,
            } => {
                self.clock = self.clock.max(tx_id);
                let state = match state_tag {
                    0 => EpistemicState::VerifiedFact,
                    1 => EpistemicState::Hypothesis {
                        confidence: f32::from_bits(confidence_bits),
                    },
                    _ => EpistemicState::RefutedTombstone {
                        reason: payload.clone(),
                    },
                };
                let dep_ts: Vec<TxTimestamp> = deps.into_iter().map(TxTimestamp).collect();
                self.insert_in_memory(TxTimestamp(tx_id), BranchId(branch_id), state, dep_ts, &payload);
            }
        }
    }

    fn insert_in_memory(
        &mut self,
        ts: TxTimestamp,
        branch: BranchId,
        state: EpistemicState,
        dependencies: Vec<TxTimestamp>,
        payload: &str,
    ) {
        let embedding = self.lsh.embed_text(payload);
        let bucket = self.lsh.hash_vector(&embedding);
        let key = CognitiveKey::new(bucket, branch, ts);

        for &parent_tx in &dependencies {
            self.causal_dependents.entry(parent_tx).or_default().push(ts);
        }

        if matches!(state, EpistemicState::RefutedTombstone { .. }) {
            for &target in &dependencies {
                self.cascade_invalidate(target);
            }
        }

        let token_count = payload.split_whitespace().count().max(1);
        let kv_bytes = vec![(ts.0 & 0xFF) as u8; token_count * 64];

        let record = CognitiveRecord {
            tx_id: ts,
            branch_id: branch,
            embedding,
            kv_tensor_bytes: Arc::from(kv_bytes.into_boxed_slice()),
            payload: payload.to_string(),
            state,
            dependencies,
        };

        self.memtable.insert(key, record);
    }

    pub fn fork_branch(&mut self, parent: BranchId, new_branch: BranchId) -> io::Result<TxTimestamp> {
        self.clock += 1;
        let ts = TxTimestamp(self.clock);
        self.wal.append(&WalOp::Fork {
            parent: parent.0,
            new_branch: new_branch.0,
            ts: ts.0,
        })?;
        self.branches.insert(
            new_branch,
            BranchMeta {
                parent_branch: Some(parent),
                fork_timestamp: ts,
            },
        );
        Ok(ts)
    }

    pub fn commit_thought(
        &mut self,
        branch: BranchId,
        state: EpistemicState,
        dependencies: Vec<TxTimestamp>,
        payload: &str,
    ) -> io::Result<TxTimestamp> {
        self.clock += 1;
        let ts = TxTimestamp(self.clock);

        let (state_tag, confidence_bits) = match &state {
            EpistemicState::VerifiedFact => (0u8, 1.0f32.to_bits()),
            EpistemicState::Hypothesis { confidence } => (1u8, confidence.to_bits()),
            EpistemicState::RefutedTombstone { .. } => (2u8, 0.0f32.to_bits()),
        };

        self.wal.append(&WalOp::Commit {
            tx_id: ts.0,
            branch_id: branch.0,
            state_tag,
            confidence_bits,
            deps: dependencies.iter().map(|d| d.0).collect(),
            payload: payload.to_string(),
        })?;

        self.insert_in_memory(ts, branch, state, dependencies, payload);
        Ok(ts)
    }

    fn cascade_invalidate(&mut self, root_bad_tx: TxTimestamp) {
        let mut stack = vec![root_bad_tx];
        while let Some(curr) = stack.pop() {
            if self.invalidated_txs.insert(curr) {
                if let Some(children) = self.causal_dependents.get(&curr) {
                    for &child in children {
                        stack.push(child);
                    }
                }
            }
        }
    }

    pub fn is_visible(&self, record: &CognitiveRecord, reader_branch: BranchId) -> bool {
        if self.invalidated_txs.contains(&record.tx_id) {
            return false;
        }
        if matches!(record.state, EpistemicState::RefutedTombstone { .. }) {
            return false;
        }

        let mut curr_branch = Some(reader_branch);
        let mut max_allowed_ts = u64::MAX;

        while let Some(b_id) = curr_branch {
            if b_id == record.branch_id {
                return record.tx_id.0 <= max_allowed_ts;
            }
            if let Some(meta) = self.branches.get(&b_id) {
                max_allowed_ts = max_allowed_ts.min(meta.fork_timestamp.0);
                curr_branch = meta.parent_branch;
            } else {
                break;
            }
        }
        false
    }

    pub fn query_context(
        &self,
        reader_branch: BranchId,
        query_text: &str,
        top_k: usize,
    ) -> Vec<(f32, CognitiveRecord)> {
        let query_vec = self.lsh.embed_text(query_text);
        let target_bucket = self.lsh.hash_vector(&query_vec);
        let mut candidates = Vec::new();

        for (key, record) in self.memtable.iter().chain(self.l1_sstable.iter()) {
            let hamming = (key.semantic_bucket ^ target_bucket).count_ones();
            if hamming <= 10 && self.is_visible(record, reader_branch) {
                let sim = cosine_similarity(&query_vec, &record.embedding);
                candidates.push((sim, record.clone()));
            }
        }

        candidates.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(top_k);
        candidates
    }

    pub fn compact_l0_to_l1(&mut self, sim_threshold: f32) -> (usize, usize) {
        let mut purged = 0;
        let mut synthesized = 0;

        let l0_items: Vec<(CognitiveKey, CognitiveRecord)> =
            std::mem::take(&mut self.memtable).into_iter().collect();
        let mut survivors: Vec<(CognitiveKey, CognitiveRecord)> = Vec::new();

        for (key, rec) in l0_items {
            if self.invalidated_txs.contains(&rec.tx_id)
                || matches!(rec.state, EpistemicState::RefutedTombstone { .. })
            {
                purged += 1;
                continue;
            }

            let mut merged = false;
            for (_, existing) in survivors.iter_mut() {
                if existing.branch_id == rec.branch_id {
                    let sim = cosine_similarity(&existing.embedding, &rec.embedding);
                    if sim >= sim_threshold {
                        existing.payload = format!("{} && {}", existing.payload, rec.payload);
                        existing.embedding = self.lsh.embed_text(&existing.payload);
                        synthesized += 1;
                        merged = true;
                        break;
                    }
                }
            }

            if !merged {
                survivors.push((key, rec));
            }
        }

        for (k, v) in survivors {
            self.l1_sstable.insert(k, v);
        }

        (purged, synthesized)
    }

    /// Cherry-picks all surviving (non-invalidated) thoughts from `src` branch,
    /// promotes them to VerifiedFact, and commits them into `dst` branch.
    pub fn merge_branch(&mut self, src: BranchId, dst: BranchId) -> io::Result<Vec<TxTimestamp>> {
        let mut to_merge = Vec::new();
        for (_, rec) in self.memtable.iter().chain(self.l1_sstable.iter()) {
            if rec.branch_id == src
                && !self.invalidated_txs.contains(&rec.tx_id)
                && !matches!(rec.state, EpistemicState::RefutedTombstone { .. })
            {
                to_merge.push((rec.tx_id, rec.payload.clone()));
            }
        }
        to_merge.sort_by_key(|(tx, _)| tx.0);

        let mut merged_txs = Vec::new();
        for (old_tx, payload) in to_merge {
            let new_payload = format!("[Merged from B{} Tx#{}] {}", src.0, old_tx.0, payload);
            let new_tx = self.commit_thought(dst, EpistemicState::VerifiedFact, vec![old_tx], &new_payload)?;
            merged_txs.push(new_tx);
        }
        Ok(merged_txs)
    }

    /// Runs an in-memory Apple Silicon speed benchmark (Commits, LSH Queries, Causal DAG Evictions)
    pub fn run_benchmark(&mut self, count: usize) -> (f64, f64, f64) {
        let start_commit = std::time::Instant::now();
        let bench_branch = BranchId(777);
        self.branches.insert(bench_branch, BranchMeta {
            parent_branch: Some(BranchId(0)),
            fork_timestamp: TxTimestamp(self.clock),
        });

        let mut first_tx = TxTimestamp(0);
        let mut prev_tx = TxTimestamp(0);
        for i in 0..count {
            self.clock += 1;
            let ts = TxTimestamp(self.clock);
            if i == 0 { first_tx = ts; }
            let deps = if i == 0 { vec![] } else { vec![prev_tx] };
            let payload = format!("Synthetic agent reasoning step {} on LSM tree MVCC storage", i);
            self.insert_in_memory(ts, bench_branch, EpistemicState::Hypothesis { confidence: 0.8 }, deps, &payload);
            prev_tx = ts;
        }
        let commit_elapsed = start_commit.elapsed().as_secs_f64().max(0.000001);
        let commits_per_sec = (count as f64) / commit_elapsed;

        let start_query = std::time::Instant::now();
        let q_runs = 1000;
        for _ in 0..q_runs {
            let _ = self.query_context(bench_branch, "LSM tree MVCC storage reasoning", 5);
        }
        let query_elapsed = start_query.elapsed().as_secs_f64().max(0.000001);
        let queries_per_sec = (q_runs as f64) / query_elapsed;

        let start_evict = std::time::Instant::now();
        self.cascade_invalidate(first_tx);
        let evict_elapsed = start_evict.elapsed().as_secs_f64().max(0.000001);
        let evicts_per_sec = (count as f64) / evict_elapsed;

        // Clean up synthetic benchmark nodes so the visual DAG stays clean
        self.memtable.retain(|_, v| v.branch_id != bench_branch);
        self.branches.remove(&bench_branch);
        self.invalidated_txs.retain(|t| t.0 < first_tx.0);

        (commits_per_sec, queries_per_sec, evicts_per_sec)
    }
}
