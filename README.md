# ⚡ ChronoLSM-KV

**A Neuro-Symbolic, Time-Travel LSM Storage Engine for Autonomous AI Agents (100% Pure Rust)**

![CI](https://github.com/nit9451/chrono-lsm-kv/actions/workflows/ci.yml/badge.svg)
![Rust](https://img.shields.io/badge/rust-std%20only-orange.svg)
![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)

---

## 🧠 Why ChronoLSM-KV?

Traditional Vector Databases (HNSW/IVF) and Key-Value stores were not designed for **long-horizon, multi-step AI agent reasoning**:
1. **RAG is Amnesiac:** Standard vector DBs store static text chunks and floats. They cannot track *why* an agent believed Hypothesis A at step T1 and refuted it at step T2.
2. **Tree-of-Thoughts Forking is Expensive:** Cloning full prompt contexts and vector namespaces for parallel sub-agents causes massive memory amplification.
3. **Poisoned Reasoning Chains:** When an early assumption is disproved by a tool execution (e.g., profiler trace or compiler error), downstream thoughts derived from that assumption remain in vector memory and cause recurring hallucinations.

**ChronoLSM-KV** solves this by fusing **Log-Structured Merge-Tree (LSM)** storage mechanics with **Multi-Version Concurrency Control (MVCC)**, **Locality-Sensitive Hashing (LSH)**, and a **Causal Dependency Directed Acyclic Graph (DAG)**.

---

## 🏗️ Core Architectural Innovations

| Mechanism | Traditional Vector / KV DB | ChronoLSM-KV Architecture |
| :--- | :--- | :--- |
| **1. Zero-Copy Thought Branching** | Duplicate prompt histories across parallel agents. | **O(1) MVCC Snapshot Isolation:** Sub-agents fork reasoning branches (FORK 99 FROM 0) that dynamically inherit parent memories prior to the fork timestamp. |
| **2. Causal DAG Tombstone Cascade** | Manual deletion by key ID; downstream hallucinations remain. | **Surgical Hallucination Eviction:** Thoughts declare DEPS [tx_id]. Issuing REFUTE [tx_id] walks the causal DAG and automatically invalidates all downstream dependent plans. |
| **3. Epistemic Compaction (L0 -> L1)** | Blindly drops overwritten keys or rebuilds HNSW graphs. | **Semantic Garbage Collection:** Background compaction purges causally refuted nodes and synthesizes redundant L0 memories into unified L1 SSTable rules. |
| **4. KV-Cache Tensor Attachment** | Returns raw text requiring LLM re-tokenization & prefill. | **Pre-computed Attention Slices:** Each record attaches a quantized KV-cache tensor slice alongside its 16-bit LSH vector signature. |

---

## 🚀 Quick Start (Zero External Dependencies)

ChronoLSM-KV is built entirely with the **Rust Standard Library (std)** and compiles in ~1 second.

### 1. Run the Automated 6-Stage Verification & Interactive REPL
```bash
cargo run --release<img width="2940" height="1608" alt="image" src="https://github.com/user-attachments/assets/3592a2b9-68c6-4e66-8e91-6437da74151e" />

```

### 2. Start the HTTP/1.1 REST Daemon & Live Visual DAG Inspector
```bash
cargo run --release -- --serve
# In a second terminal:
open dashboard.html
```

---

## 💻 Custom Query DSL Reference

```sql
-- 1. Commit verified facts on Main Branch 0
COMMIT FACT ON 0 DEPS [] "Storage engine uses write-ahead logging (WAL) with IEEE CRC32 checksums"

-- 2. Fork a zero-copy speculative reasoning branch (Branch 99 from Branch 0)
FORK 99 FROM 0

-- 3. Chain speculative hypotheses with Causal DAG dependencies (DEPS)
COMMIT HYP(0.75) ON 99 DEPS [1] "Latency spike is caused by DB connection pool exhaustion"
COMMIT HYP(0.70) ON 99 DEPS [3] "Increase SQLAlchemy pool_size to 200"

-- 4. Semantic + MVCC visibility-aware context recall
RECALL ON 99 TOP 5 "Latency spike DB pool"

-- 5. Refute root hypothesis Tx #3 (Automatically cascades & evicts dependent Tx #4!)
REFUTE [3] ON 99 "pg_stat_activity shows 3 idle connections; DB pool is healthy"

-- 6. Cherry-pick surviving discoveries from a speculative branch back into Main Branch 0
MERGE 2 INTO 0

-- 7. Trigger Epistemic Compaction (L0 MemTable -> L1 SSTable)
COMPACT 0.45

-- 8. Run native throughput benchmark (Commits/sec, LSH Queries/sec, DAG Evictions/sec)
BENCHMARK 25000
```

---
![Uploading image.png…]()<img width="1459" height="804" alt="Screenshot 2026-10-14 at 9 29 03 AM" src="https://github.com/user-attachments/assets/efbf99cf-1b3f-4ba5-adb7-64866d6c7255" />



## 📜 License
MIT License — Copyright (c) 2026 Nitish Kumar Purohit
