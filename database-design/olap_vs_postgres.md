# OLAP vs Postgres vs Redshift vs DuckDB — Study Notes

A working note on analytical query workloads: what OLAP actually buys you over Postgres, why pre-aggregation is a latency lever, and where DuckDB fits.

---

## 1. OLTP vs OLAP — two workload patterns

Both can run on PostgreSQL, but they are opposite shapes.

| Aspect | OLTP | OLAP |
|--------|------|------|
| Query type | Simple, indexed lookups | Complex aggregations, joins |
| Mutation profile | Write-heavy (INSERT/UPDATE/DELETE) | Read-heavy (mostly SELECT) |
| Data touched | 1–1,000 rows | 1M–billions of rows |
| Response time | < 100 ms | Seconds to minutes |
| Concurrency | High (1000s) | Low (10s–100s) |
| Schema | Normalized | Denormalized / star schema |
| Access pattern | Row-level | Column-level aggregation |
| Engine fit | Postgres, MySQL/InnoDB | DuckDB, Redshift, Snowflake, BigQuery |

**OLTP example**
```sql
UPDATE orders SET status = 'shipped' WHERE order_id = 12345;
SELECT * FROM accounts WHERE account_id = 5678;
```

**OLAP example**
```sql
SELECT region, date_trunc('month', transaction_date) AS month,
       SUM(amount) AS revenue, COUNT(*) AS txns
FROM transactions
JOIN customers ON transactions.customer_id = customers.id
WHERE transaction_date >= now() - interval '12 months'
GROUP BY 1, 2
ORDER BY month DESC;
```

---

## 2. OLAP "vs" Postgres — really *against the grain of* Postgres

Postgres is a row-store, MVCC, OLTP-first engine. Analytical queries want the opposite of what it optimizes for.

**Where the friction shows up**

1. **Row storage vs columnar access** — Postgres stores whole tuples in heap pages. A query reading 2 of 50 columns still pulls entire rows off disk, wasting I/O on 48 unused columns.
2. **Weak compression** — row layout kills compression ratios; columnar gets 10–50x via dictionary/RLE/delta because adjacent column values are similar.
3. **Tuple-at-a-time execution** — Volcano-style iterator, one tuple per `next()`. Columnar engines process batches of ~1,000+ values (cache- and SIMD-friendly).
4. **MVCC overhead on big scans** — visibility checks, dead tuples, vacuum pressure compound across billion-row scans.

**Bending Postgres toward OLAP (before reaching for another engine)**

```sql
-- BRIN: tiny index, ideal for naturally time-ordered append data
CREATE INDEX idx_events_brin ON events USING BRIN (event_timestamp);

-- Declarative partitioning by time → prune whole partitions
CREATE TABLE events (event_timestamp timestamptz, ...)
PARTITION BY RANGE (event_timestamp);

-- Materialized view for pre-aggregated KPIs
CREATE MATERIALIZED VIEW kpi_monthly AS
SELECT region, date_trunc('month', event_timestamp) AS month, SUM(amount)
FROM events GROUP BY 1, 2;
```

Other levers: `max_parallel_workers_per_gather`, read replicas to isolate analytical load, columnar extensions (Citus, Hydra, ParadeDB/pg_analytics).

---

## 3. Redshift — a Postgres fork that rebuilt everything below the parser

Redshift forked from PostgreSQL 8.0.2 (~2005). It still speaks the Postgres wire protocol and SQL dialect, but the storage and execution engine were entirely replaced. "Same front door, completely different building."

**What changed under the hood**

1. **Single-node row-store → MPP columnar.** Leader node parses/plans/distributes; N compute nodes each own a data slice and execute in parallel. Columnar storage with aggressive compression (AZ64, ZSTD).
2. **DISTKEY + SORTKEY** — the two knobs with no Postgres equivalent:

```sql
CREATE TABLE events (
    event_id    bigint,
    customer_id bigint,
    event_ts    timestamptz,
    amount      numeric
)
DISTSTYLE KEY
DISTKEY (customer_id)            -- co-locates matching rows on the same node → local joins
COMPOUND SORTKEY (event_ts);     -- physical order → zone-map block pruning
```
   - **DISTKEY** controls which slice each row lands on. Join two big tables on the same distkey → no network shuffle. Wrong distkey → broadcast/redistribute on every query (the #1 Redshift perf killer).
   - **SORTKEY** physically orders rows so zone maps (per-block min/max, like automatic BRIN) skip blocks.
3. **Updates are second-class** — UPDATE/DELETE are delete-mark + insert; needs VACUUM to reclaim/re-sort. Fine for batch analytics, terrible for OLTP.
4. **Managed concurrency** — limited query slots via WLM, with concurrency scaling (transient clusters) to absorb bursts.

> Modern note: RA3 nodes (S3-backed managed storage), Spectrum (query S3 directly), and a serverless option decouple compute from storage — softening the "paying for an idle cluster" critique. The distkey/sortkey/VACUUM mental model still governs performance.

---

## 4. Why pre-aggregation is a latency lever

A KPI widget shows ~12 numbers but computing them from raw events scans billions of rows and discards 99.99%+ of what it touched. Tiny output, enormous work — that ratio is the problem.

**Why it specifically reduces latency (not just cost)**

- **Moves work off the read path** — the expensive scan+aggregate runs at refresh time, in the background, not while a user waits.
- **Amortizes identical work** — the same GROUP BY runs thousands of times/day over immutable historical data. Compute-once, read-many.
- **Crushes tail latency** — a few-hundred-row lookup is boringly consistent vs a variable billion-row scan. Matters for p99 dashboard SLAs.
- **Frees concurrency slots** — cheap queries hold a slot for ms not seconds, collapsing queue delay under load (Redshift WLM; DuckDB core/RAM contention).

**Forms (by staleness/flexibility trade)**
- Materialized views / summary tables (fixed grain)
- Incremental/rolling aggregation (fold in only new events — fits append-ordered streams)
- Cube / multi-grain pre-agg (keeps drill-downs fast)

**Where it bites back**
- **Staleness** — lags by the refresh interval. Fix: *lambda pattern* — serve history from pre-agg, UNION a small live scan over the hot recent window.
- **Non-additive measures** — SUM/COUNT roll up cleanly; **COUNT(DISTINCT), percentiles, ratios do not.** Pre-agg every needed grain, or store mergeable sketches (HyperLogLog for distinct, t-digest for percentiles).
- **Cardinality/flexibility loss** — pre-agg fixes grain + dimensions; ad-hoc drill below the grain falls back to raw scan. Over-cubing explodes storage/refresh cost.

> Mental model: pre-aggregation is **caching the answer to an expensive, repeated, slowly-changing query** — and like all caching, it buys latency with staleness and invalidation complexity.

---

## 5. What does an OLAP engine actually add over Postgres?

Four architectural choices that **compound**, each attacking read amplification at a different layer:

1. **Columnar storage** → read only the columns you touch (10–20x I/O cut on wide tables). *Cannot be fixed in vanilla Postgres — baked into the heap format.*
2. **Columnar compression** → 10–50x fewer bytes to move and decompress.
3. **Vectorized execution** → batches of ~1,000+ values per operation vs tuple-at-a-time. Turns "fewer bytes read" into "fewer CPU cycles spent."
4. **(Redshift) MPP** → fan the scan across nodes; horizontal scale beyond one machine.

**Each Postgres OLAP lever only *imitates* one piece:**

| OLAP capability | Postgres imitation | Why it caps out |
|---|---|---|
| Columnar I/O | covering indexes, columnar ext. | bolt-on, not native heap; bloat, write cost |
| Zone-map skipping | BRIN | only on naturally-ordered data, coarser |
| Parallel scan | `parallel_workers` | still one node's resources |
| Compression | TOAST | large values only; not the access pattern |
| **Pre-aggregation** | **materialized views** | **equal — it's a technique, not an engine feature** |

**The honest split**
- **Fixed, known KPIs** → a Postgres materialized view competes fine. OLAP adds little here.
- **Ad-hoc scans / drill-downs below the pre-agg grain / unanticipated slicing** → columnar + vectorized (+ MPP) is a structural 10–100x Postgres's row-store cannot match, only approximate. *This is the irreducible advantage.*
- **Always-on, high-concurrency shared BI** → MPP + WLM/concurrency-scaling is a different scale class than one Postgres node.

This is why a platform ends up doing **both**: pre-aggregating (cheap latency for predictable widgets) *and* moving to a columnar engine (fast raw scans for everything not pre-aggregated). Different halves of the problem.

---

## 6. DuckDB — not "just columnar fast-scan"

DuckDB's speed = **four things, not one**; columnar alone is the least of them.

1. **Columnar storage** — read only touched columns. Table stakes for any column-store.
2. **Vectorized execution** — *orthogonal to storage and arguably the bigger lever.* You can have columnar storage and still be slow with tuple-at-a-time execution. Batches of ~1,000+ values are what convert fewer bytes read into fewer CPU cycles.
3. **Compression + zone maps** — 10–50x compression; per-block min/max skips chunks that can't match.
4. **Query-in-place on Parquet/Arrow** — no ETL load step; in-process, zero-copy from Arrow. The database comes to the data.

**The platform-relevant property your phrasing misses: zero-ops embedding.**
DuckDB is a **library in your process** (like SQLite), not a server. That's why it wins for dashboards/eval loops over Redshift — not because Redshift can't scan fast, but because DuckDB delivers columnar+vectorized with:
- no cluster to provision
- no distkey/sortkey/VACUUM/WLM to tune
- no ETL pipeline
- runs inside the same process as your code

**The trade:** single node — ceiling is one fat machine's RAM/cores. Need cross-node fan-out or hundreds of concurrent shared-warehouse users → back to Redshift/MPP.

> Refined statement: DuckDB is fast at analytical scans because it's **columnar *and* vectorized** (storage layout + execution model, plus compression and in-place Parquet/Arrow reads) — and it fits a platform specifically because it delivers that with **near-zero operational weight** as an embedded engine, at the cost of being single-node.

> Hook: **columnar answers "what does it read"; vectorized answers "how fast does it process what it read"; embedded answers "what does it cost you to run."**

---

## TL;DR

- OLTP and OLAP are opposite workload shapes; both run on Postgres but it's built for OLTP.
- Postgres can be bent toward OLAP (BRIN, partitioning, mat views, replicas, columnar extensions) but its row-store + tuple-iterator architecture caps out on large scans.
- Redshift = Postgres front-end on an MPP columnar engine; live or die by DISTKEY/SORTKEY/VACUUM/WLM.
- Pre-aggregation is a latency lever because it moves repeated, immutable scan+aggregate work off the synchronous read path — at the cost of staleness, non-additive-measure handling, and lost flexibility.
- The irreducible OLAP advantage over Postgres is **fast scans over raw data you didn't pre-aggregate** (columnar + vectorized + optional MPP).
- DuckDB = columnar + vectorized + compression + in-place reads, delivered as a **zero-ops embedded** engine for the medium-data, single-node sweet spot.
