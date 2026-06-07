# Distributed Lock — Mental Model

> **Problem:** Design a Distributed Locking service for protecting shared resources.
> **Type:** Pessimistic locking
> **Level:** Senior (L5) | **Role:** Engineering Manager

---

## 1. Domain

### Actor
- Clients — pods, services connecting to a remote database/resource

### Configuration
| Parameter | Description |
|---|---|
| `lockResetTimeout` | TTL before auto-expiry — prevents deadlocks on client crash |
| `lockId` | Identifier for the resource being locked |

### State
| Field | Type | Description |
|---|---|---|
| `status` | `Status enum` | AVAILABLE · NOT_AVAILABLE |
| `lockOwnerID` | `string` | Who currently holds the lock |
| `lockAcquiredTime` | `long` | Managed by Redis TTL — not stored explicitly |

### Events
- `tryAcquire(resourceId, ownerId)`
- `release(resourceId, ownerId)`
- `expire()` — handled automatically by Redis TTL

### Transitions

**`tryAcquire()`**
```
AVAILABLE     → NOT_AVAILABLE, ownerID=requestorId, time=now → true
NOT_AVAILABLE → return false (lock held)
```

**`release()`**
```
caller == ownerID → AVAILABLE, ownerID=null → true
caller != ownerID → return false (not the owner)
```

**`expire()`**
```
now - acquiredTime >= resetTimeout → AVAILABLE, ownerID=null
(handled by Redis PX — no background job needed)
```

---

## 2. HLD

### NFR
| Dimension | Target |
|---|---|
| Scale | 10M req/day · avg 115 TPS · peak ~1000 TPS |
| Latency | < 100ms (Redis SET NX sub-ms · network is bottleneck) |
| MTTD | < 1 min via health checks + alerting |
| MTTR | < 15 min via Active-Passive + Redis Sentinel |
| Failover | **Fail CLOSED** — Redis down = reject all acquire attempts |
| Cross-region | Async Redis replication · reads/writes to Primary only |

> **Why fail CLOSED:** Cannot guarantee lock state if Redis is unreachable.
> Fail open = two clients both think they hold the lock = data corruption.

### Components
```
Client
  → API Gateway              (L1: IP rate limit per ownerId + Auth)
  → Load Balancer            (right auto-scaling groups · reject in-flight on scale-down)
  → Lock Management Service  (shared service — NOT sidecar)
  → Redis Primary            (SET NX · TTL-based expiry · noeviction policy)
  → Redis Replica            (passive failover via Sentinel · reads never served here)
  → Downstream Resource      (DB, file, shared state — accessed only after lock acquired)
```

> **Why shared service not sidecar:** All pods must see the same lock state.
> Sidecar = local state = two pods both think they hold the lock = data corruption.

> **Why reads go to Primary only:** Redis replication is async.
> Read replica may not have seen latest SET NX — stale read breaks lock guarantee.

### Data Storage
```
Key:   lock:{resourceId}     ← what is being locked
Value: {ownerId}             ← who holds it
TTL:   lockResetTimeout      ← Redis PX — auto-expiry, no background job needed
```

**Redis eviction policy: `noeviction`**
Silent key eviction = lock appears AVAILABLE when it shouldn't = data corruption.
Redis returns error on memory pressure → operator must intervene. No silent data loss.

**Atomicity:**

Acquire — Redis SET NX (atomic by design):
```
SET lock:{resourceId} {ownerId} NX PX {ttlMs}
→ OK  = acquired
→ nil = already held
```

Release — Lua script (GET + DEL must be atomic):
```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
```

### End-to-End Flow

#### ✅ Path 1 — Lock acquired (happy path)
```
Gateway → LB → Lock Service → Redis SET NX → OK
→ access Downstream Resource
→ release lock (Lua script) → 200 OK
```

#### ❌ Path 2 — Lock not available (rejected)
```
Gateway → LB → Lock Service → Redis SET NX → nil
→ 423 Locked + Retry-After
→ client backs off and retries
(Downstream resource never touched)
```

#### ⚠️ Path 3 — Redis down
```
Gateway → LB → Lock Service → Redis unreachable
→ fail CLOSED → 503 Service Unavailable
→ fire alert → ops recovers within MTTR
```

---

## 3. LLD

### Class Structure

```java
// DistributedLock.java — state for one resource
public class DistributedLock {
    private String resourceId;
    private String ownerId;
    private long lockResetTimeout;
    private Status status;

    enum Status { AVAILABLE, NOT_AVAILABLE }

    DistributedLock(String resourceId, long lockResetTimeout) {
        this.resourceId = resourceId;
        this.lockResetTimeout = lockResetTimeout;
        this.status = Status.AVAILABLE;
    }

    synchronized boolean acquire(String requestorId) {
        if (status == Status.AVAILABLE) {
            status = Status.NOT_AVAILABLE;
            ownerId = requestorId;
            return true;
        }
        return false;
    }

    synchronized boolean release(String callerId) {
        if (status == Status.NOT_AVAILABLE && ownerId.equals(callerId)) {
            status = Status.AVAILABLE;
            ownerId = null;
            return true;
        }
        return false;
    }

    synchronized void expire() {
        // handled by Redis TTL in production
        status = Status.AVAILABLE;
        ownerId = null;
    }
}

// LockManager.java — manages registry of all locks
public class LockManager {
    private HashMap<String, DistributedLock> locks = new HashMap<>();

    public boolean acquire(String resourceId, String ownerId) {
        locks.computeIfAbsent(resourceId,
            k -> new DistributedLock(k, DEFAULT_TIMEOUT));
        return locks.get(resourceId).acquire(ownerId);
    }

    public boolean release(String resourceId, String ownerId) {
        DistributedLock lock = locks.get(resourceId);
        if (lock == null) return false;
        return lock.release(ownerId);
    }

    public DistributedLock.Status getStatus(String resourceId) {
        DistributedLock lock = locks.get(resourceId);
        return lock == null ? DistributedLock.Status.AVAILABLE : lock.status;
    }
}
```

> In production: LockManager becomes stateless.
> Redis SET NX replaces acquire(). Lua script replaces release(). Redis TTL replaces expire().

### Delta from Circuit Breaker
| Circuit Breaker | Distributed Lock |
|---|---|
| One instance per serviceId | LockManager + HashMap per resourceId |
| No ownership | Ownership check in release() |
| HSET Redis command | SET NX PX Redis command |
| recordSuccess/Failure | acquire/release |
| State persists (no TTL) | State expires via Redis TTL |

---

## 4. API Design

### Endpoints
```
POST /locks/{resourceId}/acquire   → tryAcquire()
POST /locks/{resourceId}/release   → release()
GET  /locks/{resourceId}/status    → current state
```

### Request Bodies
```json
// acquire + release
{ "ownerId": "pod-xyz-123" }
```

### Responses
```
200 — lock acquired / released successfully
423 — lock held, retry later (+ Retry-After header)
403 — caller is not the lock owner
503 — Redis unavailable, fail CLOSED
```

> **423 vs 429 vs 503:**
> 423 = resource locked (distributed lock)
> 429 = caller rate exceeded (rate limiter)
> 503 = service/downstream unavailable (circuit breaker)

---

## 5. Production

### Redis Eviction Policy Reference
| Problem | Policy | Reason |
|---|---|---|
| Rate Limiter | `volatile-lru` | Keys have TTL · eviction acceptable |
| Sliding Window | `volatile-lru` | Same |
| Circuit Breaker | `noeviction` | State loss = unexpected behaviour |
| Distributed Lock | `noeviction` | State loss = data corruption |
| LRU Cache | `allkeys-lru` | Pure cache · eviction is the feature |

### Failures
| Failure | Behaviour |
|---|---|
| Redis down | Fail CLOSED — reject all acquire attempts → 503 |
| Redis replica lag | Reads always go to Primary — replica never serves reads |
| Lock contention spike | Alert on high tryAcquire() false rate per resourceId — hotspot signal |
| Pod crash holding lock | Redis TTL auto-expires lock — no manual cleanup needed |
| Scale-down during in-flight | LB rejects new requests to draining pod · in-flight complete first |

### Observability
**Metrics — AWS CloudWatch:**
- `tryAcquire()` success vs rejection rate per resourceId
- Lock hold duration — high values = slow downstream or stuck client
- 423 rate — lock contention signal
- Redis SET NX latency (p50, p99)
- Redis connection error rate

**Logs — Splunk:**
- Every acquire + release with resourceId + ownerId + duration
- Ownership mismatch on release — security signal
- Redis unavailable + fail CLOSED triggered
- Lock expiry events — client crashed while holding lock

**Alerts:**
- Redis unreachable → page immediately
- Lock contention > threshold on one resourceId → hotspot alert
- Lock held > 2x lockResetTimeout → stuck client alert
- 503 spike → page on-call

**Tenant isolation — Salesforce lens:**
- Separate Redis clusters per tenant — noisy neighbour isolation
- Separate pod groups per tenant — one tenant's lock storms don't affect others
- Rate limit per ownerId at API Gateway — prevents single tenant DDOS
- Key pattern: `lock:{tenantId}:{resourceId}` — tenant-scoped locks

### MTTD / MTTR
- MTTD < 1 min — CloudWatch alarm on Redis errors + 503 spike
- MTTR < 15 min — Sentinel promotes replica · Lock Service reconnects automatically

---

## Score (L5 EM bar)

| Layer | Score | Key gap |
|---|---|---|
| Domain | 8.5/10 | Clean state machine · ownership check present · expiry correct |
| HLD | 8.5/10 | Redis Primary reads, fail CLOSED reasoning sharp |
| LLD | 8/10 | Right call to skip implementation · delta articulated clearly |
| API | 8/10 | 423 vs 429 vs 503 distinction strong |
| Production | 9/10 | Led unprompted · tenant isolation · eviction policy · contention monitoring |
| **Overall** | **8.4/10** | Best round · Production thinking now proactive |

### Trend
7.2 → 7.8 → 8.1 → 8.4 — consistent improvement every problem.

> Production checklist — before saying done:
> 1. What breaks?
> 2. How do I know it broke?
> 3. How long until fixed?
> 4. What does the system do while broken?
> 5. How does this behave with 10,000 Salesforce tenants simultaneously?
