# Sliding Window Rate Limiter — Mental Model

> **Problem:** Design a rate limiter using the Sliding Window (Log-based) algorithm.
> **Level:** Senior (L5) | **Role:** Engineering Manager

---

## 1. Domain

### Actor
- Clients — Devices, IPs, Users

### Configuration
| Parameter | Description |
|---|---|
| `windowSize` | Duration of the sliding window (ms) |
| `allowedRequestThreshold` | Max requests allowed within any window |
| `scope` | Key type — userId (post-auth), IP (pre-auth) |

### State
| Field | Type | Description |
|---|---|---|
| `userRequests` | `HashMap<String, Deque<Long>>` | userId → sorted queue of request timestamps |

### Event
- `newRequest(userId)` — triggered on every incoming API call

### Transitions
1. **No queue for userId** → initialise empty `Deque`, add `now`, return `true`
2. **While `queue.peekFirst() <= now − windowSize`** → `queue.pollFirst()` (prune stale from head)
3. **`queue.size() >= threshold`** → return `false` (block)
4. **else** → `queue.addLast(now)`, return `true` (allow)

> Key insight: queue length IS the counter. No separate counter field needed.

---

## 2. HLD

### NFR
| Dimension | Target |
|---|---|
| Scale | 8M req/day · peak 1000 TPS (10x average of ~92 TPS) |
| Latency | < 100ms |
| MTTD | < 1 min via health checks + alerting |
| MTTR | < 15 min via Active-Passive + Redis Sentinel |
| Failover | Fail open — rate limiter outage must not block downstream |
| Cross-region | Async replication, slight over-counting acceptable during lag |

### Components
```
Client
  → API Gateway          (Layer 1: IP rate limit + Auth service — protects /login from DDOS)
  → Load Balancer
  → Rate Limiter Service (Layer 2: userId rate limit — stateless pods, scale freely)
  → Redis ZSET           (single source of truth — Lua script atomic ops)
  → Redis Replica        (passive, promoted via Sentinel on failure)
  → Downstream Service
```

### Data Storage
- **Store:** Redis Sorted Set (ZSET)
- **Key:** `ratelimit:{userId}`
- **Score:** request timestamp (enables range queries)
- **Operations:**
  - `ZADD` — add timestamp O(log n)
  - `ZREMRANGEBYSCORE` — prune stale timestamps in one command
  - `ZCARD` — get current count O(1)

**Atomicity — Redis Lua script:**
```lua
local key = KEYS[1]
local now = ARGV[1]
local windowSize = ARGV[2]
local threshold = ARGV[3]

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - windowSize)
local count = redis.call('ZCARD', key)
if count >= tonumber(threshold) then
    return 0
end
redis.call('ZADD', key, now, now)
redis.call('EXPIRE', key, windowSize)
return 1
```

> Rule: stateless compute + atomic data layer. Never solve atomicity at the infrastructure layer.

### End-to-End Flow

#### ✅ Path 1 — Happy path (allow)
```
Client → Gateway (L1 IP check) → Auth → LB → Rate Limiter
  → Redis Lua script → returns 1 → forward to Downstream → 200 OK
```

#### ❌ Path 2 — Rate limit exceeded (block)
```
Client → Gateway → Auth → LB → Rate Limiter
  → Redis Lua script → returns 0 → 429 Too Many Requests + Retry-After
  (request never reaches downstream)
```

#### ⚠️ Path 3 — Redis down (infrastructure failure)
```
Client → Gateway → Auth → LB → Rate Limiter
  → Redis unreachable → catch exception → fail open → allow request
  → forward to Downstream → fire alert → ops recovers within MTTR
```

**Code structure:**
```java
try {
    boolean allowed = allowRequest(userId);
    if (allowed) → forward to downstream     // Path 1
    else         → return 429                // Path 2
} catch (RedisUnavailableException e) {
    → fail open, allow request               // Path 3
    → fire alert
}
```

---

## 3. LLD — Delta from Fixed Window

### Class delta
```
HashMap<String, UserRequest>   →   HashMap<String, Deque<Long>>
UserRequest inner class        →   dropped entirely
counter field                  →   dropped — queue.size() is the count
```

### Algorithm (local version)
```java
public boolean allowRequest(String userId) {
    long now = System.currentTimeMillis();
    Deque<Long> queue = userRequests.get(userId);

    if (queue == null) {
        queue = new ArrayDeque<>();
        userRequests.put(userId, queue);
    }

    // prune stale timestamps from head
    while (!queue.isEmpty() && now - queue.peekFirst() >= windowSize)
        queue.pollFirst();

    if (queue.size() >= allowedRequestThreshold)
        return false;

    queue.addLast(now);
    return true;
}
```

> In production: this entire block becomes the Redis Lua script. Same logic, atomic execution, distributed.

### Log vs Counter trade-off
| Approach | Memory | Accuracy | Use when |
|---|---|---|---|
| Log-based | O(requests) | Exact | Low-volume, accuracy critical |
| Counter-based | O(1) | Approximate (~boundary error) | High-volume, slight inaccuracy acceptable |

---

## 4. API Design — Delta from Fixed Window

### Endpoint (unchanged)
```
POST /ratelimit/{entityId}
```

### Request body (unchanged)
```json
{ "endpoint": "/api/v1/payments" }
```

### Sliding window delta — 200 response
```json
{
  "allowed": true,
  "remainingRequests": 7,
  "windowResetAt": null
}
```
> `windowResetAt` is `null` — sliding window has no fixed reset time. Window moves continuously.

### Retry-After on 429
```
Retry-After = oldest_timestamp_in_queue + windowSize - now
```
> Approximate — based on when the oldest logged request will expire.

### All other responses unchanged from fixed window
- 429: `{ "allowed": false, "retryAfter": N }`
- 500: `{ "allowed": true, "reason": "rate_limiter_unavailable" }`
- Headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`

---

## 5. Production

### Failures
| Failure | Behaviour |
|---|---|
| Redis down | Fail open — allow all requests, fire alert immediately |
| Replica lag | Slight over-counting across regions — acceptable trade-off |
| DDOS / rogue actor | Blocked at Layer 1 (IP rate limit) at API Gateway |
| Queue memory bloat | Alert on ZCARD spike — log-based stores every timestamp |

> Queue bloat is unique to sliding window — fixed window doesn't have this. Monitor ZCARD per userId.

### Observability
**Splunk (logs):**
- Every 429 with userId + endpoint — audit trail, abuse investigation
- Fail open triggered — critical, ops must know immediately
- ZCARD per request — high values = user hitting boundary constantly

**AWS CloudWatch (metrics):**
- 429 rate per userId + globally
- Redis ZREMRANGEBYSCORE latency (p50, p99) — prune cost grows with queue size
- Queue size (ZCARD) per userId — memory bloat signal
- RPS vs threshold — capacity planning
- Redis connection error rate — early warning

**Scaling:**
- Rate Limiter pods — Auto Scaling on CPU/RPS. Stateless, trivial to scale.
- Redis — Redis Cluster sharded by userId. Not vertical — Redis is single-threaded.

---

## Score (L5 EM bar)

| Layer | Score | Key gap |
|---|---|---|
| Domain | 8/10 | Timestamp comparison direction needed correction |
| HLD | 8/10 | Redis ZSET and Lua script understood well; peak TPS needed prompting |
| LLD | 9/10 | Correct EM call to skip and state only the delta |
| API | 8/10 | Delta identified correctly; Retry-After approximation needed prompting |
| Production | 7/10 | Tooling named but content needed prompting; queue bloat not volunteered |
| **Overall** | **7.8/10** | Improving — production thinking still reactive |

### Trend vs fixed window (7.2 → 7.8)
Feedback absorbed faster each round. Production instinct improving but not yet automatic.

> Before saying done: *What breaks? How do I know? How long to recover?*
