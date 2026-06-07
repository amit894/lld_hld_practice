# Fixed Window Rate Limiter — Mental Model

> **Problem:** Design a rate limiter using the Fixed Window algorithm.
> **Level:** Senior (L5) | **Role:** Engineering Manager

---

## 1. Domain

### Actor
- Clients — Devices, IPs, Users

### Configuration
| Parameter | Description |
|---|---|
| `windowSize` | Duration of each fixed window (ms) |
| `allowedRequestThreshold` | Max requests allowed per window |
| `scope` | Key type — userId (post-auth), IP (pre-auth) |

### State
| Field | Type | Description |
|---|---|---|
| `windowStartTime` | `long` | Timestamp when current window started |
| `requestCounter` | `int` | Number of requests in current window |

### Event
- `newRequest(userId)` — triggered on every incoming API call

### Transitions
1. **No window exists** → create new window `(now, counter=0)`
2. **`now − windowStartTime >= windowSize`** → reset window `(now, counter=0)`
3. **`counter >= threshold`** → block request → HTTP 429
4. **else** → allow request, `counter++`

---

## 2. HLD

### NFR
| Dimension | Target |
|---|---|
| Scale | 1M req/day, peak ~120 RPS (10x average) |
| Latency | < 50ms (Redis sub-ms, network is bottleneck) |
| MTTD | < 1 min via health checks + alerting |
| MTTR | < 15 min via Active-Passive + Redis Sentinel |
| Failover strategy | Fail open — rate limiter outage must not take down the API |
| Cross-region | Async replication, slight over-counting acceptable during lag |

### Components
```
Client
  → API Gateway          (Layer 1: IP-based rate limit — protects auth endpoints)
  → Auth Service         (JWT validation)
  → Load Balancer
  → Rate Limiter Service (Layer 2: userId-based rate limit — stateless nodes)
  → Redis                (single source of truth — INCR + Lua script)
  → Redis Replica        (passive, promoted via Sentinel on failure)
  → Downstream Service
```

**Two-layer design rationale:**
- Layer 1 (IP, pre-auth) — coarse, protects `/login` and `/token` from DDOS/brute force
- Layer 2 (userId, post-auth) — fine-grained, meaningful business-level limiting

### Data Storage
- **Store:** Redis
- **Key pattern:** `ratelimit:{userId}:{windowStart}`
- **Operation:** `INCR` (atomic, single-threaded at Redis server level)
- **Expiry:** `EXPIRE` = `windowSize` set at key creation
- **Atomicity:** Lua script wraps INCR + EXPIRE — Redis executes as single unit

```lua
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
```

### End-to-End Flow

#### ✅ Path 1 — Happy path (allow)
```
Client → Gateway (L1 IP check) → Auth → LB → Rate Limiter
  → Redis INCR → count ≤ threshold → forward to Downstream → 200 OK
```

#### ❌ Path 2 — Rate limit exceeded (block)
```
Client → Gateway → Auth → LB → Rate Limiter
  → Redis INCR → count > threshold → 429 Too Many Requests + Retry-After
  (request never reaches downstream)
```

#### ⚠️ Path 3 — Redis down (infrastructure failure)
```
Client → Gateway → Auth → LB → Rate Limiter
  → Redis unreachable → catch exception → fail open → allow request
  → forward to Downstream → fire alert → ops recovers within MTTR
```

**Code structure for paths:**
```java
try {
    boolean allowed = allowRequest(userId);  // Path 1 or 2
    if (allowed) → forward to downstream     // Path 1
    else         → return 429                // Path 2
} catch (RedisUnavailableException e) {
    → fail open, allow request               // Path 3
    → fire alert
}
```

---

## 3. LLD

### Class Structure

```java
public class FixedWindowRateLimiter {
    private long windowSize;
    private int allowedRequestThreshold;
    private HashMap<String, UserRequest> userRequests = new HashMap<>();

    public FixedWindowRateLimiter(long windowSize, int allowedRequestThreshold) { ... }

    public boolean allowRequest(String userId) {
        long now = System.currentTimeMillis();
        UserRequest u = userRequests.get(userId);

        if (u == null) {
            u = new UserRequest(now);
            userRequests.put(userId, u);
        }

        if (now - u.currentWindowStartTime >= windowSize) {
            u = new UserRequest(now);
            userRequests.put(userId, u);
        }

        if (u.requestCounter >= allowedRequestThreshold)
            return false;

        u.requestCounter++;
        return true;
    }

    private class UserRequest {
        long currentWindowStartTime;
        int requestCounter;

        UserRequest(long currentWindowStartTime) {
            this.currentWindowStartTime = currentWindowStartTime;
            this.requestCounter = 0;
        }
    }
}
```

### Thread Safety Ladder

| Level | Approach | Correctness | Performance | Distributed |
|---|---|---|---|---|
| 1 | `synchronized` method | ✅ | ❌ all users block | ❌ |
| 2 | Striped lock (per userId) | ✅ | ✅ | ❌ |
| 3 | `ConcurrentHashMap.compute()` | ✅ | ✅✅ | ❌ |
| 4 | Redis INCR + Lua script | ✅ | ✅✅ | ✅ |

**Rule:** Distributed lock is for multi-step operations crossing system boundaries. Redis INCR is atomic natively — no lock needed.

---

## 4. API Design

### Endpoint

```
POST /ratelimit/{entityId}
```

- `entityId` = authenticated `userId`, extracted from JWT by API Gateway before forwarding

### Request Body

```json
{
  "endpoint": "/api/v1/payments"
}
```

> Note: `timestamp` is NOT in the request — server uses `System.currentTimeMillis()`. Client timestamps are untrusted.

### Response Headers (all responses)

```
X-RateLimit-Limit: 10
X-RateLimit-Remaining: 3
X-RateLimit-Reset: 1686123456789
Retry-After: 30        ← 429 only
```

### Response Bodies

**200 OK — allowed**
```json
{
  "allowed": true,
  "remainingRequests": 7,
  "windowResetAt": 1686123456789
}
```

**429 Too Many Requests — blocked**
```json
{
  "allowed": false,
  "retryAfter": 30
}
```

**500 Internal Server Error — Redis down, fail open**
```json
{
  "allowed": true,
  "reason": "rate_limiter_unavailable"
}
```

---

## 5. Production

### Failures

| Failure | Behaviour |
|---|---|
| Redis down | Fail open — allow all requests, fire alert |
| Redis replica lag | Slight over-counting across regions — acceptable trade-off |
| DDOS / rogue actor | Blocked at Layer 1 (IP rate limit) at API Gateway |
| Pod/node failure | LB reroutes, Redis Sentinel promotes replica |

### Observability

**Metrics to track:**
- 429 rate per userId and globally
- Redis command latency (p50, p99)
- Requests per second (RPS) vs threshold
- Redis connection error rate

**Alerts:**
- Redis unreachable → page ops immediately
- 429 rate spike > baseline → investigate abuse
- MTTD target: < 1 min
- MTTR target: < 15 min

---

## Score (L5 EM bar)

| Layer | Score | Key gap |
|---|---|---|
| Domain | 8/10 | Constraints (per-endpoint limits, tenant quotas) not volunteered |
| HLD | 7/10 | Active-Passive confusion, thin components initially |
| LLD | 8/10 | NullPointerException caught after one prompt, PascalCase slow |
| API | 7/10 | Response body and headers needed prompting |
| Production | 6/10 | Fail open/closed needed coaching, observability not volunteered |
| **Overall** | **7.2/10** | Production thinking needs to be instinctive |

### To reach 8.5+
Before saying "done" on any design, ask yourself:
> *What breaks? How do I know it broke? How long until it's fixed?*
