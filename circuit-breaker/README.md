# Circuit Breaker — Mental Model

> **Problem:** Design a Circuit Breaker for protecting downstream dependencies.
> **Level:** Senior (L5) | **Role:** Engineering Manager

---

## 1. Domain

### Actor
- Microservices — upstream callers
- API Requests — triggering downstream calls

### Configuration
| Parameter | Description |
|---|---|
| `failureThreshold` | Number of failures before opening circuit |
| `resetTimeout` | Time before attempting probe from OPEN state |
| `halfOpenMaxProbes` | Max probe requests before deciding to close or reopen |

### State
| Field | Type | Description |
|---|---|---|
| `status` | `Status enum` | CLOSED · OPEN · HALF_OPEN |
| `failureCount` | `int` | Failures in current window |
| `lastFailureTime` | `long` | Timestamp of last failure |
| `halfOpenProbeInFlight` | `boolean` | Gate — only one probe at a time |

### Event
- `newRequest` — triggers `allowRequest()`
- `successfulResponse` — triggers `recordSuccess()`
- `failedResponse` — triggers `recordFailure()`

### State Machine
```
CLOSED → failures >= threshold → OPEN
OPEN → resetTimeout elapsed → HALF_OPEN (one probe)
HALF_OPEN → probe success → CLOSED
HALF_OPEN → probe fails → OPEN
```

### Transitions

**`allowRequest()`**
```
CLOSED    → true
OPEN      → if now - lastFailureTime >= resetTimeout:
              if probeInFlight → false
              else → HALF_OPEN, probeInFlight=true, true
            else → false
HALF_OPEN → true (probe already gated)
```

**`recordSuccess()`**
```
probeInFlight = false
failureCount = 0
if HALF_OPEN → CLOSED
```

**`recordFailure()`**
```
failureCount++
lastFailureTime = now
probeInFlight = false
if HALF_OPEN → OPEN
if failureCount >= threshold → OPEN
```

---

## 2. HLD

### NFR
| Dimension | Target |
|---|---|
| Scale | 10M req/day · peak ~1000 TPS |
| Latency | < 50ms (Redis sub-ms, network is bottleneck) |
| MTTD | < 1 min via health checks + alerting |
| MTTR | < 15 min via Active-Passive + Redis Sentinel |
| Failover | **Fail CLOSED** — Redis down = block all requests (opposite of rate limiter) |
| Cross-region | Async Redis replication · stale state acceptable |

> **Key distinction from Rate Limiter:** Circuit Breaker fails CLOSED on Redis down.
> Reason: can't risk flooding an already-failing downstream service.

### Components
```
Client
  → API Gateway              (L1: IP rate limit + Auth)
  → Load Balancer
  → Service + Sidecar CB     (circuit breaker runs as sidecar — no extra network hop)
  → Redis Hash               (shared state across all sidecars)
  → Redis Replica            (passive, Sentinel failover)
  → Downstream Service       (the dependency being protected)
```

**Why sidecar over shared service:**
- Latency — localhost call vs network hop
- Blast radius — sidecar failure affects one pod only
- No SPOF — shared CB service going down = all circuits fail simultaneously

**Why Redis for state:**
- Sidecar is stateless locally
- Redis Hash ensures consistent state across all pod sidecars
- Lua script for atomic state transitions

### Data Storage
```
HSET circuit:{serviceId} state CLOSED failureCount 0 lastFailureTime 0
HGETALL circuit:{serviceId}
```

One key per serviceId. One round trip reads all fields. No TTL — state is persistent and transition-driven (unlike rate limiter which uses key expiry).

**Atomicity — Redis Lua script:**
```lua
local key = KEYS[1]
local state = redis.call('HGET', key, 'state')
local failureCount = tonumber(redis.call('HGET', key, 'failureCount'))
local lastFailureTime = tonumber(redis.call('HGET', key, 'lastFailureTime'))
local now = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
local resetTimeout = tonumber(ARGV[3])

if state == 'CLOSED' then return 1 end
if state == 'OPEN' then
    if now - lastFailureTime >= resetTimeout then
        redis.call('HSET', key, 'state', 'HALF_OPEN')
        return 1
    end
    return 0
end
if state == 'HALF_OPEN' then return 1 end
return 0
```

### End-to-End Flow

#### ✅ Path 1 — CLOSED (happy path)
```
Gateway → Auth → LB → Service
  → Sidecar checks Redis → CLOSED
  → call Downstream → success
  → recordSuccess() → failureCount=0 → 200 OK
```

#### ❌ Path 2 — OPEN (fast-fail)
```
Gateway → Auth → LB → Service
  → Sidecar checks Redis → OPEN
  → fast-fail immediately → 503 + Retry-After
  (downstream never called — this is the value of circuit breaker)

Journey to OPEN:
  failures >= threshold → OPEN
  → resetTimeout elapsed → HALF_OPEN → probe
  → probe success → CLOSED
  → probe fails → OPEN again
```

#### ⚠️ Path 3 — Redis down
```
Gateway → Auth → LB → Service
  → Sidecar → Redis unreachable
  → default to OPEN → 503
  → fire alert → ops recovers within MTTR
```

---

## 3. LLD

### Class Structure

```java
public class CircuitBreaker {
    private int failureThreshold;
    private long lastFailureTime;
    private long resetTimeout;
    private int failureCount;
    private Status status;
    private boolean halfOpenProbeInFlight = false;

    CircuitBreaker(int failureThreshold, long resetTimeout) {
        this.failureThreshold = failureThreshold;
        this.resetTimeout = resetTimeout;
        this.failureCount = 0;
        this.status = Status.CLOSED;
    }

    enum Status { OPEN, CLOSED, HALF_OPEN }

    synchronized boolean allowRequest() {
        if (status == Status.OPEN &&
            System.currentTimeMillis() - lastFailureTime > resetTimeout) {
            if (halfOpenProbeInFlight) return false;
            status = Status.HALF_OPEN;
            halfOpenProbeInFlight = true;
            return true;
        }
        if (status == Status.CLOSED) return true;
        if (status == Status.OPEN) return false;
        return true; // HALF_OPEN
    }

    synchronized void recordSuccess() {
        failureCount = 0;
        halfOpenProbeInFlight = false;
        if (status == Status.HALF_OPEN)
            status = Status.CLOSED;
    }

    synchronized void recordFailure() {
        failureCount++;
        lastFailureTime = System.currentTimeMillis();
        halfOpenProbeInFlight = false;
        if (status == Status.HALF_OPEN)
            status = Status.OPEN;
        if (failureCount >= failureThreshold)
            status = Status.OPEN;
    }
}
```

### Thread Safety
- All three methods `synchronized` — same lock on `this`
- `halfOpenProbeInFlight` gates single probe — prevents two threads both transitioning OPEN → HALF_OPEN
- Released in both `recordSuccess()` and `recordFailure()`

> In production: all three methods become one Redis Lua script. Atomic at Redis server level. Sidecar is stateless.

### Delta from Rate Limiter
| Rate Limiter | Circuit Breaker |
|---|---|
| `HashMap<userId, UserRequest>` | Single state object per serviceId |
| `allowRequest(userId)` | `allowRequest()` + `recordSuccess()` + `recordFailure()` |
| Keyed by userId | Keyed by serviceId |
| Counter resets on window expiry | State transitions on failure/success events |

---

## 4. API Design

### Endpoints
```
POST /circuit/{serviceId}/request   → allowRequest()
POST /circuit/{serviceId}/success   → recordSuccess()
POST /circuit/{serviceId}/failure   → recordFailure()
GET  /circuit/{serviceId}/status    → current state
```

### Responses

**200 OK — CLOSED, allowed**
```json
{
  "allowed": true,
  "state": "CLOSED"
}
```

**503 Service Unavailable — OPEN, fast-fail**
```json
{
  "allowed": false,
  "state": "OPEN",
  "retryAfter": 30
}
```

> **503 vs 429:** Circuit breaker says "downstream unavailable." Rate limiter says "you're doing too much." Never confuse these.

**500 — Redis down, fail CLOSED**
```json
{
  "allowed": false,
  "reason": "circuit_breaker_unavailable"
}
```

---

## 5. Production ← Revision Focus

> **Pre-submission checklist for every design:**
> 1. What breaks?
> 2. How do I know it broke?
> 3. How long until fixed?
> 4. What does the system do while broken?
> 5. (Salesforce) How does this behave with 10,000 tenants simultaneously?

### Failures
| Failure | Behaviour |
|---|---|
| Redis down | Fail CLOSED — block all requests, fire alert immediately |
| Redis replica lag | Stale circuit state — may allow extra requests, acceptable |
| Sidecar crash | Pod restarts — state persisted in Redis, no data loss |
| Network partition | Sidecar can't reach Redis → fail CLOSED |
| Downstream slow (not failing) | Circuit stays CLOSED — consider timeout-based failures too |

> **Downstream slow note:** Circuit breaker only trips on failures. If downstream is slow but not erroring, add timeout-based failure recording — `recordFailure()` on timeout, not just on exception.

### Observability

**Metrics — AWS CloudWatch:**
- Circuit state transitions per serviceId (CLOSED→OPEN rate)
- 503 rate per serviceId
- Probe success/failure rate in HALF_OPEN
- Redis command latency (p50, p99)
- Redis connection error rate

**Logs — Splunk:**
- Every state transition with serviceId + timestamp
- Redis unavailable + fail CLOSED triggered
- Probe allowed + outcome (success/failure)

**Alerts:**
- Circuit OPEN > 5 mins → page on-call
- Redis unreachable → page immediately
- 503 spike > baseline → investigate downstream health

**Scaling:**
- Circuit Breaker sidecars scale with pods — no separate scaling needed
- Redis — single instance sufficient at 1000 TPS · scale via Redis Cluster if needed

### MTTD / MTTR
- MTTD < 1 min — CloudWatch alarm on state transition
- MTTR < 15 min — Redis Sentinel promotes replica, sidecars reconnect automatically

---

## Score (L5 EM bar)

| Layer | Score | Key gap |
|---|---|---|
| Domain | 9/10 | Clean state machine, probeInFlight added proactively |
| HLD | 8/10 | Sidecar reasoning sharp; Redis-down flip volunteered |
| LLD | 8/10 | Best first submission — gate logic correct |
| API | 8/10 | 503 vs 429 distinction strong |
| Production | 7/10 | Still reactive — use the 5-question checklist every time |
| **Overall** | **8.1/10** | Best round yet — trend 7.2 → 7.8 → 8.1 |

### Salesforce-specific lens
Always ask: *how does this behave with 10,000 orgs simultaneously?*
- Per-org circuit state → `circuit:{orgId}:{serviceId}` key pattern
- Noisy neighbour — one org's downstream failures must not trip circuit for others
- Tenant isolation is Salesforce's core architectural concern
