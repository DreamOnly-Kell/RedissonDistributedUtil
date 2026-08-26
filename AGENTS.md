# Repository Guidelines

## Project Overview

A Java 17 **Spring Boot 3.4.5** utility library wrapping **Redisson 4.7.0** distributed locks into a developer-friendly API. Provides six usage modes (fully-automatic, fully-automatic + fallback, exception-free, exception-free + fallback, semi-automatic, fully-manual) with support for single-lock and multi-lock scenarios, watchdog auto-renewal, deadlock-safe key sorting, and fallback degradation.

- **Group/Artifact:** `org.myc.demo:RedissonDistributedUtil:1.0-SNAPSHOT`
- **License:** MIT

## Architecture & Data Flow

Single-module Maven project. All source lives under `org.myc.demo.distributed` (library), `org.myc.demo.config` (Spring config), and `org.myc.demo.example` (demo).

```
DemoApplication (Spring Boot entry)
├─► RedissonConfig                    ← manual RedissonClient Bean (reads application.yml)
├─► DistributedLockUtils              ← static entry point, holds volatile RedissonClient + config
│       ├─► RedissonLockConfig        ← default wait/lease times, key prefix, multi-lock sort toggle
│       ├─► doAcquire / tryAcquireInternal
│       │       └─► RLock / RedissonMultiLock   (Redisson native)
│       ├─► doAcquireOrFallback       ← auto mode: lock → callback, or fallback on failure
│       ├─► doAcquireOrFallbackExceptionFree ← exception-free mode: same, wrapped in LockResult
│       ├─► LockHandle                ← AutoCloseable lock handle (try-with-resources friendly)
│       ├─► LockResult<T>             ← exception-free result envelope (lock status + biz/fallback outcome)
│       ├─► LockAcquireResult         ← semi-auto acquire outcome (success carries handle)
│       ├─► LockStatus                ← enum: SUCCESS / FAILED / TIMEOUT / INTERRUPTED
│       ├─► LockAcquireException      ← thrown on acquire failure (with fallback exception as cause)
│       ├─► LockCallback<T>           ← @FunctionalInterface, no lock awareness
│       └─► LockAwareCallback<T>      ← @FunctionalInterface, receives LockHandle + keys
└─► SpringBootUsageExample           ← CommandLineRunner demo of all six modes
```

**Lock lifecycle:** key → `buildKey(prefix+key)` → `RedissonClient.getLock()` → `tryLock(wait, lease)` → `LockHandle` → business callback → `unlock()` (idempotent, guarded by `AtomicBoolean`).

**Fallback flow:** lock acquire fails → execute `fallback` lambda → wrap result/exception (auto: `LockAcquireException(cause)`, exception-free: `LockResult.fallbackSuccess`/`fallbackError`).

**Multi-lock:** keys sorted by default (`Collections.sort`) to prevent cross-deadlock; wrapped in `RedissonMultiLock` for atomic acquire.

## Key Directories

| Path | Purpose |
|---|---|
| `src/main/java/org/myc/demo/distributed/` | Core library — all lock utilities, config, DTOs, callbacks |
| `src/main/java/org/myc/demo/config/` | `RedissonConfig.java` — Spring-managed RedissonClient Bean |
| `src/main/java/org/myc/demo/` | `DemoApplication.java` — Spring Boot entry point |
| `src/main/java/org/myc/demo/example/` | `UsageExample.java` (standalone), `SpringBootUsageExample.java` (Spring Boot) |
| `src/main/resources/` | `application.yml` — Redisson + lock config |
| `.agents/skills/` | Agent skill definitions (not application code) |
| `.idea/` | IntelliJ IDEA project config |

## Development Commands

```bash
# Build (requires JDK 17+)
mvn clean compile

# Package
mvn clean package

# Run Spring Boot app (requires local Redis at 127.0.0.1:6379)
mvn spring-boot:run

# No test suite exists yet — src/test/ is absent
```

Maven repos configured: Aliyun mirror, Maven Central, JBoss Community, private Nexus (`8.210.23.20:9091`).

## Code Conventions & Common Patterns

### Naming & Structure
- **Package:** `org.myc.demo.distributed` — all production classes flat in one package (no sub-packages).
- **Classes:** PascalCase, suffix describes role (`*Utils`, `*Handle`, `*Result`, `*Config`, `*Exception`, `*Callback`).
- **Enums:** `LockStatus` — UPPER_SNAKE constants, each carries `boolean acquired` + `String desc` (Chinese).
- **Functional interfaces:** `@FunctionalInterface` on `LockCallback<T>` and `LockAwareCallback<T>`. Fallback reuses `LockCallback<T>` (same signature).

### API Design Patterns
- **Six-tier API:** fully-automatic → fully-automatic + fallback → exception-free → exception-free + fallback → semi-automatic → fully-manual. Each tier has single-lock and multi-lock variants, plus default-params and full-params overloads.
- **Fallback-aware methods:** `*AndFallback` suffix (e.g. `executeWithLockAndFallback`, `tryExecuteWithLockAndFallback`). Accept primary callback + optional fallback lambda (`LockCallback<T>`, nullable). Fallback reuses `LockCallback<T>` — no new interface.
- **Fluent config:** `RedissonLockConfig` setters return `this` for chaining.
- **Factory methods:** `LockResult.success()`, `LockResult.bizError()`, `LockResult.lockFail()`, `LockResult.fallbackSuccess()`, `LockResult.fallbackError()`, `LockAcquireResult.success()`, `LockAcquireResult.fail()`.
- **Idempotent unlock:** `LockHandle.unlock()` uses `AtomicBoolean released` CAS — safe to call multiple times or from `close()`.
- **Safe unlock helper:** `DistributedLockUtils.safeUnlock(RLock)` for manual mode — checks `isHeldByCurrentThread()` before unlock, swallows exceptions.

### Error Handling
- **Checked → RuntimeException wrapping:** business exceptions in callbacks wrapped with key context.
- **Lock acquire failure:** throws `LockAcquireException` (auto mode) or returns `LockAcquireResult` with status (semi-auto).
- **Fallback exception wrapping:** in auto mode, fallback exceptions wrapped as `LockAcquireException(status, key, cause)`; in exception-free mode, wrapped in `LockResult.fallbackError()`.
- **Exception-free mode:** `tryExecuteWithLock` / `tryExecuteWithLockAndFallback` catches everything, returns `LockResult` — never throws.
- **InterruptedException:** caught, thread re-interrupted (`Thread.currentThread().interrupt()`), returns `INTERRUPTED` status.

### Logging
- **SLF4J** (`LoggerFactory.getLogger`) — log prefix `[RedissonLock]`.
- **Slow-lock warning:** >1000ms triggers `WARN` log.
- **Fallback execution:** `INFO` log when fallback is triggered.
- **Verbose mode:** `RedissonLockConfig.verboseLog` enables `INFO`-level acquire/release logs.

### Thread Safety
- `CLIENT` and `CONFIG` are `volatile` static fields.
- `LockHandle.released` is `AtomicBoolean` — single-release guarantee.
- `LockHandle.keys` and `locks` wrapped with `Collections.unmodifiableList`.

### Initialization
- **Spring Boot (default):** `DemoApplication` registers `DistributedLockUtils` as Bean; `RedissonConfig` provides `RedissonClient` from `application.yml`.
- **Non-Spring:** `DistributedLockUtils.init(redissonClient)` — call once at startup.

## Important Files

| File | Role |
|---|---|
| `pom.xml` | Maven build — Spring Boot 3.4.5 parent, Redisson 4.7.0 |
| `src/main/resources/application.yml` | Redisson connection + lock config |
| `src/main/java/org/myc/demo/DemoApplication.java` | Spring Boot entry point, registers DistributedLockUtils Bean |
| `src/main/java/org/myc/demo/config/RedissonConfig.java` | Manual RedissonClient Bean (reads `spring.redis.redisson.*`) |
| `src/main/java/org/myc/demo/distributed/DistributedLockUtils.java` | **Main entry point** — all public lock APIs (~1050 lines) |
| `src/main/java/org/myc/demo/distributed/LockHandle.java` | Lock handle with AutoCloseable, TTL query, idempotent unlock |
| `src/main/java/org/myc/demo/distributed/LockResult.java` | Exception-free result DTO with fallback states (Serializable) |
| `src/main/java/org/myc/demo/distributed/RedissonLockConfig.java` | Global config with sensible defaults |
| `src/main/java/org/myc/demo/distributed/LockStatus.java` | Status enum (SUCCESS/FAILED/TIMEOUT/INTERRUPTED) |
| `src/main/java/org/myc/demo/distributed/LockAcquireException.java` | Runtime exception for acquire failures (carries fallback cause) |
| `src/main/java/org/myc/demo/distributed/LockCallback.java` | `@FunctionalInterface` — business callback (also used for fallback) |
| `src/main/java/org/myc/demo/distributed/LockAwareCallback.java` | `@FunctionalInterface` — callback with LockHandle access |
| `src/main/java/org/myc/demo/example/UsageExample.java` | Standalone demo (non-Spring) |
| `src/main/java/org/myc/demo/example/SpringBootUsageExample.java` | Spring Boot CommandLineRunner demo of all six modes |

## Runtime/Tooling Preferences

- **JDK:** 17 (source/target in pom.xml)
- **Build:** Maven (no Gradle wrapper)
- **IDE:** IntelliJ IDEA (`.idea/` committed)
- **Framework:** Spring Boot 3.4.5 (`spring-boot-starter-web`)
- **Runtime dependency:** Redis instance (default `redis://127.0.0.1:6379` in `application.yml`)
- **Redisson:** 4.7.0 core (no `redisson-spring-boot-starter` — 4.x has no published starter)
- **No test framework** configured — JUnit not in dependencies

## Testing & QA

**No test suite exists.** `src/test/` directory is absent; no test dependencies in `pom.xml`.

To add tests:
1. Add JUnit 5 + Mockito (or embedded-redis) to `pom.xml` `<dependencies>` with `<scope>test</scope>`.
2. Create `src/test/java/org/myc/demo/distributed/`.
3. Key areas to test: lock acquire/release lifecycle, idempotent unlock, multi-lock sorting, interrupt handling, watchdog vs fixed-lease modes, `LockResult` state transitions, fallback execution on lock failure, fallback exception wrapping, `LockResult.fallbackSuccess`/`fallbackError` states.
