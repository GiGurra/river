# Resource Leak Analysis

Analysis performed on commit `c449b62` (master) on 2026-02-13, investigating
potential goroutine/resource leaks that could cause increased CPU usage over
time in production deployments.

## Context

In a 4-instance production deployment, services using River exhibited sudden
(not gradual) CPU usage increases, even in idle state with low to no jobs. The
issue resolves after pod restart. This analysis investigates whether River
itself could contain resource leaks contributing to this behavior.

## Confirmed Bugs

### 1. Connection Pool Leak in Listener.Connect()

**File:** `riverdriver/riverpgxv5/river_pgx_v5_driver.go:1067-1070`
**Severity:** Medium (edge case, but real resource leak)
**Branch:** `fix/listener-connect-pool-leak`

When `afterConnectExec` fails (used for setting custom schema search paths),
the acquired pool connection is never released back to the pool:

```go
poolConn, err := l.dbPool.Acquire(ctx)  // line 1062
if err != nil {
    return err
}

if l.afterConnectExec != "" {
    if _, err := poolConn.Exec(ctx, l.afterConnectExec); err != nil {
        return err  // BUG: poolConn never released!
    }
}
```

Compare with the correct error handling 10 lines later:

```go
if err := poolConn.QueryRow(ctx, ...).Scan(&schema); err != nil {
    poolConn.Release()  // Correctly released
    return err
}
```

**Impact:** On repeated reconnection failures where `afterConnectExec` fails,
connections accumulate in the pool without being returned. This depletes the
connection pool and can eventually cause connection acquisition to hang or fail.

**Fix:** Add `poolConn.Release()` before returning the error.

### 2. Missing ticker.Stop() in pollForSettingChanges

**File:** `producer.go:859`
**Severity:** Low (only in poll-only mode, minor resource leak)
**Branch:** `fix/poll-settings-ticker-leak`

The `pollForSettingChanges` function creates a ticker but never stops it:

```go
func (p *producer) pollForSettingChanges(ctx context.Context, ...) {
    defer wg.Done()

    ticker := time.NewTicker(p.config.QueuePollInterval)
    // Missing: defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return  // ticker leaked
        case <-ticker.C:
            // ...
        }
    }
}
```

Other functions in the same file follow the correct pattern:

```go
// heartbeatLogLoop (line 772-773)
ticker := time.NewTicker(5 * time.Second)
defer ticker.Stop()
```

**Impact:** A leaked Go ticker continues to fire internally even after the
goroutine that reads from it has exited. This is a minor resource leak. Note
that `pollForSettingChanges` only runs when there is **no notifier configured**
(poll-only mode), so standard pgx setups with LISTEN/NOTIFY are not affected.

**Fix:** Add `defer ticker.Stop()` after creating the ticker.

### 3. Notifier waitOnce Pings with Cancelled Context

**File:** `internal/notifier/notifier.go:396-403`
**Severity:** Low (design quirk, health check is effectively a no-op)
**Branch:** `fix/notifier-ping-cancelled-ctx`

In `waitOnce`, after the 5-second `needPingCtx` timeout fires:

```go
case <-needPingCtx.Done():
    if err := drainErrChan(); err != nil {  // calls cancel(), cancelling ctx
        return err
    }
    // Ping the conn to see if it's still alive
    if err := n.listener.Ping(ctx); err != nil {  // ctx is already cancelled!
        return err
    }
```

The `drainErrChan()` function calls `cancel()` at line 370, which cancels the
inner `ctx` created at line 348. Then `Ping(ctx)` uses that cancelled context,
so it always fails immediately with `context.Canceled`. Back in
`listenAndWait`, `context.Canceled` triggers a `continue`, restarting the wait
loop.

**Impact:** The health-check Ping never actually checks connection health. It
always fails and restarts the loop. This means stale/broken connections are not
detected until a real notification wait fails. The 5-second cycle is harmless
from a CPU perspective but defeats the purpose of the ping.

**Fix:** Save a reference to the parent context before creating the inner
cancellable context, and use the parent context for the Ping call.

## What Was Verified as NOT Leaking

The following components were thoroughly reviewed and found to be correctly
implemented:

- **Goroutines in notifier.waitOnce (line 353):** All three exit paths (ctx
  cancellation, ping timeout, errChan error) properly drain the goroutine via
  errChan before returning.
- **DebouncedChan (fetch limiter):** At most one goroutine at a time. Proper
  mutex protection prevents concurrent timer loops.
- **Service lifecycle (startstop package):** Clean start/stop with proper
  context cancellation propagation via `WithCancelCause`.
- **Leadership elector:** Proper timer cleanup on all exit paths. Exponential
  backoff on failures (1s to 64s).
- **Producer subroutines:** All tracked via WaitGroup, cancelled via
  `cancelSubroutines()` on shutdown, and waited on via `subroutineWG.Wait()`.
- **Notifier reconnection loop:** Exponential backoff prevents reconnection
  storms.
- **BatchCompleter:** 50ms ticker with proper `defer ticker.Stop()`. Idle-
  efficient (skips empty batches).
- **Subscription management:** Proper add/remove with mutex protection and
  `sync.Once` for unlisten.
- **All maintenance services:** Use `TickerWithInitialTick` which properly
  stops on context cancellation.
- **fetchPollLoop timer:** Properly stopped and drained on context
  cancellation.

## Assessment

None of the confirmed bugs are strong candidates for explaining **sudden CPU
spikes during idle** that resolve on restart. The connection pool leak is
gradual (and only triggers on `afterConnectExec` failures), the ticker leak is
minor, and the ping quirk is a no-op that cycles every 5 seconds.

If the CPU issue persists, the recommended investigation approach is:

1. **`goroutine` pprof profile:** Check total count and look for accumulation
   in specific stacks.
2. **`cpu` pprof profile:** 30-second profile during a spike to see where
   cycles are actually spent.
3. **`heap` pprof profile:** Check for memory leaks causing GC pressure.
4. Look specifically at stack traces in `notifier.waitOnce`,
   `producer.fetchAndRunLoop`, and `jobcompleter` code paths.
5. Check logs for leadership oscillation (`"Election change received"`
   messages).
