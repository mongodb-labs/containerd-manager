# Testing

Three test surfaces, gated so the default `cargo test` stays cheap.

| Suite | Feature flag | Speed | Touches your machine? |
|---|---|---|---|
| Unit tests | _(none)_ | seconds | no |
| Integration (against a running containerd) | `e2e-tests` | seconds | reads from existing colima |
| Colima profile lifecycle | `e2e-colima-profiles` | **minutes** | starts/stops real colima VMs |

The two e2e flags are independent and **should not be combined in the same
`cargo test` invocation** - the profile tests stop colima between scenarios,
which would break `e2e-tests`' preflight assumption that colima is up.

---

## Unit tests

```sh
cargo test
```

~140 tests. Runs in well under a second. No containerd needed - everything
that talks to gRPC is either purely in-process (cache, validators, label
parsers) or set up against a dummy lazy channel.

---

## Integration tests - `e2e-tests`

```sh
cargo test --features e2e-tests
```

Runs `tests/integration.rs` against a real containerd. Each test creates,
exercises, and tears down its own container under the `e2e-tests` namespace.

### Preconditions (asserted by a preflight in the test file)

* **Colima is running** with a containerd-style profile - i.e. some
  `~/.colima/<profile>/containerd.sock` exists and accepts a connection.

Docker Desktop **may run in parallel** - the library's auto-discovery skips
the `default` profile (which is what colima's docker shim uses) and prefers
any other profile. Useful if you want to test this project while also
working on a docker-based project.

Typical setup:

```sh
# Use (or create) a profile dedicated to containerd. The `--runtime` flag is
# only honoured at profile-creation time; if a profile already exists with a
# different runtime, colima silently ignores `--runtime` and prints a WARN.
colima start --profile containerd --runtime containerd

# (Quit Docker Desktop if it's running.)
cargo test --features e2e-tests
```

If you accidentally created the profile with the docker runtime, delete and
recreate it:

```sh
colima delete --profile containerd --force
colima start --profile containerd --runtime containerd
```

Verify after start:

```sh
colima status --profile containerd     # should say: runtime: containerd
ls ~/.colima/containerd/containerd.sock # should exist
```

If the preflight rejects your environment, it prints exactly what's wrong.

---

## Colima profile lifecycle tests - `e2e-colima-profiles`

```sh
cargo test --features e2e-colima-profiles -- --test-threads=1 --nocapture
```

Runs `tests/colima_profiles.rs`, which **starts and stops actual colima VMs**
to verify the auto-discovery path end-to-end. Each test:

1. Asserts no colima profiles are currently running (won't trample your setup).
2. `colima start --profile <unique-name>` with containerd runtime.
3. Calls `containerd_manager::connect(None)` and verifies it talks to the
   profile we just started.
4. `colima stop && colima delete` to clean up - even on panic.

### Why opt-in

* **Slow.** Each `colima start` is 30-60s.
* **Destructive.** Stops anything you had running. The preflight refuses to
  start if any profile is already up, but a crashed test could leave a
  half-built `cm-test-*` profile behind. Clean up with:

  ```sh
  colima list --json
  colima delete --profile <stale-name> --force
  ```

* **Slow tests in parallel would thrash the machine.** The file uses an
  internal mutex to serialize, but `--test-threads=1` is recommended so the
  runner doesn't spawn unrelated tasks during the long `colima start`.

### Preconditions

* `colima` CLI installed and on `PATH`.
* No colima profile currently running (asserted in the preflight).

---

## Adding a new e2e test

* Goes in `tests/integration.rs` if it just needs containerd up.
* Goes in `tests/colima_profiles.rs` if it needs a specific colima profile
  shape to be started.
* Wrap with `#![cfg(feature = "...")]` so it stays gated.
* Re-use the `cid("...")` and `cleanup()` helpers in `integration.rs`.
