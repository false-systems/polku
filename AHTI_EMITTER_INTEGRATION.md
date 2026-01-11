# AHTI Emitter Integration - Cyclic Dependency Issue

## Problem

`ahti-emitter` in `false-systems-plugins` depends on `polku-gateway` for:
- `Emitter` trait
- `Event` proto type
- `PluginError`

When we try to add `ahti-emitter` as an optional dependency to `polku-gateway`, we get a cyclic dependency:

```
polku-gateway -> ahti-emitter -> polku-gateway
```

## Solution Options

### Option 1: Extract `polku-core` crate (Recommended)

Create a new crate `polku-core` containing:
- `Emitter` trait
- `Event` proto/struct
- `PluginError`
- Common types

Then:
- `polku-gateway` depends on `polku-core`
- `ahti-emitter` depends on `polku-core` (not `polku-gateway`)
- `polku-gateway` can optionally depend on `ahti-emitter`

```
polku/
├── core/           # polku-core: traits, protos, errors
├── gateway/        # polku-gateway: server, hub, buffer
└── ...

false-systems-plugins/
└── emitters/
    └── ahti-emitter/  # depends on polku-core only
```

### Option 2: Inline ahti-emitter in gateway (Temporary)

Copy the ahti-emitter code into `polku-gateway/src/emit/ahti.rs` behind a feature flag. This duplicates code but avoids the cycle.

### Option 3: Use the proto directly

Have `ahti-emitter` generate its own proto types instead of importing from `polku-gateway`. This duplicates proto definitions but breaks the dependency.

## Recommended Fix

Go with **Option 1** - extract `polku-core`. This is the cleanest solution and follows the pattern used by other Rust ecosystem projects (e.g., `tokio-core` → `tokio`).

## Files to Create

```
polku/core/Cargo.toml
polku/core/src/lib.rs
polku/core/src/emit.rs      # Emitter trait
polku/core/src/error.rs     # PluginError
polku/core/src/proto.rs     # Event, re-export from generated
polku/core/build.rs         # Proto generation
```

## Migration Steps

1. Create `polku-core` crate with traits and protos
2. Update `polku-gateway` to depend on and re-export from `polku-core`
3. Update `ahti-emitter` to depend on `polku-core` instead of `polku-gateway`
4. Add `ahti-emitter` as optional dep to `polku-gateway`
5. Wire up in `main.rs` behind `--features ahti`

## Temporary Workaround

For now, inline the ahti-emitter code into `polku-gateway/src/emit/ahti.rs`.
