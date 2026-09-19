# Examples

Each directory is an independent Cargo crate in this repository. It depends on
CARBON through `path = "../.."`, and has its own workspace and lockfile. HTTP and
runtime dependencies therefore stay out of CARBON's development dependencies.
Commands below run from the repository root.

| Crate | Demonstrates |
| --- | --- |
| [memory](memory/src/main.rs) | Ordered reads and writes, and progress during consumer backpressure |
| [tcp_to_file](tcp_to_file/src/main.rs) | TCP upload to file segments, then TCP download |
| [http_upload](http_upload/src/main.rs) | Reqwest streaming PUTs through a bounded body channel |
| [pingora_upload](pingora_upload/src/main.rs) | Pingora streaming PUTs with a shared connection pool and no application channel |

```sh
cargo run --locked --manifest-path examples/memory/Cargo.toml
cargo run --locked --manifest-path examples/tcp_to_file/Cargo.toml
cargo run --locked --manifest-path examples/http_upload/Cargo.toml -- https://httpbin.org/put
cargo run --locked --manifest-path examples/pingora_upload/Cargo.toml -- https://httpbin.org/put
```

The HTTP examples require your own endpoint accepting streaming PUT requests.
The Reqwest example sends `hello world\n` once to each URL. The Pingora example
sends it twice, using one active upload so that connection reuse is visible in
the output. Multiple URLs can be passed to either example. Both disable CARBON
retries; dropping a writer does not undo remote writes.

## Pingora upload

The Pingora example uses HTTP/1.1 chunked transfer encoding, optionally over
TLS with Rustls and the platform certificate store. Certificate and hostname
verification remain enabled. DNS resolution uses the first returned address;
the example does not implement address failover, redirects, or authentication.
Its endpoint must consume the request body before returning its final response.
Opening, each frame write, and finalization each have a 30-second timeout.

`poll_write` creates one future owning the HTTP session and a cloned `Bytes`
handle. After `Pending`, it polls that same future. No borrowed frame reference
is kept, and cloning `Bytes` shares its storage. Finalization ends the body,
reads the final response, drains its body, and returns the session to Pingora,
which checks whether its connection can be reused. An error or cancellation
before that point drops the session without returning it to the pool.

There is no application channel or task per frame. Pingora, TLS, and the network
stack still have their own buffering and runtime work. `Ready(Ok(()))` from
`poll_write` does not acknowledge remote persistence; the HTTP result arrives
from `poll_finalize`.

Pingora's Rustls integration is experimental upstream. Its dependencies include
native compression libraries and may require a C/C++ toolchain, CMake, and
pkg-config. They are isolated to this example.

## Building and testing separately

The root `cargo test`, `cargo fmt`, and `cargo clippy` commands do not traverse
these independent workspaces. Select an example explicitly when needed:

```sh
cargo check --locked --manifest-path examples/pingora_upload/Cargo.toml
cargo test --locked --manifest-path examples/tcp_to_file/Cargo.toml
cargo fmt --manifest-path examples/memory/Cargo.toml -- --check
```
