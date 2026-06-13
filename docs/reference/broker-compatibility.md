# Broker compatibility

Pulse.Mqtt is verified against three brokers on every change through a shared conformance suite.
Each broker runs the *same* scenarios — defined once in `BrokerScenarios` — so a failure names
the broker and the capability that regressed rather than a one-off test.

## Tested brokers

| Broker | Version tested | Anonymous access | How it runs |
| --- | --- | --- | --- |
| [Eclipse Mosquitto](https://mosquitto.org/) | 2.x (`eclipse-mosquitto:2`) | `mosquitto-no-auth.conf` | Every PR and every merge |
| [EMQX](https://www.emqx.io/) | 5.8 (`emqx/emqx:5.8`) | Default listener | PRs that touch source, merges to `main`, on demand |
| [HiveMQ CE](https://www.hivemq.com/developers/community/) | 2024.3 (`hivemq/hivemq-ce:2024.3`) | Default listener | PRs that touch source, merges to `main`, on demand |

The brokers run as [Testcontainers](https://testcontainers.com/) images, so the suite needs only
a working Docker daemon — no broker installs, no shared test environment.

## Verified scenarios

Each broker passes all of the following:

| Scenario | What it proves |
| --- | --- |
| Handshake | CONNECT/CONNACK succeeds and a clean DISCONNECT is accepted. |
| QoS 0 round trip | Fire-and-forget delivery. |
| QoS 1 round trip | At-least-once delivery with PUBACK. |
| QoS 2 round trip | Exactly-once delivery through the full PUBREC/PUBREL/PUBCOMP exchange. |
| Retained message | A retained publish reaches a subscriber that connects *after* it. |
| Shared subscription | `$share/<group>/<topic>` delivers each message to exactly one group member. |
| Large payload | A 64 KB payload round-trips intact. |
| Persistent session resume | Reconnecting with `CleanStart = false` and a non-zero session expiry resumes the session (`SessionPresent = true`) and still routes its earlier subscription. |

## How the matrix is gated in CI

Running the heavier EMQX and HiveMQ images on every push would slow the inner loop, so the matrix
is split:

- **`ci.yml`** runs on every PR and push with `--filter "Category!=BrokerMatrix"`. Mosquitto and
  all other tests run; the EMQX/HiveMQ classes are skipped, keeping the fast lane quick.
- **`broker-matrix.yml`** runs `--filter "Category=BrokerMatrix"` (EMQX and HiveMQ) on PRs that
  touch the source, the integration tests, or the shared build inputs — so a cross-broker
  regression is caught before merge — and again on `main` and via manual dispatch. A matrix
  failure on `main` opens (or updates) a tracking issue, since it can't block the merge that
  caused it.

The EMQX and HiveMQ test classes carry `[Trait("Category", "BrokerMatrix")]`; Mosquitto does not,
so it always runs.

## Running the matrix locally

```bash
# Mosquitto only (the fast inner loop)
dotnet test tests/Pulse.Mqtt.IntegrationTests --filter "Category!=BrokerMatrix"

# EMQX and HiveMQ (pulls the broker images on first run)
dotnet test tests/Pulse.Mqtt.IntegrationTests --filter "Category=BrokerMatrix"
```
