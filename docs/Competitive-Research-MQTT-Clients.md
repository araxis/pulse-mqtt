# Competitive Research — .NET MQTT Clients (for Pulse)

> Method: `deep-research` workflow — 5 search angles → 24 primary sources fetched → 113 claims extracted → 25 adversarially verified (3-vote, need 2/3 to refute) → 5 confirmed, 20 killed → synthesized.
> Verification date: **2026-06-08**. Anchor artifact: MQTTnet **`5.1.0.1559`** (2026-02-04).
> ⚠️ Read the confidence labels. Only Section 1 reached high-confidence verification. Sections 2–5 are **leads**, not facts.

---

## 1. Confirmed findings (HIGH confidence)

All four rest on official primary sources (dotnet/MQTTnet wiki, GitHub releases, NuGet metadata), unanimous or 2-0 votes.

### 1.1 MQTTnet v5 removed `ManagedMqttClient` — and won't replace it soon ★ the opening
The Upgrading guide states verbatim: *"The extension is not available. It is recommended to use the regular client and doing the reconnect stuff etc. via your own code,"* and a v5 managed client is *"not yet planned but may … happen in the future."* The last version shipping `MQTTnet.Extensions.ManagedClient` was `4.3.7.1207`; it is absent from the entire 5.x branch.

- **So:** the market leader now has **no first-class auto-reconnect, offline queue, or auto-resubscribe.** A built-in, well-designed resilient layer (backoff reconnect, bounded outbound queue/backpressure, automatic re-subscription, connection-lifecycle callbacks) is exactly the gap.
- **Nuance (from the verifier):** a third-party `MQTTnet.Rx.Client` exposes a "Resilient client," but the *official* library ships nothing.
- **Plan impact:** Phase 4 promoted to flagship. See [Phase-04-Resilience-Detailed-Design.md](Phase-04-Resilience-Detailed-Design.md).
- Sources: [Upgrading guide](https://github.com/dotnet/MQTTnet/wiki/Upgrading-guide), [discussion #2142](https://github.com/dotnet/MQTTnet/discussions/2142), [discussion #2230](https://github.com/dotnet/MQTTnet/discussions/2230). Vote 2-0.

### 1.2 MQTTnet v5.1.0 marked all projects AOT-compatible
Changelog bullet *"Marked all projects as AOT compatible"* under v5.1.0, confirmed on NuGet for `5.1.0.1559`.

- **So:** AOT/trimming is **table-stakes, not a differentiator.** Verifier caveat: *"marked compatible" (e.g. `IsAotCompatible`) is not a guarantee of zero trimming warnings on every path.* Our edge is shipping **proven zero-warning** AOT (source-generated codecs, no reflection serialization).
- **Plan impact:** AOT demoted from headline differentiator (§A.1.2) to a hard quality gate (Part F).
- Sources: [releases](https://github.com/dotnet/MQTTnet/releases), [NuGet](https://www.nuget.org/packages/MQTTnet). Vote 3-0.

### 1.3 MQTTnet v5 split `MqttFactory` → `MqttClientFactory` / `MqttServerFactory`
Breaking change in the 4.x→5.0 migration; v5 samples use `new MqttClientFactory()`; devs hit the rename in issues [#2073](https://github.com/dotnet/MQTTnet/issues/2073), [#2085](https://github.com/dotnet/MQTTnet/issues/2085).

- **So:** the entry point/construction story is in flux → **DI-first construction (`AddMqttClient`, named clients) is a low-risk, validated differentiator** (our Phase 7).
- Source: [Upgrading guide](https://github.com/dotnet/MQTTnet/wiki/Upgrading-guide). Vote 3-0 (merged).

### 1.4 MQTTnet v5 dropped all pre-.NET 8 targets
Upgrading guide: *"MQTTnet 5 only supports newer (and still supported) .NET versions starting with version 8.0"* — explicitly to enable `Span<>`/`Memory<>`. NuGet `5.1.0.1559` targets only `net8.0` and `net10.0`, no dependencies. Legacy users are steered to MQTTnet 4 (hotfix-only).

- **So:** we **drop the netstandard idea** and target `net8.0` + `net10.0`; lean fully on Pipelines + Span with no compat shims.
- Sources: [Upgrading guide](https://github.com/dotnet/MQTTnet/wiki/Upgrading-guide), [NuGet](https://www.nuget.org/packages/MQTTnet), [issue #2084](https://github.com/dotnet/MQTTnet/issues/2084). Vote 2-0.

---

## 2. Corrections this forces in the plan
| # | Was assumed | Verified reality | Action |
|---|---|---|---|
| 1 | Resilience "bolted on" via `ManagedMqttClient` | v5 **removed** it; nothing planned | Promote Phase 4 to flagship (§A.0, §A.1.1) |
| 2 | Native AOT is a differentiator | MQTTnet v5.1 already AOT-marked | Reframe as parity + prove zero-warning (§A.1.2, Part F) |
| 3 | Possibly multi-target down to netstandard2.1 | MQTTnet dropped all pre-net8 | Target `net8.0`+`net10.0` only (§B.4) |
| 4 | (date) v5.1.0 "Feb 2025" | Artifact is **2026-02-04** | Cite 2026, re-check before design lock |

---

## 3. Leads — sourced but NOT adversarially verified (do not treat as fact)

These URLs are real primary sources that were fetched, but the *specific design claims* abstained in voting (0-0). Use as inspiration; confirm before committing.

**.NET competitor field** *(unverified)*
- [`hivemq/hivemq-mqtt-client-dotnet`](https://github.com/hivemq/hivemq-mqtt-client-dotnet) — appears to be an official HiveMQ **.NET** client (client-only). If real and maintained, it is a *direct* competitor — verify maintenance, API, AOT status.
- [Eclipse Paho .NET / M2Mqtt](https://eclipse.dev/paho/clients/dotnet/) — historically targets legacy runtimes (.NET Framework / Compact / Micro). Likely not modern-.NET competition; confirm.
- [`Azure/azure-iot-sdk-csharp`](https://github.com/Azure/azure-iot-sdk-csharp) — wraps MQTT internally for Azure IoT, not a general-purpose client. AWS reportedly has **no** native .NET IoT SDK. Confirm.

**Cross-language design patterns worth borrowing** *(unverified — but strong leads)*
- **Go `autopaho`** ([source](https://github.com/eclipse-paho/paho.golang/blob/master/autopaho/auto.go)) — automatic reconnect + `OnConnectionUp`/`OnConnectionDown` lifecycle callbacks that hand back a connection manager so the app re-subscribes on every reconnect. → directly shapes our `IConnectionLifecycle`.
- **Rust `rumqttc`** ([README](https://github.com/bytebeamio/rumqtt/blob/main/rumqttc/README.md)) — explicit `EventLoop` the app polls; **bounded request queue** with explicit channel capacity (`AsyncClient::new(opts, 10)`) giving natural backpressure under bad networks. → shapes our bounded-`Channel<T>` offline queue + explicit-capacity option.
- **Java HiveMQ client** ([source](https://github.com/hivemq/hivemq-mqtt-client)) — three interchangeable API flavours (blocking / async / reactive) and **Reactive Streams backpressure** for inbound QoS 1/2. → validates "backpressure as a policy" and our optional Rx adapter.
- **Python `aiomqtt`** ([source](https://github.com/empicano/aiomqtt)) — async-context + streaming consumption ergonomics.
- Third-party **`MQTTnet.AspNetCore.Routing`** ([source](https://github.com/IoTSharp/MQTTnet.AspNetCore.Routing)) — ASP.NET-style attribute routing (`[MqttRoute("{zipCode:int}/temperature")]`) for inbound dispatch, which core MQTTnet lacks. → confirms demand for our native `ITopicRouter`; the *first-class, in-core* version is still our differentiator.

**MQTT-over-QUIC** *(unverified — zero confirmed claims)*
- Sources fetched: [EMQX MQTT-over-QUIC](https://docs.emqx.com/en/emqx/latest/mqtt-over-quic/introduction.html), [NanoMQ QUIC](https://nanomq.io/docs/en/latest/quic/quic-doc.html), [emqx/NanoSDK](https://github.com/emqx/NanoSDK), [.NET `System.Net.Quic`](https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/quic/quic-overview). Maturity/standardization **not** established here.
- **Decision held:** QUIC stays **P1/opt-in** until verified — do not make it a v1 core promise.

**Modern .NET wire-protocol idioms** *(no claims survived verification)*
- Sources: [System.IO.Pipelines](https://devblogs.microsoft.com/dotnet/system-io-pipelines-high-performance-io-in-net/), [OTel messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/) & [metrics](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/), [Native AOT in .NET 10](https://code.soundaranbu.com/state-of-nativeaot-net10).
- These underpin our "next-gen" claim but weren't independently confirmed — treat the OTel `messaging.*` conventions and Pipelines approach as **best-practice defaults to validate by building + benchmarking**, not settled fact.

---

## 4. Open questions for a follow-up verification pass
1. **HiveMQtt (.NET):** is `hivemq-mqtt-client-dotnet` actively maintained, client-only, AOT-clean? It may be the closest competitor — build/differentiate decision depends on it.
2. **QUIC maturity:** broker support breadth (EMQX/NanoMQ + others), standardization status, and .NET client viability in 2026 — is it a credible differentiator yet?
3. **Resilience API shape:** which inbound/outbound model wins — rumqtt-style bounded channel, autopaho-style lifecycle callbacks, or HiveMQ-style reactive? (Our Phase 4 design picks **callbacks + bounded channel**, with Rx as an optional adapter — to be validated against these primaries.)
4. **OTel for MQTT:** confirm the exact `messaging.*` attributes/metrics that apply to MQTT pub/sub before freezing the instrumentation surface (Phase 8).

---

## 5. One-line takeaway
The leading library just **vacated the resilience space** and made AOT table-stakes. So Pulse's credible "next-generation" claim rests on **(a) built-in, swappable resilience**, **(b) first-class routing/typed messaging**, **(c) DI/observability by default**, and **(d) proven-clean AOT performance** — in that priority order.
