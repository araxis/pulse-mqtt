---
layout: home

hero:
  name: Pulse.Mqtt
  text: The MQTT client .NET deserves
  tagline: Resilient by default, fast by design, swappable everywhere. MQTT 5.0 and 3.1.1 for net8.0 and net10.0.
  actions:
    - theme: brand
      text: Get started
      link: /guide/getting-started
    - theme: alt
      text: Why Pulse?
      link: /guide/introduction
    - theme: alt
      text: Benchmarks
      link: /Benchmark-vs-MQTTnet

features:
  - title: Resilient by default
    details: Background connect, exponential-backoff reconnect, automatic re-subscription, a bounded offline queue, and sticky faults on terminal failures. Survive broker restarts without writing a single retry loop.
  - title: Swappable everywhere
    details: Reconnect policy, retry classification, session store, offline store, serializer, transport, lifecycle hooks — each one is a small interface with a solid default. Replacing any of them is one line.
  - title: Fast and allocation-light
    details: Span-based codec with zero-allocation publish encoding, single-write framing, pipelines end to end, bounded queues everywhere. Beats MQTTnet on throughput, allocations, and connect latency.
  - title: Topic routing built in
    details: Route templates like sensors/{deviceId}/temp dispatch messages to handlers with captured parameters, per-route bounded queues, and fault isolation. Streams when you prefer await foreach.
  - title: Typed messaging and RPC
    details: Publish and consume objects through a pluggable serializer, with source-generated JSON included. Request/response over MQTT 5 response topics and correlation data, both caller and responder.
  - title: Honest engineering
    details: 300+ deterministic tests plus integration tests against real Mosquitto, fuzz-hardened decoding, verified Native AOT, and published benchmarks including the scenarios the competition wins.
---
