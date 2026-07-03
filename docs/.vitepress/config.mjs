import { defineConfig } from 'vitepress'

// One entry per add-on package. The Add-ons group lives in the shared sidebar below, so these
// pages sit in the same tree as the rest of the docs even though they live under /packages/.
const addOnPackages = [
  { text: 'Overview', link: '/packages/' },
  { text: 'Dependency injection', link: '/packages/dependency-injection' },
  { text: 'Endpoints', link: '/packages/endpoints' },
  { text: 'Dataflow', link: '/packages/dataflow' },
  { text: 'SQLite storage', link: '/packages/storage-sqlite' },
  { text: 'LiteDB storage', link: '/packages/storage-litedb' },
  { text: 'Serializer overview', link: '/packages/serializers' },
  { text: 'JSON serializer', link: '/packages/serialization-json' },
  { text: 'MessagePack serializer', link: '/packages/serialization-messagepack' },
  { text: 'Protobuf serializer', link: '/packages/serialization-protobuf' },
  { text: 'WebSocket transport', link: '/packages/transport-websocket' },
  { text: 'QUIC transport', link: '/packages/transport-quic' },
  { text: 'Reconnect policy', link: '/packages/resilience-polly' },
  { text: 'Testing', link: '/packages/testing' },
  { text: 'Analyzers', link: '/packages/analyzers' },
]

// The Guide and Packages sections share one sidebar, so following an add-on link never swaps the
// sidebar out from under the reader or drops them into a different-looking tree.
const mainSidebar = [
  {
    text: 'Start here',
    items: [
      { text: 'Introduction', link: '/guide/introduction' },
      { text: 'Getting started', link: '/guide/getting-started' },
      { text: 'Package add-ons', link: '/guide/package-add-ons' },
      { text: 'Connecting', link: '/guide/connecting' },
    ],
  },
  {
    text: 'Add-ons',
    collapsed: false,
    items: addOnPackages,
  },
  {
    text: 'Messaging',
    items: [
      { text: 'Publishing', link: '/guide/publishing' },
      { text: 'Subscribing', link: '/guide/subscribing' },
      { text: 'Routing', link: '/guide/routing' },
      { text: 'Typed messaging', link: '/guide/typed-messaging' },
      { text: 'Request and response', link: '/guide/request-response' },
      { text: 'Fluent API', link: '/guide/fluent-api' },
    ],
  },
  {
    text: 'Operations',
    items: [
      { text: 'Resilience', link: '/guide/resilience' },
      { text: 'Presence', link: '/guide/presence' },
      { text: 'Lifecycle and state', link: '/guide/lifecycle' },
      { text: 'Dependency injection', link: '/guide/dependency-injection' },
      { text: 'Health checks', link: '/guide/health-checks' },
      { text: 'Observability', link: '/guide/observability' },
      { text: 'Testing', link: '/guide/testing' },
      { text: 'Analyzers', link: '/guide/analyzers' },
    ],
  },
  {
    text: 'Going deeper',
    items: [
      { text: 'Migrating from MQTTnet', link: '/guide/migrating-from-mqttnet' },
      { text: 'Extending the client', link: '/guide/extending' },
      { text: 'The raw client', link: '/guide/raw-client' },
      { text: 'Native AOT', link: '/guide/native-aot' },
      { text: 'Performance', link: '/guide/performance' },
      { text: 'Releasing', link: '/guide/releasing' },
    ],
  },
]

export default defineConfig({
  title: 'Pulse.Mqtt',
  description: 'A high-performance, resilient MQTT 5.0 client for modern .NET',
  base: '/pulse-mqtt/',
  lastUpdated: true,
  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/introduction' },
      { text: 'Packages', link: '/packages/' },
      { text: 'Reference', link: '/reference/packages' },
      { text: 'Benchmarks', link: '/Benchmark-vs-MQTTnet' },
    ],
    socialLinks: [
      { icon: 'github', link: 'https://github.com/araxis/pulse-mqtt' },
    ],
    search: { provider: 'local' },
    outline: { level: [2, 3] },
    footer: {
      message: 'Released under the MIT License.',
    },
    sidebar: {
      '/guide/': mainSidebar,
      '/packages/': mainSidebar,
      '/reference/': [
        {
          text: 'Reference',
          items: [
            { text: 'Packages', link: '/reference/packages' },
            { text: 'Options', link: '/reference/options' },
            { text: 'MQTT protocol compatibility', link: '/reference/protocol-compatibility' },
            { text: 'Connection states', link: '/reference/connection-states' },
            { text: 'Errors', link: '/reference/errors' },
            { text: 'Broker compatibility', link: '/reference/broker-compatibility' },
          ],
        },
        {
          text: 'Project',
          items: [
            { text: 'Road to 1.0', link: '/road-to-1.0' },
            { text: 'Benchmark suite', link: '/Benchmark-Suite' },
            { text: 'Benchmark comparison', link: '/Benchmark-vs-MQTTnet' },
            { text: 'Development plan', link: '/NG-MQTT-Client-Development-Plan' },
            { text: 'Resilience design', link: '/Phase-04-Resilience-Detailed-Design' },
          ],
        },
      ],
    },
  },
})
