import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Pulse.Mqtt',
  description: 'A high-performance, resilient MQTT 5.0 client for modern .NET',
  base: '/pulse-mqtt/',
  lastUpdated: true,
  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/introduction' },
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
      '/guide/': [
        {
          text: 'Start here',
          items: [
            { text: 'Introduction', link: '/guide/introduction' },
            { text: 'Getting started', link: '/guide/getting-started' },
            { text: 'Connecting', link: '/guide/connecting' },
          ],
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
          ],
        },
        {
          text: 'Going deeper',
          items: [
            { text: 'Extending the client', link: '/guide/extending' },
            { text: 'The raw client', link: '/guide/raw-client' },
            { text: 'Native AOT', link: '/guide/native-aot' },
            { text: 'Performance', link: '/guide/performance' },
            { text: 'Releasing', link: '/guide/releasing' },
          ],
        },
      ],
      '/reference/': [
        {
          text: 'Reference',
          items: [
            { text: 'Packages', link: '/reference/packages' },
            { text: 'Options', link: '/reference/options' },
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
            { text: 'MQTTnet comparison', link: '/Benchmark-vs-MQTTnet' },
            { text: 'Development plan', link: '/NG-MQTT-Client-Development-Plan' },
            { text: 'Competitive research', link: '/Competitive-Research-MQTT-Clients' },
            { text: 'Resilience design', link: '/Phase-04-Resilience-Detailed-Design' },
          ],
        },
      ],
    },
  },
})
