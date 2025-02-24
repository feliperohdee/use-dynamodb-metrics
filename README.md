# use-dynamodb-metrics

A TypeScript library for storing and aggregating time-series metrics using Amazon DynamoDB. It provides a robust, scalable system for tracking statistics with configurable time periods and automatic data aggregation.

[![TypeScript](https://img.shields.io/badge/-TypeScript-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Vitest](https://img.shields.io/badge/-Vitest-729B1B?style=flat-square&logo=vitest&logoColor=white)](https://vitest.dev/)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## ✨ Features

- 💾 **DynamoDB Backend**: Uses DynamoDB for persistent storage of time-series metrics
- 🔄 **Automatic Aggregation**: Automatically aggregates metrics over time periods
- 🏷️ **Namespace Support**: Group statistics by namespaces for better organization
- 📈 **Session Tracking**: Built-in support for tracking user sessions and unique users
- 📝 **Detailed Logs**: Maintains logs of individual events for detailed analysis
- ⏱️ **TTL Support**: Automatic cleanup of old data using DynamoDB TTL
- 📊 **Flexible Time Periods**: Support for hourly, daily, weekly, and monthly aggregation
- 🔍 **Nested Metrics**: Support for complex nested metric structures
- 🧹 **Key Normalization**: Optional normalization of metric keys for consistency

## Installation

```bash
npm install use-dynamodb-metrics
# or
yarn add use-dynamodb-metrics
```

## Quick Start

### Initialize Stats Tracking

```typescript
import Stats from 'use-dynamodb-metrics';

const stats = new Stats({
	accessKeyId: process.env.AWS_ACCESS_KEY,
	secretAccessKey: process.env.AWS_SECRET_KEY,
	region: process.env.AWS_REGION,
	statsTableName: 'app-metrics-stats',
	logsTableName: 'app-metrics-logs',
	sessionTableName: 'app-metrics-sessions',
	createTable: true, // Optional: automatically create DynamoDB tables
	normalizeKeys: false, // Optional: normalize metric keys (default: false)
	ttlDays: 90 // Optional: number of days to keep data (default: 90)
});
```

### Record Statistics

```typescript
// Record metrics with automatic session handling
await stats.put({
	namespace: 'app-metrics',
	metrics: {
		requestCount: 1,
		responseTime: 150,
		errors: {
			validation: 2,
			server: 1
		},
		status: 'success'
	},
	session: 'user-123', // Optional: specify a session ID
	timestamp: '2024-01-01T15:30:00.000Z' // Optional: specify custom timestamp
});
```

### Query Statistics

```typescript
// Get aggregated stats for a time period
const result = await stats.getStats({
	namespace: 'app-metrics',
	from: '2024-01-01T00:00:00Z',
	to: '2024-01-31T23:59:59Z'
});

// Get histogram of stats over time
const histogram = await stats.getStatsHistogram({
	namespace: 'app-metrics',
	from: '2024-01-01T00:00:00Z',
	to: '2024-01-31T23:59:59Z',
	period: 'day' // 'hour' | 'day' | 'week' | 'month'
});

// Fetch session data
const sessions = await stats.fetchSessions({
	namespace: 'app-metrics',
	from: '2024-01-01T00:00:00Z',
	to: '2024-01-31T23:59:59Z',
	id: 'user-123' // Optional: filter by session ID
});

// Fetch detailed event logs
const logs = await stats.fetchLogs({
	namespace: 'app-metrics',
	from: '2024-01-01T00:00:00Z',
	to: '2024-01-31T23:59:59Z',
	session: 'user-123', // Optional: filter by session ID
	limit: 100, // Optional: limit number of results
	desc: true // Optional: sort in descending order
});
```

### Clear Statistics

```typescript
// Clear all stats for a namespace
const result = await stats.clear('app-metrics');
console.log(`Cleared ${result.count} statistics records`);
```

## API Reference

### Constructor Options

```typescript
type ConstructorOptions = {
	accessKeyId: string;
	secretAccessKey: string;
	region: string;
	statsTableName: string;
	logsTableName: string;
	sessionTableName: string;
	createTable?: boolean;
	normalizeKeys?: boolean; // Default: false
	sessionIdleTimeoutMinutes?: number; // Default: 30
	uniqueUserTimeoutMinutes?: number; // Default: 30
	ttlDays?: number; // Default: 90
};
```

### Recording Stats

```typescript
type PutInput = {
	namespace: string;
	metrics: Record<string, string | number | Record<string, any>>;
	session?: string; // Optional: session identifier
	timestamp?: string; // Optional: ISO datetime
};
```

### Querying Stats

```typescript
// Fetch aggregated stats for a time period
type GetStatsInput = {
	namespace: string;
	from: string; // ISO datetime
	to: string; // ISO datetime
};

// Fetch histogram of stats over time
type GetStatsHistogramInput = GetStatsInput & {
	period: 'hour' | 'day' | 'week' | 'month';
};

// Fetch session data
type FetchSessionsInput = {
	namespace: string;
	from?: string; // Optional: ISO datetime
	to?: string; // Optional: ISO datetime
	id?: string; // Optional: session ID prefix
	limit?: number; // Optional: max results (default: 100, max: 1000)
	desc?: boolean; // Optional: sort direction (default: false)
	startKey?: Record<string, any>; // Optional: for pagination
};

// Fetch detailed logs
type FetchLogsInput = {
	namespace: string;
	from?: string; // Optional: ISO datetime
	to?: string; // Optional: ISO datetime
	session?: string; // Optional: session ID
	limit?: number; // Optional: max results (default: 100, max: 1000)
	desc?: boolean; // Optional: sort direction (default: false)
	startKey?: Record<string, any>; // Optional: for pagination
};
```

## How It Works

The library uses three DynamoDB tables:

1. **Stats Table**: Stores aggregated metrics by time period
2. **Session Table**: Tracks user sessions with hit counts and durations
3. **Logs Table**: Records individual events with detailed metrics

Key features:

- Automatically flattens nested metric objects (e.g., `errors.validation` becomes `metrics.errors.validation`)
- Aggregates numeric values over time periods
- Counts occurrences of string values (e.g., `status: 'success'` increments `metrics.status.success`)
- Tracks sessions and unique users with configurable timeout periods
- Rounds timestamps to the appropriate periods for aggregation
- Handles timezone conversions and ISO8601 dates with offsets
- Manages data TTL for automatic cleanup

## DynamoDB Schema

The library uses the following DynamoDB schema across its three tables:

### Stats Table

- Partition Key: `namespace`
- Sort Key: `id` (timestamp-based)
- Additional Attributes:
  - `hits`: Count of events
  - `sessions`: Count of unique sessions
  - `uniqueUsers`: Count of unique users
  - `metrics.*`: Flattened metric values
  - `ttl`: Unix timestamp for DynamoDB TTL

### Session Table

- Partition Key: `namespace`
- Sort Key: `id` (session ID with index)
- Additional Attributes:
  - `hits`: Count of events in session
  - `index`: Session index counter
  - `durationSeconds`: Session duration
  - `ttl`: Unix timestamp for DynamoDB TTL

### Logs Table

- Partition Key: `namespace`
- Sort Key: `id` (complex key with session ID and timestamp)
- Additional Attributes:
  - `session`: Session identifier
  - `metrics`: Complete metrics object

## Development

```bash
# Required environment variables
export AWS_ACCESS_KEY='YOUR_ACCESS_KEY'
export AWS_SECRET_KEY='YOUR_SECRET_KEY'
export AWS_REGION='YOUR_REGION'

# Run tests
yarn test
```

## License

MIT © [Felipe Rohde](mailto:feliperohdee@gmail.com)

## 👨‍💻 Author

**Felipe Rohde**

- Twitter: [@feliperohdee](https://twitter.com/felipe_rohde)
- Github: [@feliperohdee](https://github.com/feliperohdee)
- Email: feliperohdee@gmail.com
