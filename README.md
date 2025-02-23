# use-dynamodb-stats

A TypeScript library for storing and aggregating time-series metrics using Amazon DynamoDB. It provides a robust, scalable system for tracking statistics with configurable time periods and automatic data aggregation.

[![TypeScript](https://img.shields.io/badge/-TypeScript-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Vitest](https://img.shields.io/badge/-Vitest-729B1B?style=flat-square&logo=vitest&logoColor=white)](https://vitest.dev/)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## ✨ Features

- 💾 **DynamoDB Backend**: Uses DynamoDB for persistent storage of time-series metrics
- 🔄 **Automatic Aggregation**: Automatically aggregates metrics over time periods
- 🏷️ **Namespace Support**: Group statistics by namespaces for better organization
- ⏱️ **TTL Support**: Automatic cleanup of old data using DynamoDB TTL
- 📊 **Flexible Time Periods**: Support for hourly, daily, weekly, and monthly aggregation
- 🔍 **Nested Metrics**: Support for complex nested metric structures

## Installation

```bash
npm install use-dynamodb-stats
# or
yarn add use-dynamodb-stats
```

## Quick Start

### Initialize Stats Tracking

```typescript
import Stats from 'use-dynamodb-stats';

const stats = new Stats({
	accessKeyId: process.env.AWS_ACCESS_KEY_ID,
	secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
	region: process.env.AWS_REGION,
	tableName: 'YOUR_TABLE_NAME',
	createTable: true, // Optional: automatically create DynamoDB table
	ttlDays: 90 // Optional: number of days to keep data (default: 90)
});
```

### Record Statistics

```typescript
// Record metrics
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
	}
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
```

### Clear Statistics

```typescript
// Clear all stats for a namespace
const result = await stats.clearStats('app-metrics');
console.log(`Cleared ${result.count} statistics records`);
```

## API Reference

### Constructor Options

```typescript
type ConstructorOptions = {
	accessKeyId: string;
	secretAccessKey: string;
	region: string;
	tableName: string;
	createTable?: boolean;
	ttlDays?: number; // Default: 90
};
```

### Recording Stats

```typescript
type PutInput = {
	namespace: string;
	metrics: Record<string, string | number | Record<string, any>>;
	timestamp?: Date; // Optional: specify custom timestamp
};
```

### Querying Stats

```typescript
type GetStatsInput = {
	namespace: string;
	from: string; // ISO datetime
	to: string; // ISO datetime
};

type GetStatsHistogramInput = GetStatsInput & {
	period: 'hour' | 'day' | 'week' | 'month';
};
```

## How It Works

The library automatically:

- Flattens nested metric objects for storage (e.g., `errors.validation` becomes `metrics.errors.validation`)
- Aggregates numeric values over time periods
- Counts occurrences of string values (e.g., status codes)
- Rounds timestamps to the specified period for aggregation
- Handles timezone conversions and ISO8601 dates
- Manages data TTL for automatic cleanup

## DynamoDB Schema

The library uses the following DynamoDB schema:

- Partition Key: `namespace`
- Sort Key: `id` (timestamp-based)
- Additional Attributes:
  - `metrics.*`: Flattened metric values
  - `ttl`: Unix timestamp for DynamoDB TTL
  - `__createdAt`: ISO timestamp of creation
  - `__updatedAt`: ISO timestamp of last update

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
