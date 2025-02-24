import _ from 'lodash';
import Dynamodb from 'use-dynamodb';
import z from 'zod';

const metricsInput = z.record(
	z.string(),
	z.union([
		z.string(),
		z.number(),
		z.record(z.any()) // Allow nested objects
	])
);

const getStatsInput = z.object({
	namespace: z.string(),
	from: z.string().datetime({ offset: true }),
	to: z.string().datetime({ offset: true })
});

const getStatsHistogramInput = getStatsInput.extend({
	period: z.enum(['hour', 'day', 'week', 'month'])
});

const putInput = z.object({
	namespace: z.string(),
	metrics: metricsInput,
	timestamp: z.string().datetime({ offset: true }).optional()
});

const stats = z.object({
	__createdAt: z
		.string()
		.datetime()
		.default(() => {
			return new Date().toISOString();
		}),
	__updatedAt: z
		.string()
		.datetime()
		.default(() => {
			return new Date().toISOString();
		}),
	id: z.string(),
	namespace: z.string(),
	ttl: z.number().optional()
});

namespace Stats {
	export type ConstructorOptions = {
		accessKeyId: string;
		createTable?: boolean;
		normalizeKeys?: boolean;
		region: string;
		secretAccessKey: string;
		tableName: string;
		ttlDays?: number;
	};

	export type GetStatsInput = z.infer<typeof getStatsInput>;
	export type GetStatsHistogramInput = z.infer<typeof getStatsHistogramInput>;
	export type MetricsInput = z.infer<typeof metricsInput>;
	export type PutInput = z.infer<typeof putInput>;
	export type Stats = z.infer<typeof stats>;
}

class Stats {
	private db: Dynamodb<Stats.Stats>;
	private normalizeKeys: boolean;
	private ttlDays: number;

	constructor(options: Stats.ConstructorOptions) {
		const db = new Dynamodb<Stats.Stats>({
			accessKeyId: options.accessKeyId,
			region: options.region,
			schema: {
				partition: 'namespace',
				sort: 'id'
			},
			secretAccessKey: options.secretAccessKey,
			table: options.tableName
		});

		if (options.createTable) {
			(async () => {
				await db.createTable();
			})();
		}

		this.db = db;
		this.normalizeKeys = options.normalizeKeys || false;
		this.ttlDays = options.ttlDays || 90;
	}

	async clearStats(namespace: string): Promise<{ count: number }> {
		return this.db.clear(namespace);
	}

	private flattenObject(obj: Record<string, any>, prefix = ''): { key: string; value: number }[] {
		return _.reduce(
			obj,
			(reduction: { key: string; value: number }[], value, key) => {
				if (this.normalizeKeys) {
					key = this.normalizeString(key);
				}

				const newKey = prefix ? `${prefix}.${key}` : key;

				if (value && _.isObject(value) && !_.isArray(value)) {
					return [...reduction, ...this.flattenObject(value, newKey)];
				}

				if (_.isNumber(value)) {
					return [...reduction, { key: newKey, value }];
				}

				if (_.isString(value)) {
					if (this.normalizeKeys) {
						value = this.normalizeString(value);
					}

					return [...reduction, { key: `${newKey}.${value}`, value: 1 }];
				}

				return reduction;
			},
			[]
		);
	}

	private generateUpdateExpression(metrics: { key: string; value: number }[]): {
		attributeNames: Record<string, string>;
		attributeValues: Record<string, number>;
		updateExpression: string;
	} {
		const attributeNames: Record<string, string> = {
			'#t': 'ttl'
		};
		const attributeValues: Record<string, number> = {
			':t': Math.floor(Date.now() / 1000 + this.ttlDays * 24 * 60 * 60)
		};

		const additions = _.map(metrics, (metric, index) => {
			const attrName = `#m${index}`;
			const attrValue = `:v${index}`;

			attributeNames[attrName] = `metrics.${metric.key}`;
			attributeValues[attrValue] = metric.value;

			return `${attrName} ${attrValue}`;
		});

		return {
			attributeNames,
			attributeValues,
			updateExpression: [`ADD ${additions.join(', ')}`, `SET #t = :t`].join(' ')
		};
	}

	async getStats(input: Stats.GetStatsInput) {
		const args = await getStatsInput.parseAsync(input);
		const from = new Date(args.from);
		from.setMinutes(0, 0, 0);

		const to = new Date(args.to);
		to.setMinutes(59, 59, 999);

		const res = await this.db.query({
			attributeNames: {
				'#id': 'id',
				'#namespace': 'namespace'
			},
			attributeValues: {
				':from': from.toISOString(),
				':namespace': args.namespace,
				':to': to.toISOString()
			},
			limit: Infinity,
			queryExpression: '#namespace = :namespace AND #id BETWEEN :from AND :to'
		});

		const aggregatedMetrics = _.reduce(
			res.items,
			(reduction, item) => {
				_.forEach(item, (value, key) => {
					if (!_.startsWith(key, 'metrics.')) {
						return;
					}

					if (!_.isNumber(value)) {
						return;
					}

					reduction[key] = (reduction[key] || 0) + value;
				});

				return reduction;
			},
			{} as Record<string, number>
		);

		return {
			metrics: {},
			...this.unflattenMetrics(aggregatedMetrics),
			from: from.toISOString(),
			namespace: args.namespace,
			to: to.toISOString()
		};
	}

	async getStatsHistogram(input: Stats.GetStatsHistogramInput) {
		const args = await getStatsHistogramInput.parseAsync(input);
		const from = new Date(args.from);
		from.setMinutes(0, 0, 0);

		const to = new Date(args.to);
		to.setMinutes(59, 59, 999);

		const res = await this.db.query({
			attributeNames: {
				'#id': 'id',
				'#namespace': 'namespace'
			},
			attributeValues: {
				':from': from.toISOString(),
				':namespace': args.namespace,
				':to': to.toISOString()
			},
			limit: Infinity,
			queryExpression: '#namespace = :namespace AND #id BETWEEN :from AND :to'
		});

		// Group items into buckets based on the chosen period
		const histogram: Record<string, Record<string, number>> = {};

		for (const item of res.items) {
			const itemDate = new Date(item.id);
			const bucketDate = this.roundToPeriod(itemDate, args.period);
			const bucketKey = bucketDate.toISOString();

			if (!histogram[bucketKey]) {
				histogram[bucketKey] = {};
			}

			// Aggregate metric values from each item into its bucket
			for (const key in item) {
				if (!_.startsWith(key, 'metrics.')) {
					continue;
				}

				// @ts-expect-error
				if (!_.isNumber(item[key])) {
					continue;
				}

				// @ts-expect-error
				histogram[bucketKey][key] = (histogram[bucketKey][key] || 0) + item[key];
			}
		}

		const completeHistogram: Record<string, any> = {};
		const fromBucketDate = this.roundToPeriod(from, args.period);
		const toBucketDate = this.roundToPeriod(to, args.period);

		while (fromBucketDate <= toBucketDate) {
			const bucketKey = fromBucketDate.toISOString();

			completeHistogram[bucketKey] = this.unflattenMetrics(histogram[bucketKey] || {});

			switch (args.period) {
				case 'hour':
					fromBucketDate.setHours(fromBucketDate.getHours() + 1);
					break;
				case 'day':
					fromBucketDate.setDate(fromBucketDate.getDate() + 1);
					break;
				case 'week':
					fromBucketDate.setDate(fromBucketDate.getDate() + 7);
					break;
				case 'month':
					fromBucketDate.setMonth(fromBucketDate.getMonth() + 1);
					break;
			}
		}

		return {
			from: from.toISOString(),
			histogram: completeHistogram,
			namespace: args.namespace,
			period: args.period,
			to: to.toISOString()
		};
	}

	private hourFloor(date?: string): Date {
		const rounded = date ? new Date(date) : new Date();
		rounded.setMinutes(0, 0, 0);

		return rounded;
	}

	private normalizeString = _.memoize((value: string): string => {
		value = _.trim(value);
		value = _.toLower(value);
		value = _.deburr(value);
		value = value.replace(/([a-z])([A-Z])/g, '$1-$2');
		value = value.replace(/\s+/g, '-');
		value = value.replace(/\-+/g, '-');
		value = _.trim(value, '-');

		return value;
	});

	async put(input: Stats.PutInput) {
		const args = await putInput.parseAsync(input);
		const flattened = this.flattenObject(args.metrics);
		const id = this.hourFloor(args.timestamp).toISOString();
		const { attributeNames, attributeValues, updateExpression } = this.generateUpdateExpression(flattened);

		return this.db.update({
			attributeNames,
			attributeValues,
			filter: {
				item: {
					id,
					namespace: args.namespace
				}
			},
			updateExpression,
			upsert: true
		});
	}

	private roundToPeriod(date: Date, period: 'hour' | 'day' | 'week' | 'month'): Date {
		const newDate = new Date(date);

		switch (period) {
			case 'hour':
				newDate.setMinutes(0, 0, 0);
				break;
			case 'day':
				newDate.setHours(0, 0, 0, 0);
				break;
			case 'week': {
				newDate.setHours(0, 0, 0, 0);

				const day = newDate.getDay(); // 0 (Sun) to 6 (Sat)
				newDate.setDate(newDate.getDate() - day);
				break;
			}
			case 'month':
				newDate.setDate(1);
				newDate.setHours(0, 0, 0, 0);
				break;
		}

		return newDate;
	}

	private unflattenMetrics(metrics: Record<string, number>): Record<string, any> {
		const res: Record<string, any> = {};

		_.forEach(metrics, (value, key) => {
			_.set(res, key, value);
		});

		return res;
	}
}

export default Stats;
