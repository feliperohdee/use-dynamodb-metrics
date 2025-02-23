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
	timestamp: z.date().optional()
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
		this.ttlDays = options.ttlDays || 90;
	}

	async clearStats(namespace: string): Promise<{ count: number }> {
		return this.db.clear(namespace);
	}

	private flattenObject(obj: Record<string, any>, prefix = ''): { key: string; value: number }[] {
		return _.reduce(
			obj,
			(reduction: { key: string; value: number }[], value, key) => {
				const newKey = prefix ? `${prefix}.${key}` : key;

				if (value && _.isObject(value) && !_.isArray(value)) {
					return [...reduction, ...this.flattenObject(value, newKey)];
				}

				if (_.isNumber(value)) {
					return [...reduction, { key: newKey, value }];
				}

				if (_.isString(value)) {
					return [...reduction, { key: `${newKey}.${value}`, value: 1 }];
				}

				return reduction;
			},
			[]
		);
	}

	private generateTimeId(date: Date = new Date()): string {
		return this.roundToHour(date).toISOString();
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
		const from = this.generateTimeId(new Date(args.from));
		const to = this.generateTimeId(new Date(args.to));

		const res = await this.db.query({
			attributeNames: {
				'#id': 'id',
				'#namespace': 'namespace'
			},
			attributeValues: {
				':from': from,
				':namespace': args.namespace,
				':to': to
			},
			queryExpression: '#namespace = :namespace AND #id BETWEEN :from AND :to'
		});

		const aggregatedMetrics = _.reduce(
			res.items,
			(reduction, record) => {
				_.forEach(record, (value, key) => {
					if (!_.startsWith(key, 'metrics.')) {
						return;
					}

					reduction[key] = (reduction[key] || 0) + (value as number);
				});

				return reduction;
			},
			{} as Record<string, number>
		);

		return {
			metrics: {},
			...this.unflattenMetrics(aggregatedMetrics),
			from,
			namespace: args.namespace,
			to
		};
	}

	async put(input: Stats.PutInput) {
		const args = await putInput.parseAsync(input);
		const flattened = this.flattenObject(args.metrics);
		const id = this.generateTimeId(args.timestamp);
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

	private roundToHour(date: Date): Date {
		const rounded = new Date(date);
		rounded.setMinutes(0, 0, 0);

		return rounded;
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
