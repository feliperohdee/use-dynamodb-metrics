import _ from 'lodash';
import Dynamodb, { concatConditionExpression } from 'use-dynamodb';
import z from 'zod';
import zDefault from 'use-zod-default';

const log = z.object({
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
	metrics: z.record(z.any()),
	namespace: z.string(),
	session: z.string()
});

const session = z.object({
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
	durationSeconds: z.number(),
	hits: z.number(),
	index: z.number(),
	id: z.string(),
	namespace: z.string(),
	ttl: z.number().optional()
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
	hits: z.number(),
	id: z.string(),
	namespace: z.string(),
	sessions: z.number(),
	ttl: z.number().optional(),
	uniqueUsers: z.number()
});

const metricsInput = z.record(
	z.string(),
	z.union([
		z.string(),
		z.number(),
		z.record(z.any()) // Allow nested objects
	])
);

const fetchLogsInput = z.object({
	desc: z.boolean().default(false),
	from: z.string().datetime({ offset: true }).optional(),
	limit: z.number().min(1).max(1000).default(100),
	namespace: z.string(),
	session: z.string().optional(),
	startKey: z.record(z.any()).optional(),
	to: z.string().datetime({ offset: true }).optional()
});

const fetchSessionsInput = z.object({
	desc: z.boolean().default(false),
	from: z.string().datetime({ offset: true }).optional(),
	id: z.string().optional(),
	limit: z.number().min(1).max(1000).default(100),
	namespace: z.string(),
	startKey: z.record(z.any()).optional(),
	to: z.string().datetime({ offset: true }).optional()
});

const getStatsInput = z.object({
	namespace: z.string(),
	from: z.string().datetime({ offset: true }),
	to: z.string().datetime({ offset: true })
});

const getStatsHistogramInput = getStatsInput.extend({ period: z.enum(['hour', 'day', 'week', 'month']) });

const putInput = z.object({
	namespace: z.string(),
	metrics: metricsInput,
	session: z.string().optional(),
	timestamp: z.string().datetime({ offset: true }).optional()
});

const putSessionInput = z.object({
	id: z.string(),
	namespace: z.string(),
	timestamp: z.string().datetime({ offset: true }).optional()
});

const AGGREGATE_KEYS = ['hits', 'metrics', 'sessions', 'uniqueUsers'];

namespace Stats {
	export type ConstructorOptions = {
		accessKeyId: string;
		createTable?: boolean;
		logsTableName: string;
		normalizeKeys?: boolean;
		region: string;
		secretAccessKey: string;
		sessionIdleTimeoutMinutes?: number;
		sessionTableName: string;
		statsTableName: string;
		ttlDays?: number;
		uniqueUserTimeoutMinutes?: number;
	};

	export type FetchLogsInput = z.input<typeof fetchLogsInput>;
	export type FetchSessionsInput = z.input<typeof fetchSessionsInput>;
	export type GetStatsHistogramInput = z.input<typeof getStatsHistogramInput>;
	export type GetStatsInput = z.input<typeof getStatsInput>;
	export type Log = z.infer<typeof log>;
	export type MetricsInput = z.input<typeof metricsInput>;
	export type PutInput = z.input<typeof putInput>;
	export type PutSessionInput = z.input<typeof putSessionInput>;
	export type Session = z.infer<typeof session>;
	export type Stats = z.infer<typeof stats>;
}

class Stats {
	public db: {
		logs: Dynamodb<Stats.Log>;
		session: Dynamodb<Stats.Session>;
		stats: Dynamodb<Stats.Stats>;
	};

	public normalizeKeys: boolean;
	public sessionIdleTimeoutMinutes: number;
	public ttlDays: number;
	public uniqueUserTimeoutMinutes: number;

	constructor(options: Stats.ConstructorOptions) {
		const db = {
			logs: new Dynamodb<Stats.Log>({
				accessKeyId: options.accessKeyId,
				region: options.region,
				schema: {
					partition: 'namespace',
					sort: 'id'
				},
				secretAccessKey: options.secretAccessKey,
				table: options.logsTableName
			}),
			session: new Dynamodb<Stats.Session>({
				accessKeyId: options.accessKeyId,
				region: options.region,
				schema: {
					partition: 'namespace',
					sort: 'id'
				},
				secretAccessKey: options.secretAccessKey,
				table: options.sessionTableName
			}),
			stats: new Dynamodb<Stats.Stats>({
				accessKeyId: options.accessKeyId,
				region: options.region,
				schema: {
					partition: 'namespace',
					sort: 'id'
				},
				secretAccessKey: options.secretAccessKey,
				table: options.statsTableName
			})
		};

		if (options.createTable) {
			(async () => {
				await Promise.all([db.logs.createTable(), db.session.createTable(), db.stats.createTable()]);
			})();
		}

		this.db = db;
		this.normalizeKeys = options.normalizeKeys || false;
		this.sessionIdleTimeoutMinutes = options.sessionIdleTimeoutMinutes || 30;
		this.ttlDays = options.ttlDays || 90;
		this.uniqueUserTimeoutMinutes = options.uniqueUserTimeoutMinutes || 30;
	}

	async clear(namespace: string): Promise<{ count: number }> {
		const [logs, session, stats] = await Promise.all([
			this.db.logs.clear(namespace),
			this.db.session.clear(namespace),
			this.db.stats.clear(namespace)
		]);

		return { count: logs.count + session.count + stats.count };
	}

	async fetchLogs(input: Stats.FetchLogsInput) {
		const args = await fetchLogsInput.parseAsync(input);
		const queryOptions: Dynamodb.QueryOptions<Stats.Log> = {
			attributeNames: {
				'#namespace': 'namespace'
			},
			attributeValues: {
				':namespace': args.namespace
			},
			queryExpression: '#namespace = :namespace',
			limit: args.limit,
			scanIndexForward: !args.desc,
			startKey: args.startKey
		};

		if (args.session) {
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#id': 'id'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':id': args.session
			};

			queryOptions.queryExpression = concatConditionExpression(queryOptions.queryExpression!, 'begins_with(#id, :id)');
		}

		if (args.from || args.to) {
			const from = args.from ? new Date(args.from).toISOString() : '';
			const to = args.to ? new Date(args.to).toISOString() : '';

			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#__createdAt': '__createdAt'
			};

			if (from && to) {
				queryOptions.attributeValues = {
					...queryOptions.attributeValues,
					':from': `${args.session}#${from}`,
					':to': `${args.session}#${to}`
				};

				queryOptions.filterExpression = '#__createdAt BETWEEN :from AND :to';
			} else if (from) {
				queryOptions.attributeValues = {
					...queryOptions.attributeValues,
					':from': `${args.session}#${from}`
				};

				queryOptions.filterExpression = '#__createdAt >= :from';
			} else if (to) {
				queryOptions.attributeValues = {
					...queryOptions.attributeValues,
					':to': `${args.session}#${to}`
				};

				queryOptions.filterExpression = '#__createdAt <= :to';
			}
		}

		const res = await this.db.logs.query(queryOptions);

		return {
			...res,
			items: _.map(res.items, item => {
				return zDefault(log, {
					...item,
					id: item.id.split('#')[0]
				});
			})
		};
	}

	async fetchSessions(input: Stats.FetchSessionsInput) {
		const args = await fetchSessionsInput.parseAsync(input);
		const queryOptions: Dynamodb.QueryOptions<Stats.Log> = {
			attributeNames: {
				'#namespace': 'namespace'
			},
			attributeValues: {
				':namespace': args.namespace
			},
			queryExpression: '#namespace = :namespace',
			limit: args.limit,
			scanIndexForward: !args.desc,
			startKey: args.startKey
		};

		if (args.id) {
			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#id': 'id'
			};

			queryOptions.attributeValues = {
				...queryOptions.attributeValues,
				':id': args.id
			};

			queryOptions.queryExpression = concatConditionExpression(queryOptions.queryExpression!, 'begins_with(#id, :id)');
		}

		if (args.from || args.to) {
			const from = args.from ? new Date(args.from).toISOString() : '';
			const to = args.to ? new Date(args.to).toISOString() : '';

			queryOptions.attributeNames = {
				...queryOptions.attributeNames,
				'#__createdAt': '__createdAt'
			};

			if (from && to) {
				queryOptions.attributeValues = {
					...queryOptions.attributeValues,
					':from': from,
					':to': to
				};

				queryOptions.filterExpression = '#__createdAt BETWEEN :from AND :to';
			} else if (from) {
				queryOptions.attributeValues = {
					...queryOptions.attributeValues,
					':from': from
				};

				queryOptions.filterExpression = '#__createdAt >= :from';
			} else if (to) {
				queryOptions.attributeValues = {
					...queryOptions.attributeValues,
					':to': to
				};

				queryOptions.filterExpression = '#__createdAt <= :to';
			}
		}

		const res = await this.db.session.query(queryOptions);

		return {
			...res,
			items: _.map(res.items, item => {
				return zDefault(session, item);
			})
		};
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

	private generateUpdateExpression({
		incrementSession,
		incrementUniqueUser,
		metrics
	}: {
		incrementSession: boolean;
		incrementUniqueUser: boolean;
		metrics: { key: string; value: number }[];
	}): {
		attributeNames: Record<string, string>;
		attributeValues: Record<string, number | string>;
		updateExpression: string;
	} {
		const attributeNames: Record<string, string> = {
			'#h': 'hits',
			'#s': 'sessions',
			'#t': 'ttl',
			'#u': 'uniqueUsers'
		};

		const attributeValues: Record<string, number | string> = {
			':one': 1,
			':ttl': this.getTtlSeconds()
		};

		if (!incrementSession || !incrementUniqueUser) {
			attributeValues[':zero'] = 0;
		}

		const add = _.compact([
			// Increment hits
			'#h :one',
			// Increment session
			incrementSession ? '#s :one' : '#s :zero',
			// Increment unique users
			incrementUniqueUser ? '#u :one' : '#u :zero',
			// Increment metrics
			..._.map(metrics, (metric, index) => {
				const attrName = `#m${index}`;

				attributeNames[attrName] = `metrics.${metric.key}`;

				if (metric.value !== 1) {
					const attrValue = `:v${index}`;
					attributeValues[attrValue] = metric.value;

					return `${attrName} ${attrValue}`;
				}

				return `${attrName} :one`;
			})
		]);

		return {
			attributeNames,
			attributeValues,
			updateExpression: [`ADD ${add.join(', ')}`, `SET #t = :ttl`].join(' ')
		};
	}

	async getStats(input: Stats.GetStatsInput) {
		const args = await getStatsInput.parseAsync(input);
		const from = new Date(args.from);
		from.setMinutes(0, 0, 0);

		const to = new Date(args.to);
		to.setMinutes(59, 59, 999);

		const res = await this.db.stats.query({
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
					if (
						!_.some(AGGREGATE_KEYS, aggregateKey => {
							return _.startsWith(key, aggregateKey);
						})
					) {
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
			hits: 0,
			sessions: 0,
			uniqueUsers: 0,
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

		const res = await this.db.stats.query({
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
				if (
					!_.some(AGGREGATE_KEYS, aggregateKey => {
						return _.startsWith(key, aggregateKey);
					})
				) {
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

	private getTtlSeconds(): number {
		return Math.floor(_.now() / 1000 + this.ttlDays * 24 * 60 * 60);
	}

	private hourFloor(date?: string): Date {
		const rounded = date ? new Date(date) : new Date();
		rounded.setMinutes(0, 0, 0);

		return rounded;
	}

	private isSessionExpired(lastSessionUpdatedAt: string | Date): boolean {
		const lastActivity = new Date(lastSessionUpdatedAt).getTime();
		const now = _.now();
		const idleTimeMs = now - lastActivity;

		return idleTimeMs > this.sessionIdleTimeoutMinutes * 60 * 1000;
	}

	private isUniqueUser(lastSessionUpdatedAt: string | Date): boolean {
		const lastVisit = new Date(lastSessionUpdatedAt).getTime();
		const now = _.now();
		const timeSinceLastVisitMs = now - lastVisit;

		return timeSinceLastVisitMs > this.uniqueUserTimeoutMinutes * 60 * 1000;
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
		const { incrementSession, incrementUniqueUser, session } = await this.putSession({
			id: args.session || '',
			namespace: args.namespace
		});

		if (session) {
			const suffix = args.timestamp ? new Date(args.timestamp).getTime() : _.now();
			const randomSuffix = Math.floor(Math.random() * 1000)
				.toString()
				.padStart(3, '0');

			const logId = `${session.id}#${suffix}#${randomSuffix}`;

			await this.db.logs.put(
				zDefault(log, {
					id: logId,
					metrics: args.metrics,
					namespace: args.namespace,
					session: session.id
				})
			);
		}

		const metrics = this.flattenObject(args.metrics);
		const id = this.hourFloor(args.timestamp).toISOString();
		const { attributeNames, attributeValues, updateExpression } = this.generateUpdateExpression({
			incrementSession,
			incrementUniqueUser,
			metrics
		});

		return this.db.stats.update({
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

	private async putSession(input: Stats.PutSessionInput) {
		const args = await putSessionInput.parseAsync(input);

		if (!args.id) {
			return {
				incrementSession: false,
				incrementUniqueUser: false,
				session: null
			};
		}

		const prevSession = await this.db.session.getLast({
			item: {
				id: args.id.split('#')[0],
				namespace: args.namespace
			},
			prefix: true
		});

		if (!prevSession || this.isSessionExpired(prevSession.__updatedAt)) {
			const nextIndex = prevSession ? prevSession.index + 1 : 1;
			const res = await this.db.session.put(
				zDefault(session, {
					durationSeconds: 0,
					hits: 1,
					id: `${args.id}#${nextIndex}`,
					index: nextIndex,
					namespace: args.namespace,
					ttl: this.getTtlSeconds()
				}),
				{ overwrite: true }
			);

			return {
				incrementSession: true,
				incrementUniqueUser: prevSession ? this.isUniqueUser(prevSession.__updatedAt) : true,
				session: zDefault(session, res)
			};
		}

		const updatedPrevSession = await this.db.session.update({
			attributeNames: {
				'#durationSeconds': 'durationSeconds',
				'#hits': 'hits'
			},
			attributeValues: {
				':durationSeconds': Math.floor((_.now() - new Date(prevSession.__updatedAt).getTime()) / 1000),
				':one': 1
			},
			filter: {
				item: {
					id: prevSession.id,
					namespace: prevSession.namespace
				}
			},
			updateExpression: 'ADD #hits :one SET #durationSeconds = :durationSeconds'
		});

		return {
			incrementSession: false,
			incrementUniqueUser: this.isUniqueUser(prevSession.__updatedAt),
			session: zDefault(session, updatedPrevSession)
		};
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
