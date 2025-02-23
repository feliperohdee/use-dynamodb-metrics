import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, Mock, vi } from 'vitest';

import Stats from './index';

describe('/index', () => {
	let stats: Stats;

	beforeAll(() => {
		stats = new Stats({
			accessKeyId: process.env.AWS_ACCESS_KEY || '',
			createTable: true,
			region: process.env.AWS_REGION || '',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
			tableName: 'use-dynamodb-stats-spec'
		});
	});

	beforeEach(() => {
		stats = new Stats({
			accessKeyId: process.env.AWS_ACCESS_KEY || '',
			createTable: true,
			region: process.env.AWS_REGION || '',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
			tableName: 'use-dynamodb-stats-spec'
		});
	});

	afterAll(async () => {
		await stats.clearStats('spec');
	});

	describe('flattenObject', () => {
		it('should flatten', () => {
			const input = {
				value1: 10,
				value2: 20,
				value3: 'test'
			};

			// @ts-expect-error
			const res = stats.flattenObject(input);

			expect(res).toEqual([
				{ key: 'value1', value: 10 },
				{ key: 'value2', value: 20 },
				{ key: 'value3.test', value: 1 }
			]);
		});

		it('should flatten nested object', () => {
			const input = {
				nested: {
					deep: {
						value1: 30,
						value2: 20,
						value3: 'test'
					}
				},
				value1: 10,
				value2: 20,
				value3: 'test'
			};

			// @ts-expect-error
			const res = stats.flattenObject(input);
			expect(res).toEqual([
				{
					key: 'nested.deep.value1',
					value: 30
				},
				{
					key: 'nested.deep.value2',
					value: 20
				},
				{
					key: 'nested.deep.value3.test',
					value: 1
				},
				{
					key: 'value1',
					value: 10
				},
				{
					key: 'value2',
					value: 20
				},
				{
					key: 'value3.test',
					value: 1
				}
			]);
		});
	});

	describe('generateTimeId', () => {
		it('should round time to the hour', () => {
			const date = new Date('2024-01-01T15:30:45.123Z');

			// @ts-expect-error
			const res = stats.generateTimeId(date);
			expect(res).toEqual('2024-01-01T15:00:00.000Z');
		});

		it('should handle dates with gmt offset', () => {
			const date = new Date('2024-01-01T15:30:45.123-03:00');

			// @ts-expect-error
			const res = stats.generateTimeId(date);
			expect(res).toEqual('2024-01-01T18:00:00.000Z');
		});

		it('should use current time when no date provided', () => {
			const now = new Date();
			now.setMinutes(0, 0, 0);

			// @ts-expect-error
			const res = stats.generateTimeId();
			expect(new Date(res).getHours()).toEqual(now.getHours());
		});
	});

	describe('generateUpdateExpression', () => {
		it('should generate correct DynamoDB update expression', () => {
			const metrics = [
				{ key: 'value1', value: 10 },
				{ key: 'value2', value: 20 }
			];

			// @ts-expect-error
			const res = stats.generateUpdateExpression(metrics);

			expect(res.attributeNames).toEqual({
				'#t': 'ttl',
				'#m0': 'metrics.value1',
				'#m1': 'metrics.value2'
			});

			expect(res.attributeValues).toEqual({
				':v0': 10,
				':v1': 20,
				':t': expect.any(Number)
			});

			const ttlSeconds = res.attributeValues[':t'] - Date.now() / 1000;
			const ttlsDays = ttlSeconds / (60 * 60 * 24);

			// @ts-expect-error
			expect(ttlsDays).toBeCloseTo(stats.ttlDays);
			expect(res.updateExpression).toEqual('ADD #m0 :v0, #m1 :v1 SET #t = :t');
		});
	});

	describe('getStats', () => {
		beforeEach(async () => {
			await Promise.all(
				_.times(2, () => {
					return stats.put({
						metrics: {
							nested: {
								deep: {
									value1: 10,
									value2: 20,
									value3: 'test'
								}
							},
							value1: 10,
							value2: 20,
							value3: 'test'
						},
						namespace: 'spec'
					});
				})
			);
		});

		afterEach(async () => {
			await stats.clearStats('spec');
		});

		it('should get stats', async () => {
			const from = new Date();
			from.setHours(0, 0, 0, 0);

			const to = new Date();
			to.setHours(23, 59, 59, 999);

			const res = await stats.getStats({
				from: from.toISOString(),
				namespace: 'spec',
				to: to.toISOString()
			});

			expect(res).toEqual({
				// @ts-expect-error
				from: stats.generateTimeId(from),
				metrics: {
					nested: {
						deep: {
							value2: 40,
							value3: {
								test: 2
							},
							value1: 20
						}
					},
					value3: {
						test: 2
					},
					value1: 20,
					value2: 40
				},
				namespace: 'spec',
				// @ts-expect-error
				to: stats.generateTimeId(to)
			});
		});

		it('should get empty stats', async () => {
			const from = new Date();
			from.setHours(0, 0, 0, 0);

			const to = new Date();
			to.setHours(0, 0, 0, 0);

			const res = await stats.getStats({
				from: from.toISOString(),
				namespace: 'spec',
				to: to.toISOString()
			});

			expect(res).toEqual({
				// @ts-expect-error
				from: stats.generateTimeId(from),
				metrics: {},
				namespace: 'spec',
				// @ts-expect-error
				to: stats.generateTimeId(to)
			});
		});
	});

	describe('put', () => {
		afterEach(async () => {
			await stats.clearStats('spec');
		});

		it('should put metrics', async () => {
			const res = await stats.put({
				metrics: {
					nested: {
						deep: {
							value1: 10,
							value2: 20,
							value3: 'test'
						}
					},
					value1: 10,
					value2: 20,
					value3: 'test'
				},
				namespace: 'spec'
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				id: expect.any(String),
				'metrics.nested.deep.value1': 10,
				'metrics.nested.deep.value2': 20,
				'metrics.nested.deep.value3.test': 1,
				'metrics.value1': 10,
				'metrics.value2': 20,
				'metrics.value3.test': 1,
				namespace: 'spec',
				ttl: expect.any(Number)
			});
		});
	});

	describe('roundToHour', () => {
		it('should round time to the start of the hour', () => {
			const date = new Date('2024-01-01T15:30:45.123Z');

			// @ts-expect-error
			const res = stats.roundToHour(date);
			expect(res.toISOString()).toEqual('2024-01-01T15:00:00.000Z');
		});

		it('should not modify original date object', () => {
			const original = new Date('2024-01-01T15:30:45.123Z');
			const originalTime = original.getTime();

			// @ts-expect-error
			stats.roundToHour(original);
			expect(original.getTime()).toEqual(originalTime);
		});
	});

	describe('unflattenMetrics', () => {
		it('should unflatten', () => {
			const input = {
				value1: 10,
				value2: 20
			};

			// @ts-expect-error
			const res = stats.unflattenMetrics(input);
			expect(res).toEqual({
				value1: 10,
				value2: 20
			});
		});

		it('should unflatten nested metrics', () => {
			const input = {
				value1: 10,
				value2: 20,
				'value3.test': 1,
				'nested.deep.value1': 30,
				'nested.deep.value2': 20,
				'nested.deep.value3.test': 1
			};

			// @ts-expect-error
			const res = stats.unflattenMetrics(input);
			expect(res).toEqual({
				nested: {
					deep: {
						value1: 30,
						value2: 20,
						value3: {
							test: 1
						}
					}
				},
				value1: 10,
				value2: 20,
				value3: {
					test: 1
				}
			});
		});
	});
});
