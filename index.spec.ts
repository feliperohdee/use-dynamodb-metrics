import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, Mock, vi } from 'vitest';

import Stats from './index';

process.env.TZ = 'UTC';

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

		it('should normalize keys', () => {
			// @ts-expect-error
			stats.normalizeKeys = true;

			const input = {
				Develóper: 10,
				'Devel  óper': 'TypéScrípt'
			};

			// @ts-expect-error
			const res = stats.flattenObject(input);
			expect(res).toEqual([
				{ key: 'developer', value: 10 },
				{ key: 'devel-oper.typescript', value: 1 }
			]);
		});
	});

	describe('generateUpdateExpression', () => {
		it('should generate DynamoDB update expression', () => {
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

	describe('getStats / getStatsHistogram', () => {
		beforeAll(async () => {
			await Promise.all(
				_.times(3, i => {
					const timestamp = new Date('2024-01-01T15:30:00.000Z');
					timestamp.setDate(timestamp.getDate() + (i % 2));

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
						namespace: 'spec',
						timestamp: timestamp.toISOString()
					});
				})
			);
		});

		afterAll(async () => {
			await stats.clearStats('spec');
		});

		describe('getStats', () => {
			it('should get stats', async () => {
				const res = await stats.getStats({
					from: '2024-01-01T15:30:00.000Z',
					namespace: 'spec',
					to: '2024-01-02T15:30:00.000Z'
				});

				expect(res).toEqual({
					from: '2024-01-01T15:00:00.000Z',
					metrics: {
						nested: {
							deep: {
								value1: 30,
								value2: 60,
								value3: {
									test: 3
								}
							}
						},
						value3: {
							test: 3
						},
						value1: 30,
						value2: 60
					},
					namespace: 'spec',
					to: '2024-01-02T15:59:59.999Z'
				});
			});

			it('should get empty stats', async () => {
				const res = await stats.getStats({
					from: '2024-02-01T12:30:00-03:00',
					namespace: 'spec',
					to: '2024-02-02T12:30:00-03:00'
				});

				expect(res).toEqual({
					from: '2024-02-01T15:00:00.000Z',
					metrics: {},
					namespace: 'spec',
					to: '2024-02-02T15:59:59.999Z'
				});
			});
		});

		describe('getStatsHistogram', () => {
			it('should get stats histogram (hourly)', async () => {
				const res = await stats.getStatsHistogram({
					from: '2024-01-01T00:00:00+03:00',
					namespace: 'spec',
					period: 'hour',
					to: '2024-01-02T00:00:00+03:00'
				});

				expect(res).toEqual({
					from: '2023-12-31T21:00:00.000Z',
					histogram: {
						'2023-12-31T21:00:00.000Z': {},
						'2023-12-31T22:00:00.000Z': {},
						'2023-12-31T23:00:00.000Z': {},
						'2024-01-01T00:00:00.000Z': {},
						'2024-01-01T01:00:00.000Z': {},
						'2024-01-01T02:00:00.000Z': {},
						'2024-01-01T03:00:00.000Z': {},
						'2024-01-01T04:00:00.000Z': {},
						'2024-01-01T05:00:00.000Z': {},
						'2024-01-01T06:00:00.000Z': {},
						'2024-01-01T07:00:00.000Z': {},
						'2024-01-01T08:00:00.000Z': {},
						'2024-01-01T09:00:00.000Z': {},
						'2024-01-01T10:00:00.000Z': {},
						'2024-01-01T11:00:00.000Z': {},
						'2024-01-01T12:00:00.000Z': {},
						'2024-01-01T13:00:00.000Z': {},
						'2024-01-01T14:00:00.000Z': {},
						'2024-01-01T15:00:00.000Z': {
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
							}
						},
						'2024-01-01T16:00:00.000Z': {},
						'2024-01-01T17:00:00.000Z': {},
						'2024-01-01T18:00:00.000Z': {},
						'2024-01-01T19:00:00.000Z': {},
						'2024-01-01T20:00:00.000Z': {},
						'2024-01-01T21:00:00.000Z': {}
					},
					namespace: 'spec',
					period: 'hour',
					to: '2024-01-01T21:59:59.999Z'
				});
			});

			it('should get stats histogram (daily)', async () => {
				const res = await stats.getStatsHistogram({
					from: '2024-01-01T15:30:00.000Z',
					namespace: 'spec',
					period: 'day',
					to: '2024-01-02T15:30:00.000Z'
				});

				expect(res).toEqual({
					from: '2024-01-01T15:00:00.000Z',
					histogram: {
						'2024-01-01T00:00:00.000Z': {
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
							}
						},
						'2024-01-02T00:00:00.000Z': {
							metrics: {
								nested: {
									deep: {
										value2: 20,
										value3: {
											test: 1
										},
										value1: 10
									}
								},
								value3: {
									test: 1
								},
								value1: 10,
								value2: 20
							}
						}
					},
					namespace: 'spec',
					period: 'day',
					to: '2024-01-02T15:59:59.999Z'
				});
			});

			it('should get stats histogram with gmt offset (daily)', async () => {
				const res = await stats.getStatsHistogram({
					from: '2024-01-01T00:00:00-03:00',
					namespace: 'spec',
					period: 'day',
					to: '2024-01-03T00:00:00-03:00'
				});

				expect(res).toEqual({
					from: '2024-01-01T03:00:00.000Z',
					histogram: {
						'2024-01-01T00:00:00.000Z': {
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
							}
						},
						'2024-01-02T00:00:00.000Z': {
							metrics: {
								nested: {
									deep: {
										value2: 20,
										value3: {
											test: 1
										},
										value1: 10
									}
								},
								value3: {
									test: 1
								},
								value1: 10,
								value2: 20
							}
						},
						'2024-01-03T00:00:00.000Z': {}
					},
					namespace: 'spec',
					period: 'day',
					to: '2024-01-03T03:59:59.999Z'
				});
			});
		});
	});

	describe('hourFloor', () => {
		it('should round time to the start of the hour', () => {
			// @ts-expect-error
			const res = stats.hourFloor('2024-01-01T15:30:45.123Z');
			expect(res.toISOString()).toEqual('2024-01-01T15:00:00.000Z');
		});

		it('should round GMT time to the start of the hour', () => {
			// @ts-expect-error
			const res = stats.hourFloor('2024-01-01T15:30:45.123-03:00');
			expect(res.toISOString()).toEqual('2024-01-01T18:00:00.000Z');
		});

		it('should round time to the start of the hour when no timestamp is provided', () => {
			const now = new Date();
			now.setMinutes(0, 0, 0);

			// @ts-expect-error
			const res = stats.hourFloor();
			expect(res.toISOString()).toEqual(now.toISOString());
		});
	});

	describe('normalizeString', () => {
		it('should normalize string', () => {
			// @ts-expect-error
			expect(stats.normalizeString('Develóper')).toEqual('developer');
			// @ts-expect-error
			expect(stats.normalizeString('Devel  óper')).toEqual('devel-oper');
			// @ts-expect-error
			expect(stats.normalizeString(' tÉst and TéSt ')).toEqual('test-and-test');
			// @ts-expect-error
			expect(stats.normalizeString(' - tÉst - and - TéSt - ')).toEqual('test-and-test');
		});
	});

	describe('put', () => {
		afterEach(async () => {
			await stats.clearStats('spec');
		});

		it('should put', async () => {
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
				namespace: 'spec',
				timestamp: '2024-01-01T15:30:00.000Z'
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				id: '2024-01-01T15:00:00.000Z',
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
