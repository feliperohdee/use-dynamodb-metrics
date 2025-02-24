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
			logsTableName: 'use-dynamodb-metrics-logs-spec',
			region: process.env.AWS_REGION || '',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
			sessionTableName: 'use-dynamodb-metrics-sessions-spec',
			statsTableName: 'use-dynamodb-metrics-stats-spec'
		});
	});

	beforeEach(() => {
		stats = new Stats({
			accessKeyId: process.env.AWS_ACCESS_KEY || '',
			createTable: true,
			logsTableName: 'use-dynamodb-metrics-logs-spec',
			region: process.env.AWS_REGION || '',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
			sessionTableName: 'use-dynamodb-metrics-sessions-spec',
			statsTableName: 'use-dynamodb-metrics-stats-spec'
		});
	});

	afterAll(async () => {
		await stats.clear('spec');
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
		it('should generate', () => {
			const metrics = [
				{ key: 'value1', value: 1 },
				{ key: 'value2', value: 20 }
			];

			// @ts-expect-error
			const { attributeNames, attributeValues, updateExpression } = stats.generateUpdateExpression({
				incrementSession: false,
				incrementUniqueUser: false,
				metrics
			});

			expect(attributeNames).toEqual({
				'#h': 'hits',
				'#s': 'sessions',
				'#t': 'ttl',
				'#u': 'uniqueUsers',
				'#m0': 'metrics.value1',
				'#m1': 'metrics.value2'
			});

			expect(attributeValues).toEqual({
				':one': 1,
				':ttl': expect.any(Number),
				':v1': 20,
				':zero': 0
			});

			expect(updateExpression).toEqual('ADD #h :one, #s :zero, #u :zero, #m0 :one, #m1 :v1 SET #t = :ttl');
		});

		it('should generate with [session, uniqueUser]', () => {
			const metrics = [
				{ key: 'value1', value: 1 },
				{ key: 'value2', value: 20 }
			];

			// @ts-expect-error
			const { attributeNames, attributeValues, updateExpression } = stats.generateUpdateExpression({
				incrementSession: true,
				incrementUniqueUser: true,
				metrics
			});

			expect(attributeNames).toEqual({
				'#h': 'hits',
				'#m0': 'metrics.value1',
				'#m1': 'metrics.value2',
				'#s': 'sessions',
				'#t': 'ttl',
				'#u': 'uniqueUsers'
			});

			expect(attributeValues).toEqual({
				':one': 1,
				':ttl': expect.any(Number),
				':v1': 20
			});

			expect(updateExpression).toEqual('ADD #h :one, #s :one, #u :one, #m0 :one, #m1 :v1 SET #t = :ttl');
		});
	});

	describe('fetchLogs / fetchSessions / getStats / getStatsHistogram', () => {
		beforeAll(async () => {
			for (let i = 0; i < 3; i++) {
				const timestamp = new Date('2024-01-01T15:30:00.000Z');
				timestamp.setDate(timestamp.getDate() + (i % 2));

				await stats.put({
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
					session: `session-${i % 2}`,
					timestamp: timestamp.toISOString()
				});
			}
		});

		afterAll(async () => {
			await stats.clear('spec');
		});

		describe('fetchLogs', () => {
			it('should fetch logs', async () => {
				const res = await stats.fetchLogs({
					namespace: 'spec'
				});

				expect(res).toEqual({
					count: 3,
					items: [
						expect.objectContaining({
							id: 'session-0',
							session: 'session-0#1'
						}),
						expect.objectContaining({
							id: 'session-0',
							session: 'session-0#1'
						}),
						expect.objectContaining({
							id: 'session-1',
							session: 'session-1#1'
						})
					],
					lastEvaluatedKey: null
				});
			});

			it('should fetch logs with [session]', async () => {
				const res = await stats.fetchLogs({
					namespace: 'spec',
					session: 'session-0'
				});

				expect(res).toEqual({
					count: 2,
					items: [
						expect.objectContaining({
							id: 'session-0',
							session: 'session-0#1'
						}),
						expect.objectContaining({
							id: 'session-0',
							session: 'session-0#1'
						})
					],
					lastEvaluatedKey: null
				});
			});

			it('should fetch logs with [from, to]', async () => {
				const res = await stats.fetchLogs({
					from: '2024-01-02T15:30:00.000Z',
					namespace: 'spec',
					to: '2024-01-02T15:30:00.000Z'
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});
		});

		describe('fetchSessions', () => {
			it('should fetch sessions', async () => {
				const res = await stats.fetchSessions({
					namespace: 'spec'
				});

				expect(res).toEqual({
					count: 2,
					items: [expect.objectContaining({ id: 'session-0#1' }), expect.objectContaining({ id: 'session-1#1' })],
					lastEvaluatedKey: null
				});
			});

			it('should fetch sessions with [id]', async () => {
				const res = await stats.fetchSessions({
					namespace: 'spec',
					id: 'session-0'
				});

				expect(res).toEqual({
					count: 1,
					items: [expect.objectContaining({ id: 'session-0#1' })],
					lastEvaluatedKey: null
				});
			});

			it('should fetch sessions with [from, to]', async () => {
				const res = await stats.fetchSessions({
					namespace: 'spec',
					from: '2024-01-02T15:30:00.000Z',
					to: '2024-01-02T15:30:00.000Z'
				});

				expect(res).toEqual({
					count: 0,
					items: [],
					lastEvaluatedKey: null
				});
			});
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
					hits: 3,
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
					sessions: 2,
					to: '2024-01-02T15:59:59.999Z',
					uniqueUsers: 2
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
					hits: 0,
					metrics: {},
					namespace: 'spec',
					sessions: 0,
					to: '2024-02-02T15:59:59.999Z',
					uniqueUsers: 0
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
							hits: 2,
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
							sessions: 1,
							uniqueUsers: 1
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
							hits: 2,
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
							sessions: 1,
							uniqueUsers: 1
						},
						'2024-01-02T00:00:00.000Z': {
							hits: 1,
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
							},
							sessions: 1,
							uniqueUsers: 1
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
							hits: 2,
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
							sessions: 1,
							uniqueUsers: 1
						},
						'2024-01-02T00:00:00.000Z': {
							hits: 1,
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
							},
							sessions: 1,
							uniqueUsers: 1
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

	describe('getTtlSeconds', () => {
		it('should get ttl', () => {
			// @ts-expect-error
			const seconds = stats.getTtlSeconds();
			const diffSeconds = seconds - _.now() / 1000;
			const diffSecondsInDays = diffSeconds / (60 * 60 * 24);

			expect(diffSecondsInDays).toBeCloseTo(stats.ttlDays);
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

	describe('isSessionExpired', () => {
		beforeEach(() => {
			vi.spyOn(_, 'now').mockReturnValue(new Date('2024-01-01T12:00:00Z').getTime());
		});

		afterEach(() => {
			vi.clearAllMocks();
		});

		it('should return true when session is expired', () => {
			// Create timestamp for 31 minutes ago (session timeout is 30 min)
			const oldTimestamp = new Date(_.now() - 31 * 60 * 1000).toISOString();
			// @ts-expect-error
			const res = stats.isSessionExpired(oldTimestamp);

			expect(res).toEqual(true);
		});

		it('should return false when session is not expired', () => {
			// Create timestamp for 29 minutes ago (session timeout is 30 min)
			const recentTimestamp = new Date(_.now() - 29 * 60 * 1000).toISOString();

			// @ts-expect-error
			const res = stats.isSessionExpired(recentTimestamp);
			expect(res).toEqual(false);
		});
	});

	describe('isUniqueUser', () => {
		beforeEach(() => {
			vi.spyOn(_, 'now').mockReturnValue(new Date('2024-01-01T12:00:00Z').getTime());
		});

		afterEach(() => {
			vi.clearAllMocks();
		});

		it('should return true when user is considered unique', () => {
			// Create timestamp for 31 minutes ago (uniqueUser timeout is 30 min)
			const oldTimestamp = new Date(_.now() - 31 * 60 * 1000).toISOString();
			// @ts-expect-error
			const res = stats.isUniqueUser(oldTimestamp);

			expect(res).toEqual(true);
		});

		it('should return false when user is not considered unique', () => {
			// Create timestamp for 29 minutes ago (uniqueUser timeout is 30 min)
			const recentTimestamp = new Date(_.now() - 29 * 60 * 1000).toISOString();

			// @ts-expect-error
			const res = stats.isUniqueUser(recentTimestamp);
			expect(res).toEqual(false);
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
		beforeEach(() => {
			// @ts-expect-error
			vi.spyOn(stats, 'putSession');
			vi.spyOn(stats.db.logs, 'put');
		});

		afterEach(async () => {
			await stats.clear('spec');
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

			// @ts-expect-error
			expect(stats.putSession).toHaveBeenCalledWith({
				id: '',
				namespace: 'spec'
			});
			expect(stats.db.logs.put).not.toHaveBeenCalled();

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				id: '2024-01-01T15:00:00.000Z',
				hits: 1,
				'metrics.nested.deep.value1': 10,
				'metrics.nested.deep.value2': 20,
				'metrics.nested.deep.value3.test': 1,
				'metrics.value1': 10,
				'metrics.value2': 20,
				'metrics.value3.test': 1,
				namespace: 'spec',
				sessions: 0,
				ttl: expect.any(Number),
				uniqueUsers: 0
			});
		});

		it('should put with session', async () => {
			const res = await stats.put({
				metrics: {
					value1: 10,
					value2: 20,
					value3: 'test'
				},
				namespace: 'spec',
				session: 'session-id',
				timestamp: '2024-01-01T15:30:00.000Z'
			});

			// @ts-expect-error
			expect(stats.putSession).toHaveBeenCalledWith({
				id: 'session-id',
				namespace: 'spec'
			});

			expect(stats.db.logs.put).toHaveBeenCalledWith({
				__createdAt: expect.any(String),
				__updatedAt: expect.any(String),
				id: expect.stringMatching(/^session-id#1#\d+#\d+$/),
				metrics: expect.any(Object),
				namespace: 'spec',
				session: 'session-id#1'
			});

			expect(res).toEqual({
				__createdAt: expect.any(String),
				__ts: expect.any(Number),
				__updatedAt: expect.any(String),
				id: '2024-01-01T15:00:00.000Z',
				hits: 1,
				'metrics.value1': 10,
				'metrics.value2': 20,
				'metrics.value3.test': 1,
				namespace: 'spec',
				sessions: 1,
				ttl: expect.any(Number),
				uniqueUsers: 1
			});
		});
	});

	describe('putSession', () => {
		beforeEach(() => {
			// @ts-expect-error
			vi.spyOn(stats, 'isSessionExpired').mockReturnValue(false);
			// @ts-expect-error
			vi.spyOn(stats, 'isUniqueUser').mockReturnValue(false);
			vi.spyOn(_, 'now').mockReturnValue(new Date('2024-01-01T12:00:00Z').getTime());
		});

		afterEach(async () => {
			vi.clearAllMocks();
			await stats.clear('spec');
		});

		it('should return empty session info when no id provided', async () => {
			// @ts-expect-error
			const res = await stats.putSession({
				id: '',
				namespace: 'spec'
			});

			expect(res).toEqual({
				incrementSession: false,
				incrementUniqueUser: false,
				session: null
			});
		});

		it('should create a new session when no previous session exists', async () => {
			// @ts-expect-error
			const res = await stats.putSession({
				id: 'session-id',
				namespace: 'spec'
			});

			// @ts-expect-error
			expect(stats.isSessionExpired).not.toHaveBeenCalled();
			// @ts-expect-error
			expect(stats.isUniqueUser).not.toHaveBeenCalled();

			expect(res).toEqual({
				incrementSession: true,
				incrementUniqueUser: true,
				session: {
					__createdAt: expect.any(String),
					__updatedAt: expect.any(String),
					durationSeconds: 0,
					hits: 1,
					id: 'session-id#1',
					index: 1,
					namespace: 'spec',
					ttl: expect.any(Number)
				}
			});
		});

		it('should create a new session when previous session is expired', async () => {
			// @ts-expect-error
			const prev = await stats.putSession({
				id: 'session-id',
				namespace: 'spec'
			});

			// @ts-expect-error
			vi.mocked(stats.isSessionExpired).mockReturnValue(true);

			// @ts-expect-error
			const res = await stats.putSession({
				id: 'session-id',
				namespace: 'spec'
			});

			// @ts-expect-error
			expect(stats.isSessionExpired).toHaveBeenCalled(prev.session?.__updatedAt);
			// @ts-expect-error
			expect(stats.isUniqueUser).toHaveBeenCalled(prev.session?.__updatedAt);

			expect(res).toEqual({
				incrementSession: true,
				incrementUniqueUser: false,
				session: {
					__createdAt: expect.any(String),
					__updatedAt: expect.any(String),
					durationSeconds: 0,
					hits: 1,
					id: 'session-id#2',
					index: 2,
					namespace: 'spec',
					ttl: expect.any(Number)
				}
			});
		});

		it('should update existing session when not expired', async () => {
			// @ts-expect-error
			const prev = await stats.putSession({
				id: 'session-id',
				namespace: 'spec'
			});

			vi.mocked(_.now).mockReturnValue(new Date('2024-01-01T12:00:10Z').getTime());

			// @ts-expect-error
			const res = await stats.putSession({
				id: 'session-id',
				namespace: 'spec'
			});

			// @ts-expect-error
			expect(stats.isSessionExpired).toHaveBeenCalled(prev.session?.__updatedAt);
			// @ts-expect-error
			expect(stats.isUniqueUser).toHaveBeenCalled(prev.session?.__updatedAt);

			expect(res).toEqual({
				incrementSession: false,
				incrementUniqueUser: false,
				session: {
					__createdAt: expect.any(String),
					__updatedAt: expect.any(String),
					durationSeconds: 10,
					hits: 2,
					id: 'session-id#1',
					index: 1,
					namespace: 'spec',
					ttl: expect.any(Number)
				}
			});
		});

		it('should handle unique user flag', async () => {
			// @ts-expect-error
			vi.mocked(stats.isUniqueUser).mockReturnValue(true);

			// @ts-expect-error
			const res = await stats.putSession({
				id: 'session-id',
				namespace: 'spec'
			});

			expect(res.incrementUniqueUser).toEqual(true);
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
