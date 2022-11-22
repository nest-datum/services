import getCurrentLine from 'get-current-line';
import Redis from 'ioredis';
import { InjectRedis } from '@liaoliaots/nestjs-redis';
import { Injectable } from '@nestjs/common';
import { ErrorException } from '@nest-datum/exceptions';

let timeout;

/**
 * Cache manager with storage combinations.
 * @class
 * @classdesc Adding and retrieving data from the cache. Cache clearing algorithm.
 */
@Injectable()
export class CacheService {
	constructor(@InjectRedis(process.env.REDIS_CACHE_NAMESPACE) private readonly redisCache: Redis) {
	}

	/**
	 * Raises a flag indicating the start of the cleanup process.
	 * @param {string} name - The name of the section in the radish to be cleared.
	 */
	takeCleaningProcess(name: string): boolean {
		return (this.redisCache[name] = true);
	}

	/**
	 * Remove the clear flag to end the process and unlock the cache.
	 * @param {string} name - The name of the section in the radish to be cleared.
	 */
	releaseCleaningProcess(name: string) {
		clearTimeout(timeout);

		timeout = setTimeout(() => {
			delete this.redisCache[name];
		}, 1000);

		return timeout;
	}

	/**
	 * Gets all the key in the radish according to the specified pattern.
	 * @param {string} pattern
	 */
	async keys(pattern: string): Promise<any> {
		return await this.redisCache.keys(pattern);
	}

	/**
	 * Recursively clearing all keys in redis with scanStream functions.
	 * @param {string} name - The name of the section in the radish to be cleared.
	 * @param {string|number|undefined} query - The query on which the search pattern is formed.
	 */
	async clear(name: string, query: any|undefined = undefined): Promise<any> {
		this.takeCleaningProcess(name);

		if (query
			&& (typeof query === 'string'
				|| typeof query === 'number')
			&& !Number.isNaN(query)) {
			const key = JSON.stringify(query);
			const value = await this.redisCache.get(`${name}.${key}`);

			if (Array.isArray(value)) {
				throw new Error(`An error occurred while clearing the cache for "${name}" with key "${key}".`);
			}
			await this.redisCache.del(`${name}.${key}`);
		}
		else {
			await (new Promise(async (resolve, reject) => {
				let scanStream = await this.redisCache.scanStream({
					match: `${name}*`,
					count: 60,
				}),
					exitTimeout;

				scanStream.on('data', async (resultKeys) => {
					let i = 0;

					while (i < resultKeys.length) {
						try {
							await this.redisCache.del(resultKeys[i]);
						}
						catch (err) {
							console.error(`cache service delete: ${resultKeys[i]} ${err}.`);
							clearTimeout(exitTimeout);

							return reject(err);
						}
						i++;
					}
				});
				scanStream.on('end', () => {
					clearTimeout(exitTimeout);

					return resolve(true);
				});
				exitTimeout = setTimeout(() => {
					scanStream = undefined;

					return reject(true);
				}, 12000);
			}));
		}

		return this.releaseCleaningProcess(name);
	}

	/**
	 * Get data from cache.
	 * @param {string} name - The name of the section in the radish to be cleared.
	 * @param {string|number|undefined} query - The query on which the search pattern is formed.
	 */
	async get(name: string, query: any): Promise<any> {
		try {
			if (typeof query === 'undefined'
				|| query === null
				|| (typeof query === 'number' && Number.isNaN(query))) {
				throw new Error(`Properties data for redis cache are incorect.`);
			}
			let id,
				output;

			if (typeof query === 'object') {
				if (query['id']
					&& typeof query['id'] === 'string') {
					id = query['id'];
				}
				query = JSON.stringify(query);
			}
			if (!this.redisCache[name]) {
				output = await this.redisCache.get(`${name}.${query}`);
			}
			if (!output && id) {
				output = await this.redisCache.get(`${name}.${id}`);
			}

			return (typeof output === 'undefined'
				|| output === 'undefined'
				|| output === null)
				? undefined
				: JSON.parse(output);
		}
		catch (err) {
			throw new ErrorException(err.message, getCurrentLine(), query);
		}
	}

	/**
	 * Get data from cache.
	 * @param {string} name - The name of the section in the radish to be cleared.
	 * @param {string|number|undefined} query - The query on which the search pattern is formed.
	 * @param {object} data - Data
	 */
	async set(name: string, query: any, data = {}): Promise<any> {
		try {
			if (typeof query === 'undefined'
				|| query === null
				|| (typeof query === 'number' && Number.isNaN(query))) {
				throw new Error(`Properties data for redis cache are incorect.`);
			}
			if (typeof query === 'object') {
				query = JSON.stringify(query);
			}
			return await this.redisCache.set(`${name}.${query}`, JSON.stringify(data));
		}
		catch (err) {
			throw new ErrorException(err.message, getCurrentLine(), query);
		}
	}
}
