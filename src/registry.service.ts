const { exec } = require('child_process');

import Redis from 'ioredis';
import getCurrentLine from 'get-current-line';
import { v4 as uuidv4 } from 'uuid';
import { lastValueFrom } from 'rxjs';
import { map } from 'rxjs/operators';
import { InjectRedis } from '@liaoliaots/nestjs-redis';
import {
	Injectable,
	Logger,
} from '@nestjs/common';
import { 
	ClientProxyFactory,
	Transport, 
} from '@nestjs/microservices';
import { 
	ErrorException,
	NotFoundException, 
	NotificationException,
} from '@nest-datum/exceptions';
import { generateTokens } from '@nest-datum/jwt';
import * as Validators from '@nest-datum/validators';
import { LogsService } from './logs.service';
import { CacheService } from './cache.service';

let dropTimeout;

/**
 * A service that manages the registration of services and the transport of data between them.
 * @class
 * @classdesc The class describes the functions of a load balancer, adding a service to Redis at application startup, as well as CRUD operations for managing existing replicas in Redis.
 */
@Injectable()
export class RegistryService {
	/**
	 * Current service name.
	 * @defaultValue `registry`
	 */
	protected serviceName: string = 'registry';

	constructor(
		@InjectRedis(process.env.REDIS_REGISTRY_NAMESPACE) private readonly redisRegistry: Redis,
		private readonly logsService: LogsService,
		private readonly cacheService: CacheService,
	) {
	}
	
	/**
	 * Forms the name of the hashtable in radish for the values of the series, 
	 * which store data about all replicas of the current service.
	 * @param {string} name - Service name
	 */
	serviceTypeName(name: string = ''): string {
		return `registry.${name || this.serviceName}`;
	}

	/**
	 * Set service name when registering it in redis
	 * @param {string} name
	 */
	setServiceName(name: string = ''): string {
		this.serviceName = name;

		return name;
	}

	/**
	 * Checking the server's health and increasing the load parameter by one. 
	 * If the service is down, 
	 * then remove it from the radish (only if NODE_ENV is not development).
	 * @param {transporter} ClientRedis|ClientNats|ClientMqtt|ClientGrpcProxy|ClientKafka|ClientTCP
	 * @param {replicaData} object
	 */
	async transporterConnected(transporter, replicaData): Promise<boolean> {
		try {
			await transporter.connect();
			await this.redisRegistry.hmset(this.serviceTypeName(replicaData['name']), replicaData['id'], JSON.stringify({
				...replicaData,
				load: replicaData['load'] + 1,
			}));

			return true;
		}
		catch (err) {
			if (process.env.NODE_ENV !== 'development') {
				await this.redisRegistry.hdel(this.serviceTypeName(replicaData['name']), replicaData['id']);
			}
			return false;
		}
	}

	/**
	 * Decreases the value of load parameter by one.
	 */
	async clearResources(): Promise<any> {
		const data = await this.redisRegistry.hmget(this.serviceTypeName(), process.env.APP_ID);
		if (data[0]) {
			const dataParsed = JSON.parse(data[0]);
			const newLoad = Number(dataParsed.load) - 1;
			
			await this.redisRegistry.hmset(this.serviceTypeName(), process.env.APP_ID, JSON.stringify({
				...dataParsed,
				load: (newLoad < 0
					|| Number.isNaN(newLoad))
					? 0
					: newLoad,
				updatedAt: Date.now(),
			}));
		}
	}

	/**
	 * Specifies a less loaded service from the list of transmitted replica objects. 
	 * The replica is selected relative to the minimum value of the load parameter.
	 * When trying to connect to the service, the functionality of the replica is checked. 
	 * In case of incorrect operation, the replica is removed from the pool, 
	 * and the balancer proceeds to search for the next matching replica.
	 * @param {Array} replicas - List of microservice replicas to select the optimal one
	 * @throws Will throw an error if replica is invalid or not found.
	 */
	async loadBalancer(replicas: object): Promise<any> {
		let id,
			load,
			selectedReplicaData = {};

		for (id in replicas) {
			const replicaData = JSON.parse(replicas[id].toString());

			replicaData['id'] = id;

			if (replicaData['load'] === 0) {
				selectedReplicaData = replicaData;
				break;
			}
			if (replicaData['load'] < load
				|| typeof load === 'undefined') {
				selectedReplicaData = replicaData;
			}
		}
		if (selectedReplicaData['host'] 
			&& selectedReplicaData['port']
			&& (selectedReplicaData['host'] !== process.env.TRANSPORT_HOST
				|| selectedReplicaData['port'] != process.env.TRANSPORT_PORT)) {
			const transport = process.env.TRANSPORT_PROVIDER;

			const transporter = ClientProxyFactory.create({
				transport: Transport[transport],
				options: {
					host: selectedReplicaData['host'],
					port: Number(selectedReplicaData['port']),
				},
			});

			if (!this.transporterConnected(transporter, selectedReplicaData)) {
				delete replicas[selectedReplicaData['id']];

				return await this.loadBalancer(replicas);
			}

			return {
				transporter,
				...selectedReplicaData,
			};
		}
		else {
			return undefined;
		}
		throw new ErrorException(`Replica "${selectedReplicaData['id']}": "${selectedReplicaData['name']}" is invalid or not found.`, getCurrentLine(), selectedReplicaData);
	}

	/**
	 * Choosing the optimal server for subsequent interaction with it. 
	 * Getting data about the service and the object for transport logic.
	 * @param {string} name - Service name
	 */
	async select(name: string): Promise<any> {
		const data = await this.redisRegistry.hgetall(this.serviceTypeName(name));

		if (data
			&& typeof data === 'object'
			&& (Array.isArray(data)
				? (data.length > 0)
				: (Object.keys(data).length > 0))) {
			return await this.loadBalancer(data);
		}
	}

	/**
	 * Sending data to another service.
	 * @param {string} name - Service name
	 * @param {string} cmd - Command name
	 * @param {object} payload - Data
	 * @throws Will throw an error if the request is invalid or the service is not working correctly.
	 */
	async send(name: string, cmd: string, payload: object): Promise<any> {
		const transporter = (await this.select(name) || {}).transporter;

		if (transporter) {
			if (!payload['accessToken']) {
				payload['accessToken'] = (await generateTokens({
					id: process.env.APP_ID,
					email: `${process.env.APP_ID}@nest-datum.com`,
					roleId: 'admin',
				}))['accessToken'];
			}

			const response = await lastValueFrom(transporter
				.send({ cmd }, payload)
				.pipe(map(response => response)));

			if (response['exceptionType']) {
				switch (response['exceptionType']) {
					case 'notFound':
						throw new NotFoundException(response['message'], getCurrentLine(), { name, cmd, payload });
					default:
						throw new ErrorException(response['message'], getCurrentLine(), { name, cmd, payload });
				}
			}
			return response;
		}
		throw new NotFoundException(`Service not found.`, getCurrentLine(), { name, cmd, payload });
	}

	/**
	 * Processing a request to get a list of data of the current entity 
	 * (with the possibility of pagination, search, filtering, sorting).
	 * @param {object} payload - Incoming request object.
	 * @throws Will throw an error if the request is invalid or the database is not working correctly.
	 */
	async many(payload): Promise<any> {
		try {
			const cachedData = await this.cacheService.get(`${process.env.APP_ID}.registry.many`, payload);

			if (cachedData) {
				return cachedData;
			}

			let i = 0,
				scanData,
				total = 0,
				output = [];

			while (i < payload['page']) {
				scanData = await this.redisRegistry.scan(i, 'MATCH', 'registry.*', 'COUNT', payload['limit']);
				total += scanData[1].length;
				i++;
			}

			if (!(Number(scanData[0]) >= 0)
				|| !Array.isArray(scanData[1])) {
				throw new Error(`Redis registry scan failed.`);
			}
			i = 0;

			while (i < scanData[1].length) {
				const hgetallData = await this.redisRegistry.hgetall(scanData[1][i].toString());
				let replicaId,
					replicas = [];

				for (replicaId in hgetallData) {
					replicas.push(JSON.parse(hgetallData[replicaId]));
				}
				output.push({
					name: replicas[0]['name'],
					replicas,
				});
				i++;
			}
			await this.cacheService.set(`${process.env.APP_ID}.registry.many`, payload, [ output, total ]);

			return [ output, total ];

		}
		catch (err) {
			throw new ErrorException(err.message, getCurrentLine(), payload);
		}
	}

	/**
	 * Get service name by replica id
	 * @param {string} id
	 */
	private async serviceTypeNameById(id: string): Promise<string> {
		const manyData = await this.many({
			page: 1,
			limit: 99999,
		});

		if (Array.isArray(manyData[0])
			&& manyData[0].length >= 0) {

			return ((manyData[0]
				.filter((manyDataItem) => (manyDataItem['replicas'] || [])
					.filter((manyDataItemReplica) => manyDataItemReplica['id'] === id)
					.length > 0))[0] || {})['name'];
		}
		throw new NotFoundException(`Service with id does not exist.`, getCurrentLine(), { id });
	}

	/**
	 * Get one model of current entity by payload request object.
	 * @param {object} payload - Incoming request object.
	 * @throws Will throw an error if the request is invalid or the database is not working correctly.
	 */
	async one(payload): Promise<any> {
		try {
			const cachedData = await this.cacheService.get(`${process.env.APP_ID}.registry.one`, payload);

			if (cachedData) {
				return cachedData;
			}
			const serviceTypeName = await this.serviceTypeNameById(payload['id']);
			const data = await this.redisRegistry.hmget(this.serviceTypeName(serviceTypeName), payload['id']);
			const dataParsed = JSON.parse(data[0]);
			let output = dataParsed;
			
			if (payload['select']
				&& typeof payload['select'] === 'object') {
				output = {};

				let selectKeys = (Array.isArray(payload['select']))
					? payload['select']
					: Object.keys(payload['select']);

				Object
					.keys(dataParsed)
					.filter((key) => selectKeys.includes(key))
					.forEach((key) => (output[key] = dataParsed[key]));
			}
			if (typeof output !== 'undefined'
				&& output !== null) {
				await this.cacheService.set(`${process.env.APP_ID}.registry.one`, payload, output);
			
				return output;
			}
			throw new NotFoundException(`"Service with id "${payload['id']}" does not exist."`, getCurrentLine(), payload);
		}
		catch (err) {
			throw ((err.__proto__.constructor.name === 'NotFoundException')
				? err
				: (new ErrorException(err.message, getCurrentLine(), payload)));
		}
	}

	/**
	 * Drop model of current entity by payload request object.
	 * @param {object} payload - Incoming request object.
	 * @throws Will throw an error if the request is invalid or the database is not working correctly.
	 */
	async drop(payload): Promise<any> {
		try {
			await this.cacheService.clear(`${process.env.APP_ID}.registry.many`);
			await this.cacheService.clear(`${process.env.APP_ID}.registry.one`, payload);

			const serviceTypeName = await this.serviceTypeNameById(payload['id']);

			await this.redisRegistry.hdel(this.serviceTypeName(serviceTypeName), payload['id']);

			/*clearTimeout(dropTimeout);

			dropTimeout = setTimeout(() => {
				new Promise((resolve, reject) => {
					exec(`pm2 stop ${process.env.APP_ID}`, (err, stdout, stderr) => {
						if (err) {
							return reject(new Error(err.message));
						}
						if (stderr) {
							return reject(new Error(stderr));
						}
						console.log(`Service stopped! ${stderr}`);
					});
				});
			}, 5000);*/

			return true;
		}
		catch (err) {
			throw new ErrorException(err.message, getCurrentLine(), payload);
		}
	}

	/**
	 * Create model of current entity by payload request object.
	 * @param {object} payload - Incoming request object.
	 * @throws Will throw an error if the request is invalid or the database is not working correctly.
	 */
	async create(payload) {
		try {
			await this.cacheService.clear(`${process.env.APP_ID}.registry.many`);

			const id = payload['id'] || uuidv4();
			const data = {
				...payload,
				id,
				createdAt: Date.now(),
				load: 0,
			};

			await this.redisRegistry.hmset(this.serviceTypeName(payload['name']), id, JSON.stringify(data));
			await this.logsService.emit(new NotificationException(`New service registered "${payload['name']} - ${payload['host']}:${payload['port']}".`, payload));

			return data;
		}
		catch (err) {
			throw new ErrorException(err.message, getCurrentLine(), payload);
		}
	}

	/**
	 * Create model of current entity by payload request object.
	 * @param {object} payload - Incoming request object.
	 * @throws Will throw an error if the request is invalid or the database is not working correctly.
	 */
	async update(payload): Promise<any> {
		try {
			await this.cacheService.clear(`${process.env.APP_ID}.registry.many`);
			await this.cacheService.clear(`${process.env.APP_ID}.registry.one`, payload);

			const data = await this.one({ id: payload['id'] });
			const serviceTypeName = await this.serviceTypeNameById(payload['id']);
			const output = { ...data };

			if (payload['name'] 
				&& payload['name'] !== data['name']) {
				await this.drop({ id: payload['id'] });
			}
			if (payload['newId']) {
				data['id'] = payload['newId'];
				data['prevId'] = data['id'];

				await this.drop({ id: payload['id'] });
				delete payload['id'];
			}

			Object
				.keys(output)
				.forEach((key) => payload[key]
					&& (data[key] = payload[key]));

			await this.redisRegistry.hmset(this.serviceTypeName(data['name']), data['id'], JSON.stringify({
				...data,
				updatedAt: Date.now(),
			}));
			
			if (payload['newId']) {
				await this.logsService.emit(new NotificationException(`Identifier changed to "${payload['newId']}" in replica "${payload['host']}:${payload['port']}" for service "${payload['name']}".`, payload));
			}
			return data;
		}
		catch (err) {			
			throw new ErrorException(err.message, getCurrentLine(), payload);
		}
	}

	/**
	 * Adds a new entry with information about the current replica 
	 * to the service registry in Redis.
	 */
	async start() {
		try {
			const name = Validators.str('name', process.env.APP_NAME, {
				isRequired: true,
				min: 1,
				max: 255,
			});
			const host = Validators.host('host', process.env.TRANSPORT_HOST, {
				isRequired: true,
			});
			const port = Validators.int('port', process.env.TRANSPORT_PORT, {
				isRequired: true,
				min: 2,
				max: 99999,
			});
			const mysqlMasterHost = Validators.host('host', process.env.MYSQL_MASTER_HOST);
			const mysqlMasterPort = Validators.int('port', process.env.MYSQL_MASTER_PORT, {
				min: 2,
				max: 99999,
			});
			const id = Validators.str('id', process.env.APP_ID, { min: 1 }) || uuidv4();

			process.env['APP_ID'] = id;

			await this.create({
				id,
				name,
				host,
				port,
				...(mysqlMasterHost
					&& mysqlMasterPort)
					? {
						mysqlMasterHost,
						mysqlMasterPort,
					}
					: {},
				transport: process.env.TRANSPORT_PROVIDER,
				user: {
					manually: true,
				},
				isStart: true,
			});
		}
		catch (err) {
			console.error(err.message);
		}
	}

	/**
	 * 
	 */
	async getRoleAdmin(): Promise<object> {
		return {
			name: 'Admin',
		};
	}
}
