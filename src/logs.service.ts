import getCurrentLine from 'get-current-line';
import Redis from 'ioredis';
import { lastValueFrom } from 'rxjs';
import { map } from 'rxjs/operators';
import { InjectRedis } from '@liaoliaots/nestjs-redis';
import { Injectable } from '@nestjs/common';
import { 
	ClientProxyFactory,
	Transport, 
} from '@nestjs/microservices';
import { 
	Exception,
	ErrorException, 
} from '@nest-datum/exceptions';
import { RegistryService } from './registry.service';

@Injectable()
export class LogsService {
	public serviceName = 'logs';

	constructor(@InjectRedis(process.env.REDIS_REGISTRY_NAMESPACE) private readonly redisRegistry: Redis) {
	}

	/**
	 * Forms the name of the hashtable in radish for the values of the series, 
	 * which store data about all replicas of the current service.
	 * @param {string} name - Service name
	 * @return {string}
	 */
	serviceTypeName(name: string = ''): string {
		return `registry.${name || this.serviceName}`;
	}

	/**
	 * Determine the health of the requested microservice 
	 * and increase "load" parameter.
	 * If the server does not respond, remove it from the pool.
	 * @param {ClientTCP|ClientRedis|ClientNats|ClientMqtt|ClientGrpcProxy|ClientRMQ|ClientKafka} transporter - object created with ClientProxyFactory
	 * @param {object} replicaData - Properties of selected replica
	 * @return {boolean}
	 */
	async transporterConnected(transporter, replicaData): Promise<boolean> {
		try {
			let interval,
				index = 0;

			transporter.connect();

			await (new Promise((resolve, reject) => {
				interval = setInterval(() => {
					if (transporter
						&& transporter['isConnected']) {
						clearInterval(interval);
						resolve(true);
					}
					else if (index >= 10) {
						clearInterval(interval);
						reject(new Error('Service is unavailable'));
					}
					index += 1;
				}, 200);
			}));
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
		}
		return false;
	}

	/**
	 * Decrement the value of "load" property by one. 
	 * The function is used after returning the result of the work of microservices 
	 * as an indicator of the load on the service.
	 * @return {void}
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
	 * @return {Promise}
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

			if (!await this.transporterConnected(transporter, selectedReplicaData)) {
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
	 * @return {Promise}
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

	async emit(payload: Exception, accessToken = 'null') {
		if (payload
			&& typeof payload === 'object'
			&& typeof payload['toLogOptionsData'] === 'function'
			&& typeof payload['getCmd'] === 'function') {
			const service = await this.select('logs');
			
			if (service) {
				const optionsData = (payload.toLogOptionsData() || {})['payload'];
				
				await lastValueFrom(service
					.transporter
					.send({ cmd: payload.getCmd() }, {
						...optionsData,
						accessToken,
					})
					.pipe(map(response => response)));
			}
		}
	}
}
