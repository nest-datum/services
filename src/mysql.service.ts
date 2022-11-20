const mergeDeep = require('merge-deep');

import {  
	Not,
	LessThan,
	LessThanOrEqual,
	MoreThan,
	MoreThanOrEqual,
	Equal,
	Like,
	ILike,
	Between,
	In,
	Any,
	IsNull,
} from 'typeorm';

export class MysqlService {
	protected whereOperators = {
		'$Not': Not,
		'$LessThan': LessThan,
		'$LessThanOrEqual': LessThanOrEqual,
		'$MoreThan': MoreThan,
		'$MoreThanOrEqual': MoreThanOrEqual,
		'$Equal': Equal,
		'$Like': Like,
		'$ILike': ILike,
		'$Between': Between,
		'$In': In,
		'$Any': Any,
		'$IsNull': IsNull,
	};
	protected selectDefaultMany = {};
	protected queryDefaultMany = {};

	buildWhere(filter = {}, where = {}) {
		let key;

		for (key in (filter || {})) {
			let i = 0,
				filterKey = key,
				filterValue = filter[key];

			if (Array.isArray(filterValue)) {
				where[filterKey] = (this.whereOperators[filterValue[0]])
					? this.whereOperators[filterValue[0]](filterValue.slice(1))
					: filterValue;
			}
			else if (typeof filterValue === 'object') {
				where[filterKey] = this.buildWhere(filterValue);
			}
			else {
				where[filterKey] = filterValue;
			}
		}
		return where;
	}

	buildQueryDeep(query = '', querySelect = {}, where = {}) {
		let key;

		for (key in (querySelect || {})) {
			where[key] = (typeof querySelect[key] === 'object'
				&& querySelect[key]
				&& !Array.isArray(querySelect[key])
				&& Object.keys(querySelect[key]).length > 0)
				? this.buildQueryDeep(query, querySelect[key])
				: Like(`%${query}%`);
		}
		return where;
	}

	buildQuery(query = '', querySelect = {}, where = []) {
		let key;

		for (key in (querySelect || {})) {
			where.push({ 
				[key]: (typeof querySelect[key] === 'object'
					&& querySelect[key]
					&& !Array.isArray(querySelect[key])
					&& Object.keys(querySelect[key]).length > 0)
					? this.buildQueryDeep(querySelect[key], query)
					: Like(`%${query}%`), 
			});
		}
		return where;
	}

	buildRelationsByFilter(filter = {}, collect = {}) {
		let key;

		for (key in (filter || {})) {
			let i = 0,
				filterKey = key,
				filterValue = filter[key];

			if (filterValue
				&& typeof filterValue === 'object'
				&& !Array.isArray(filterValue)) {
				let childKey,
					childFlag = false;

				for (childKey in filterValue) {
					if (filterValue[childKey]
						&& typeof filterValue[childKey] === 'object'
						&& !Array.isArray(filterValue[childKey])) {
						childFlag = true;

						break;
					}
				}
				collect[filterKey] = childFlag
					? this.buildRelationsByFilter(filterValue)
					: true;
			}
		}
		return collect;
	}

	buildRelations(relations, filter) {
		const relationsByFilter = this.buildRelationsByFilter(filter);

		if (relations
			&& typeof relations === 'object'
			&& !Array.isArray(relations)) {
			return mergeDeep(relationsByFilter || {}, relations);
		}
		return relationsByFilter;
	}

	buildPagination(page: number, limit: number) {
		limit = (!Number.isNaN(limit) && limit >= 0)
			? Number(limit)
			: undefined;

		const skip = (page > 0)
			? ((page - 1) * limit)
			: 0;

		return {
			skip,
			...(!Number.isNaN(limit) && limit >= 0)
				? { take: limit }
				: {},
		};
	}

	buildOrder(sort = {}) {
		return (Object.keys(sort || {}).length > 0)
			? sort
			: (this.selectDefaultMany['createdAt']
				? { createdAt: 'DESC' }
				: undefined);
	}

	async findMany({ 
		page = 1, 
		limit = 20, 
		query, 
		filter, 
		sort, 
		relations,
	}): Promise<any> {
		const relationsPrepared = this.buildRelations(relations, filter);
		const wherePrepared = this.buildWhere(filter);
		const order = this.buildOrder(sort);
		let where;

		if (query) {
			where = this.buildQuery(query, this.queryDefaultMany);
			where = where.map((item) => ({
				...item,
				...wherePrepared,
			}));
		}
		
		return {
			select: this.selectDefaultMany,
			...(Object.keys(relationsPrepared).length > 0)
				? { relations: relationsPrepared }
				: {},
			...((Array.isArray(where) && where.length > 0)
				|| (Object.keys(wherePrepared).length > 0))
				? { where: where || wherePrepared }
				: {},
			...(Object.keys(order).length > 0)
				? { order }
				: {},
			...this.buildPagination(page, limit),
		};
	}

	async findOne({
		relations,
		id,
	}): Promise<any> {
		const filter = { id };
		const relationsPrepared = this.buildRelations(relations, filter);
		const where = this.buildWhere(filter);

		return {
			select: this.selectDefaultMany,
			...(Object.keys(relationsPrepared).length > 0)
				? { relations: relationsPrepared }
				: {},
			...(Object.keys(where).length > 0)
				? { where }
				: {},
		};
	}

	async dropByIsDeleted(repository, id): Promise<any> {
		const entity = await repository.findOne({
			select: {
				id: true,
				isDeleted: true,
			},
			where: {
				id,
			},
		});

		if (entity['isDeleted']) {
			await repository.delete({ id });
		}
		else {
			await repository.update({ id }, { isDeleted: true });
		}
		return true;
	}

	async updateWithId(repository, payload): Promise<any> {
		const id = payload['id'];

		if (payload['newId']) {
			payload['id'] = payload['newId'];
			delete payload['newId'];
		}
		delete payload['user'];

		return await repository.update({ id }, payload);
	}
}
