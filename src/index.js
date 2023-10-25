const { MongoClient, ObjectId } = require('mongodb');
const { dotNotationToObject, queryData, searchData, sortData, isValidDate } = require('@cocreate/utils')
const clients = new Map()


async function dbClient(data) {
    if (data.storageUrl) {
        let client = clients.get(data.storageUrl)
        if (!client && !clients.has(data.storageUrl)) {
            try {
                clients.set(data.storageUrl, client)
                client = await MongoClient.connect(data.storageUrl, { useNewUrlParser: true, useUnifiedTopology: true });
                clients.set(data.storageUrl, client)
            } catch (error) {
                console.error(`${data.organization_id}: storageName ${data.storageName} failed to connect to mongodb`)
                errorHandler(data, error)
                return { status: false }
            }
        }
        return client
    }
    return
}

function send(data) {
    let [method, type] = data.method.split('.')
    // let action = data.method.replace(/\.([a-z])/g, (_, match) => match.toUpperCase());

    if (type === 'database')
        return database(method + type.charAt(0).toUpperCase() + type.slice(1), data)
    if (type === 'array')
        return array(method + type.charAt(0).toUpperCase() + type.slice(1), data)
    if (type === 'object')
        return object(method + type.charAt(0).toUpperCase() + type.slice(1), data)
}

function database(action, data) {
    return new Promise(async (resolve, reject) => {
        let type = 'database'
        let databaseArray = []
        let dataTransferedIn = 0
        let dataTransferedOut = 0

        try {
            const client = await dbClient(data)
            if (!client || client.status === false)
                return data
            if (action == 'readDatabase') {
                const db = client.db().admin();
                // TODO: support if a database name is defined then return the database details and stats
                // else apply filter and return dbs for which each should have stats and details
                // if (data.database) {
                //     const db = client.db(data.organization_id)
                //     const stats = await db.stats()
                // }

                // List all the available databases
                db.listDatabases(function (err, dbs) {
                    dataTransferedIn += getBytes(dbs)

                    for (let database of dbs.databases) {
                        if (data.$filter && data.$filter.query) {
                            let isFilter = queryData(database, data.$filter.query)
                            if (isFilter)
                                databaseArray.push({ database, storage: data.storageName })
                        } else
                            databaseArray.push({ database, storage: data.storageName })
                    }

                    resolve(createData(data, databaseArray, type))
                })
            }
            if (action == 'deleteDatabase') {
                const db = client.db(data.database);
                db.dropDatabase().then(restult => {
                    dataTransferedIn += getBytes(restult)
                    resolve(restult)
                })
            }
        } catch (error) {
            errorHandler(data, error)
            console.log(action, 'error', error);
            resolve(data);
        }

    }, (error) => {
        errorHandler(data, error)
    });
}

function array(action, data) {
    return new Promise(async (resolve, reject) => {
        let type = 'array'
        let arrayArray = [];
        let dataTransferedIn = 0
        let dataTransferedOut = 0

        try {
            const client = await dbClient(data)
            if (!client || client.status === false)
                return data

            if (data.request)
                data.array = data.request

            let databases = data.database;
            if (!Array.isArray(databases))
                databases = [databases]

            let databasesLength = databases.length
            for (let database of databases) {
                const db = client.db(database);

                if (action == 'readCollection') {

                    let { query, sort } = await createFilter(data);

                    db.listCollections().toArray(function (error, result) {
                        if (error)
                            errorHandler(data, error, database)

                        if (result) {
                            dataTransferedIn += getBytes(result)

                            for (let res of result) {
                                if (data.$filter && data.$filter.query) {
                                    let isFilter = queryData(res, data.$filter.query)
                                    if (isFilter)
                                        arrayArray.push({ name: res.name, database, storage: data.storageName })
                                } else
                                    arrayArray.push({ name: res.name, database, storage: data.storageName })
                            }
                        }

                        databasesLength -= 1
                        if (!databasesLength) {
                            data = createData(data, arrayArray, type, dataTransferedIn, dataTransferedOut)
                            resolve(data)
                        }
                    })
                } else {
                    let arrays
                    let value
                    if (action == 'updateCollection')
                        arrays = Object.entries(data.array)
                    else
                        arrays = data.array;

                    if (!Array.isArray(arrays))
                        arrays = [arrays]

                    let arraysLength = arrays.length
                    for (let array of arrays) {

                        if (action == 'createCollection') {
                            dataTransferedOut += getBytes(array)
                            db.createCollection(array, function (error, result) {
                                if (error)
                                    errorHandler(data, error, database, array)

                                if (result) {
                                    dataTransferedIn += getBytes(result)
                                    arrayArray.push({ name: array, database, storage: data.storageName })
                                }

                                arraysLength -= 1
                                if (!arraysLength)
                                    databasesLength -= 1

                                if (!databasesLength && !arraysLength) {
                                    data = createData(data, arrayArray, type, dataTransferedIn, dataTransferedOut)
                                    resolve(data)
                                }
                            })
                        } else {
                            if (action == 'updateCollection') {
                                [array, value] = array
                            }

                            const arrayObj = db.collection(array);

                            if (action == 'updateCollection') {
                                dataTransferedOut += getBytes(result)
                                arrayObj.rename(value, function (error, result) {
                                    if (error)
                                        errorHandler(data, error, database, array)

                                    if (result) {
                                        dataTransferedIn += getBytes(result)
                                        arrayArray.push({ name: value, oldName: array, database, storage: data.storageName })
                                    }
                                    arraysLength -= 1
                                    if (!arraysLength)
                                        databasesLength -= 1

                                    if (!databasesLength && !arraysLength) {
                                        data = createData(data, arrayArray, type, dataTransferedIn, dataTransferedOut)
                                        resolve(data)
                                    }

                                })
                            }

                            if (action == 'deleteCollection') {
                                arrayObj.drop(function (error, result) {
                                    if (error)
                                        errorHandler(data, error, database, array)

                                    if (result) {
                                        dataTransferedOut += getBytes(result)
                                        arrayArray.push({ name: array, database, storage: data.storageName })
                                    }

                                    arraysLength -= 1
                                    if (!arraysLength)
                                        databasesLength -= 1

                                    if (!databasesLength && !arraysLength) {
                                        data = createData(data, arrayArray, type, dataTransferedIn, dataTransferedOut)
                                        resolve(data)
                                    }

                                })

                            }
                        }

                    }
                }
            }

        } catch (error) {
            errorHandler(data, error)
            console.log(action, 'error', error);
            resolve(data);
        }
    }, (error) => {
        errorHandler(data, error)
    });
}

function object(action, data) {
    return new Promise(async (resolve, reject) => {
        try {
            const client = await dbClient(data)
            if (!client || client.status === false)
                return data

            let dataTransferedIn = 0
            let dataTransferedOut = 0
            let type = 'object'
            let documents = [];

            if (data.request)
                data[type] = data.request

            if (!data['timeStamp'])
                data['timeStamp'] = new Date()
            else
                data['timeStamp'] = new Date(data['timeStamp'])

            let databases = data.database;
            if (!Array.isArray(databases))
                databases = [databases]

            for (let database of databases) {
                let arrays = data.array;
                if (!Array.isArray(arrays))
                    arrays = [arrays]

                for (let array of arrays) {
                    const db = client.db(database);
                    const arrayObj = db.collection(array);
                    const reference = { $storage: data.storageName, $database: database, $array: array }

                    if (!data[type])
                        data[type] = []
                    if (data[type] && !Array.isArray(data[type]))
                        data[type] = [data[type]]

                    let isFilter
                    if (data.$filter)
                        isFilter = true
                    if (isFilter && !data[type].length)
                        data[type] = [{}]

                    let filter = await createFilter(data, arrayObj);

                    let projections = {}, projection = {}, update = {}, options = {}

                    if (action === 'updateObject')
                        createUpdate(update, options, data, true)

                    for (let i = 0; i < data[type].length; i++) {
                        delete data[type][i].$storage
                        delete data[type][i].$database
                        delete data[type][i].$array

                        if (action !== 'createObject' && data[type][i].$filter) {
                            isFilter = true
                            reference['$filter'] = data[type][i].$filter
                            filter = await createFilter({ $filter: data[type][i].$filter }, arrayObj)
                        }

                        let { query, sort, index, limit } = filter

                        if (action === 'createObject') {
                            data[type][i] = replaceArray(data[type][i])
                            data[type][i] = dotNotationToObject(data[type][i])
                            data[type][i]['organization_id'] = data['organization_id'];
                            data[type][i]['created'] = { on: data.timeStamp, by: data.user_id || data.clientId }
                        } else if (action === 'readObject') {
                            projection = createProjection(data[type][i])
                        } else if (action === 'updateObject') {
                            data[type][i].modified = { on: data.timeStamp, by: data.user_id || data.clientId }
                            data[type][i].organization_id = data.organization_id
                            createUpdate(update, options, data[type][i])
                        }

                        if (data[type][i]._id || action === 'createObject') {
                            if (action !== 'createObject') {
                                try {
                                    query._id = new ObjectId(data[type][i]._id);
                                } catch (error) {
                                    if (action === 'updateObject' && options.upsert) {
                                        data[type][i]._id = ObjectId()
                                        query._id = data[type][i]._id;
                                    } else {
                                        errorHandler(data, error, database, array)
                                        continue;
                                    }
                                }
                            }

                            try {

                                dataTransferedOut += getBytes({ query, update, projection, options })

                                let result
                                if (action === 'createObject') {
                                    if (data[type][i]._id) {
                                        try {
                                            data[type][i]._id = new ObjectId(data[type][i]._id);
                                        } catch (error) {
                                            delete data[type][i]._id
                                        }
                                    }
                                    result = await arrayObj.insertOne(data[type][i]);
                                    // TODO: type error occuring when pushing the item pushes but throws an error
                                    data[type][i]._id = result.insertedId.toString()
                                    documents.push({ ...data[type][i], ...reference })
                                } else if (action === 'readObject') {
                                    result = await arrayObj.findOne(query, projection);
                                    result._id = result._id.toString()
                                    documents.push({ ...result, ...reference })
                                } else if (action === 'updateObject') {
                                    result = await arrayObj.updateOne(query, update, options);

                                    // TODO: handle upsert false and id does not exist
                                    data[type][i]._id = query._id.toString()
                                    documents.push({ ...data[type][i], ...reference })
                                } else if (action === 'deleteObject') {
                                    result = await arrayObj.deleteOne(query);
                                    documents.push({ ...reference, _id: data[type][i]._id })
                                }
                                dataTransferedIn += getBytes(result)

                            } catch (error) {
                                errorHandler(data, error, database, array)
                            }
                        } else if (isFilter) {
                            try {
                                if (action === 'readObject')
                                    projection = { ...projections, ...projection }

                                delete query._id

                                dataTransferedOut += getBytes({ query, projection, sort, index, limit })
                                let document = ''

                                const cursor = arrayObj.find(query, projection).sort(sort).skip(index).limit(limit);
                                if (!(await cursor.hasNext()) && action === 'updateObject' && data.upsert)
                                    document = { _id: ObjectId(data[type][i]._id) }

                                while (await cursor.hasNext() || document) {
                                    if (!document)
                                        document = await cursor.next();

                                    dataTransferedIn += getBytes(document)

                                    if (data.$filter && data.$filter.search) {
                                        let isMatch = searchData(document, data.$filter.search)
                                        if (!isMatch)
                                            continue;
                                    }

                                    if (action === 'readObject') {
                                        documents.push({ ...document, ...reference, _id: document._id.toString() })
                                    } else {
                                        dataTransferedOut += getBytes({ _id: document._id, update, options })

                                        let result
                                        if (action === 'updateObject') {
                                            if (options.returnNewDocument) {
                                                let object = await arrayObj.findOneAndUpdate({ _id: document._id }, update, options);
                                                for (let key of Object.keys(object)) {
                                                    if (key === '_id')
                                                        continue
                                                    let newArrayKey = options.newArray[key]
                                                    // TODO: get index based on $operator
                                                    let index = object[key].length - 1
                                                    if (index >= 0)
                                                        data[type][i][newArrayKey.replace('[]', `[${index}]`)] = data[type][i][newArrayKey]
                                                }
                                            } else
                                                result = await arrayObj.updateOne({ _id: document._id }, update, options);
                                            // TODO: if update.$push or update.$each read document with projection to get index and update the keys [] to include index
                                        } else if (action === 'deleteObject') {
                                            result = await arrayObj.deleteOne({ _id: document._id });
                                        }

                                        dataTransferedIn += getBytes(result)
                                        documents.push({ ...data[type][i], ...reference, _id: document._id.toString() })
                                        data[type].push({ ...data[type][i], ...reference, _id: document._id.toString() })

                                    }
                                    document = ''
                                }
                            } catch (error) {
                                errorHandler(data, error, database, array)
                            }

                        }
                    }

                    // if (action === 'createObject') {
                    //     try {
                    //         dataTransferedOut += getBytes(data[type])
                    //         const result = await arrayObj.insertMany(data[type]);
                    //         dataTransferedIn += getBytes(result)

                    //         for (let i = 0; i < data[type].length; i++) {
                    //             data[type][i]._id = data[type][i]._id.toString()
                    //         }
                    //     } catch (error) {
                    //         errorHandler(data, error, database, array)
                    //     }
                    // }

                }
            }

            data = createData(data, documents, type, dataTransferedIn, dataTransferedOut)
            resolve(data)

        } catch (error) {
            errorHandler(data, error)
            console.log(action, 'error', error);
            resolve(data);
        }
    }, (error) => {
        errorHandler(data, error)
    });

}

function createUpdate(update, options, data, isGlobal) {
    if (data.upsert)
        options.upsert = data.upsert
    if (data.$upsert)
        options.upsert = data.$upsert

    Object.keys(data).forEach(key => {
        if (isGlobal && !key.startsWith('$') || key === '_id')
            return
        data[key] = isValidDate(data[key])

        let operator
        if (key.endsWith(']')) {
            const regex = /^(.*(?:\[\d+\].*?)?)\[(.*?)\](?:\[\])?$/;
            var match = key.match(regex);
            var index = parseInt(match[2], 10);
            if (index === NaN)
                operator = match[2]
            var arrayKey = match[1].replace(/\[(\d+)\]/g, '.$1');
        }

        if (key.startsWith('$'))
            operator = key.split('.')[0]
        else if (!operator && typeof data[key] === 'string' && data[key].startsWith('$'))
            operator = data[key]

        if (!update['$set'])
            update['$set'] = {}

        let originalKey = key
        key = key.replace(/\[(\d+)\]/g, '.$1');

        if (originalKey.endsWith('[]')) {
            if (!options.projection) {
                options.projection = {}
                options.arrayKey = {}
                options.returnNewDocument = true
            } else {
                options.projection[key.replace(operator + '.', '')] = 1
                options.arrayKey[key.replace(operator + '.', '')] = originalKey
            }
            if (!key.startsWith('$'))
                operator = '$push'
        }

        let operators = ['$rename', '$inc', '$push', '$each', '$splice', '$unset', '$delete', '$slice', '$pop', '$shift', '$addToSet', '$pull', '$currentDate']
        if (!operators.includes(operator) && typeof index !== 'number') {
            if (!isGlobal)
                update['$set'][key] = data[originalKey]
            return
        }

        let updates = {}
        if (operator === '$rename') {
            if (key === '$rename')
                for (let oldkey of Object.keys(data[originalKey])) {
                    key = '$rename.' + oldkey
                    updates[key] = data[originalKey][oldkey].replace(/\[(\d+)\]/g, '.$1')
                }
            else
                updates[key] = data[originalKey].replace(/\[(\d+)\]/g, '.$1')

        } else if (operator === '$delete' || operator === '$unset' || operator === '$slice') {
            operator = '$unset'
            updates[key] = 1
            if (!updates['$pull'])
                updates['$pull'] = {}
            updates['$pull'][key] = null
        } else if (operator === '$pop') {
            key = arrayKey
            updates[key] = index || 1
        } else if (operator === '$addToSet' || operator === '$pull') {
            updates[key] = data[originalKey]
        } else if (operator === '$push' || operator === '$each' || typeof index === 'number') {
            updates[key] = data[originalKey]
            if (typeof index === 'number' && index >= 0) {
                if (operator === '$push')
                    updates[key] = [data[originalKey]]

                let insert = { $each: updates[key] }
                insert.$postion = index
                updates[key] = insert
            }
        } else if (operator === '$inc') {
            updates[key] = data[originalKey]
        } else if (operator === '$currentDate') {
            updates[key] = data[originalKey]
        }

        if (!update[operator])
            update[operator] = {}

        if (key === operator)
            update[operator] = { ...update[operator], ...replaceArray(updates[key]) }
        else
            update[operator][key.replace(operator + '.', '')] = updates[key]
    })
}

async function createFilter(data, arrayObj) {
    let query = {}, sort = {}, index = 0, limit = 0, count

    if (data.$filter) {
        if (data.$filter.query)
            query = createQuery(data.$filter.query);

        if (data.$filter.sort) {
            for (let i = 0; i < data.$filter.sort.length; i++) {
                let direction = data.$filter.sort[i].direction
                if (direction == 'desc' || direction == -1)
                    direction = -1;
                else
                    direction = 1;

                sort[data.$filter.sort[i].key] = direction
            }
        }

        if (arrayObj) {
            count = await arrayObj.estimatedDocumentCount()
            data.$filter.count = count
        }

        if (data.$filter.index)
            index = data.$filter.index
        if (data.$filter.limit)
            limit = data.$filter.limit
        if (limit)
            limit = index + limit;
    }

    if (data['organization_id'])
        query['organization_id'] = { $eq: data['organization_id'] }

    return { query, sort, index, limit, count }
}


// TODO: improve mongodb query to cover many cases
function createQuery(queries) {
    let query = new Object();

    for (let item of queries) {

        if (!item.key)
            continue

        if (item.key == "_id") {
            if (item.value)
                try {
                    item.value = ObjectId(item.value)
                } catch (error) {
                    continue
                }
            else
                continue
        }

        item.value = isValidDate(item.value)

        let key = item.key;
        if (!query[key]) {
            query[key] = {};
        }

        switch (item.operator) {
            case '$includes':
            case 'includes':
                query[key]['$regex'] = item.value;
                break;

            case '$range':
                if (item.value[0] !== null && item.value[1] !== null) {
                    query[key] = { $gte: item.value[0], $lte: item.value[1] };
                } else if (item.value[0] !== null) {
                    query[key] = { $gte: item.value[0] };
                } else if (item.value[1] !== null) {
                    query[key] = { $lte: item.value[1] };
                }
                break;

            case 'equals':
                query[$eq][item.operator] = item.value;
            case '$eq':
            case '$ne':
            case '$lt':
            case '$lte':
            case '$gt':
            case '$gte':
            case '$regex':
                query[key][item.operator] = item.value;
                break;
            case '$in':
            case '$nin':
                if (!Array.isArray(item.value))
                    query[key] = [item.value]
                else
                    query[key] = { $in: item.value }
                break;
            case '$or':
                if (!query[item.operator])
                    query[item.operator] = [{ [key]: item.value }];
                else
                    query[item.operator].push({ [key]: item.value })
                delete query[key]
                break;
            case '$geoWithin':
                try {
                    let value = JSON.parse(item.value);
                    if (item.type) {
                        query[key]['$geoWithin'] = {
                            [item.type]: value
                        }
                    }
                } catch (e) {
                    console.log('geowithin error');
                }
                break;
        }
    }

    //. global search
    //. we have to set indexes in text fields ex: db.chart.createIndex({ "$**": "text" })
    // if (data['searchKey']) {
    //   query["$text"] = {$search: "\"Ni\""};
    // }

    return query;
}

function createProjection(data) {
    let projection = {}

    Object.keys(data).forEach((key) => {
        if (!['_id', 'organization_id'].includes(key) && !key.startsWith('$'))
            projection[key.replace(/\[(\d+)\]/g, '.$1')] = 1
    });

    return projection;
}

function replaceArray(data = {}) {
    let object = {}

    Object.keys(data).forEach((key) => {
        object[key.replace(/\[(\d+)\]/g, '.$1')] = data[key]
    });

    return object;
}

function createData(data, array, type, dataTransferedIn, dataTransferedOut) {
    if (dataTransferedIn) {
        process.emit("setBandwidth", {
            type: 'in',
            data: dataTransferedIn,
            organization_id: data.organization_id
        });
    }

    if (dataTransferedOut) {
        process.emit("setBandwidth", {
            type: 'out',
            data: dataTransferedOut,
            organization_id: data.organization_id
        });
    }

    if (!data.request)
        data.request = data[type] || {}

    data[type] = array

    if (data.returnLog) {
        if (!data.log)
            data.log = []
        data.log.push(...data[type])
    }

    return data
}

function getBytes(data) {
    const jsonString = JSON.stringify(data);
    return Buffer.byteLength(jsonString, 'utf8');
}

function errorHandler(data, error, database, array) {
    if (typeof error == 'object')
        error['storage'] = 'mongodb'
    else
        error = { storage: data.storageName, message: error }

    if (database)
        error['database'] = database
    if (array)
        error['array'] = array
    if (data.error)
        data.error.push(error)
    else
        data.error = [error]
}

module.exports = { send }
