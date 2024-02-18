const { MongoClient, ObjectId } = require('mongodb');
const { dotNotationToObject, queryData, searchData, sortData, isValidDate } = require('@cocreate/utils')
const clients = new Map()
const organizations = {}

async function dbClient(data) {
    if (data.storageUrl) {
        if (!organizations[data.organization_id])
            organizations[data.organization_id] = {}
        try {
            if (!organizations[data.organization_id][data.storageUrl])
                organizations[data.organization_id][data.storageUrl] = MongoClient.connect(data.storageUrl, { useNewUrlParser: true, useUnifiedTopology: true })

            organizations[data.organization_id][data.storageUrl] = await organizations[data.organization_id][data.storageUrl]
            return organizations[data.organization_id][data.storageUrl]

        } catch (error) {
            console.error(`${data.organization_id}: storageName ${data.storageName} failed to connect to mongodb`)
            errorHandler(data, error)
            return { status: false }
        }
    }

    errorHandler(data, 'missing StorageUrl')
    return
}

function send(data) {
    let [type, method] = data.method.split('.')
    if (type === 'database')
        return database(method, data)
    if (type === 'array')
        return array(method, data)
    if (type === 'object')
        return object(method, data)
}

function database(method, data) {
    return new Promise(async (resolve, reject) => {
        let type = 'database'
        let databaseArray = []
        let dataTransferedIn = 0
        let dataTransferedOut = 0

        try {
            const client = await dbClient(data)
            if (!client || client.status === false)
                return data
            if (method == 'read') {
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
            if (method == 'delete') {
                const db = client.db(data.database);
                db.dropDatabase().then(restult => {
                    dataTransferedIn += getBytes(restult)
                    resolve(restult)
                })
            }
        } catch (error) {
            errorHandler(data, error)
            console.log(method, 'error', error);
            resolve(data);
        }

    }, (error) => {
        errorHandler(data, error)
    });
}

function array(method, data) {
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

                if (method == 'read') {

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
                    if (method == 'update')
                        arrays = Object.entries(data.array)
                    else
                        arrays = data.array;

                    if (!Array.isArray(arrays))
                        arrays = [arrays]

                    let arraysLength = arrays.length
                    for (let array of arrays) {

                        if (method == 'create') {
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
                            if (method == 'update') {
                                [array, value] = array
                            }

                            const arrayObj = db.collection(array);

                            if (method == 'update') {
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

                            if (method == 'delete') {
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
            console.log(method, 'error', error);
            resolve(data);
        }
    }, (error) => {
        errorHandler(data, error)
    });
}

function object(method, data) {
    return new Promise(async (resolve, reject) => {
        try {
            const client = await dbClient(data)

            if (!client || client.status === false)
                return data

            let dataTransferedIn = 0
            let dataTransferedOut = 0
            let type = 'object'
            let documents = [];

            if (!data['timeStamp'])
                data['timeStamp'] = new Date().toISOString();

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
                    else if (typeof data[type] === 'string')
                        data[type] = [{ _id: data[type] }]
                    else if (!Array.isArray(data[type]))
                        data[type] = [data[type]]

                    let isFilter
                    if (data.$filter)
                        isFilter = true
                    if ((isFilter && !data[type].length) || data.isFilter)
                        data[type].splice(0, 0, { isFilter: 'isEmptyObjectFilter' });

                    let filter = await createFilter(data, arrayObj);

                    let projections = {}, projection = {}, update = {}, options = {}

                    if (method === 'update')
                        createUpdate(update, options, data, true)

                    for (let i = 0; i < data[type].length; i++) {
                        let $storage = data[type][i].$storage || []
                        let $database = data[type][i].$database || []
                        let $array = data[type][i].$array || []

                        if (!Array.isArray($storage))
                            $storage = [data[type][i].$storage]
                        if (!Array.isArray($database))
                            $database = [data[type][i].$database]
                        if (!Array.isArray($array))
                            $array = [data[type][i].$array]

                        $storage.push(data.storageName)
                        $database.push(database)
                        $array.push(array)

                        delete data[type][i].$storage
                        delete data[type][i].$database
                        delete data[type][i].$array

                        if (method !== 'create' && data[type][i].$filter) {
                            isFilter = true
                            reference['$filter'] = data[type][i].$filter
                            filter = await createFilter({ $filter: data[type][i].$filter }, arrayObj)
                        }

                        let { query, sort, index, limit } = filter

                        if (method === 'create') {
                            data[type][i] = replaceArray(data[type][i])
                            data[type][i] = dotNotationToObject(data[type][i])
                            data[type][i]['organization_id'] = data['organization_id'];
                            data[type][i]['created'] = { on: new Date(data.timeStamp), by: data.user_id || data.clientId }
                        } else if (method === 'read') {
                            projection = createProjection(data[type][i])
                        } else if (method === 'update') {
                            if (!data[type][i].modified)
                                data[type][i].modified = { on: new Date(data.timeStamp), by: data.user_id || data.clientId }
                            else
                                data[type][i].modified.on = new Date(data[type][i].modified.on)

                            data[type][i].organization_id = data.organization_id
                            createUpdate(update, options, data[type][i])
                        }

                        if (data[type][i]._id || method === 'create') {
                            if (method !== 'create') {
                                try {
                                    query._id = new ObjectId(data[type][i]._id);
                                } catch (error) {
                                    if (method === 'update' && options.upsert) {
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
                                if (method === 'create') {
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
                                    // documents.push({ ...data[type][i], ...reference })
                                } else if (method === 'read') {
                                    result = await arrayObj.findOne(query, projection);
                                    result._id = result._id.toString()

                                    if (data[type][i].$storage && data[type][i].modified && data[type][i].modified.on) {
                                        if (!result) {
                                            result = await arrayObj.insertOne(data[type][i]);
                                            data[type][i]._id = result.insertedId.toString()
                                        } else if (result && new Date(data[type][i].modified.on) > new Date(result.modified.on)) {
                                            data[type][i] = { ...result, ...data[type][i] }
                                            createUpdate(update, options, data[type][i])
                                            result = await arrayObj.updateOne(query, update, options);
                                        } else
                                            data[type][i] = { ...data[type][i], ...result }
                                    } else
                                        data[type][i] = { ...data[type][i], ...result }
                                } else if (method === 'update') {
                                    result = await arrayObj.updateOne(query, update, options);

                                    // TODO: handle upsert false and id does not exist
                                    data[type][i]._id = query._id.toString()
                                    // documents.push({ ...data[type][i], ...reference })
                                } else if (method === 'delete') {
                                    result = await arrayObj.deleteOne(query);
                                    // documents.push({ ...reference, _id: data[type][i]._id })
                                }

                                data[type][i].$storage = $storage
                                data[type][i].$database = $database
                                data[type][i].$array = $array

                                dataTransferedIn += getBytes(result)

                            } catch (error) {
                                errorHandler(data, error, database, array)
                            }
                        } else if (isFilter) {
                            try {
                                // TODO: index is 1 if indexeddb already returned an item interfering with query
                                // if (data.array === "keys")
                                //     index = 0

                                if (method === 'read')
                                    projection = { ...projections, ...projection }

                                dataTransferedOut += getBytes({ query, projection, sort, index, limit })

                                let document = ''
                                const cursor = arrayObj.find(query, projection).sort(sort).skip(index).limit(limit);
                                if (!(await cursor.hasNext()) && method === 'update' && data.upsert)
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

                                    if (method === 'read') {
                                        document._id = document._id.toString()
                                        let object = data[type].find(obj => obj._id && obj._id.toString() === document._id.toString())
                                        if (object) {
                                            if (object.$storage && object.modified && object.modified.on) {
                                                if (document && document.modified && new Date(object.modified.on) > new Date(document.modified.on)) {
                                                    object = { ...document, ...object }
                                                    createUpdate(update, options, object)
                                                    document = await arrayObj.updateOne(query, update, options);
                                                    dataTransferedIn += getBytes(document)
                                                } else
                                                    documents.push({ ...document, ...reference })
                                            } else
                                                documents.push({ ...document, ...reference })

                                        } else
                                            documents.push({ ...document, ...reference })
                                    } else {
                                        dataTransferedOut += getBytes({ _id: document._id, update, options })

                                        let result
                                        if (method === 'update') {
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
                                        } else if (method === 'delete') {
                                            result = await arrayObj.deleteOne({ _id: document._id });
                                        }

                                        dataTransferedIn += getBytes(result)
                                        documents.push({ ...data[type][i], ...reference, _id: document._id.toString() })
                                        // data[type].push({ ...data[type][i], ...reference, _id: document._id.toString() })

                                    }
                                    document = ''
                                }
                            } catch (error) {
                                errorHandler(data, error, database, array)
                            }

                        }
                    }

                    // if (method === 'create') {
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
            console.log(method, 'error', error);
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
    if (data.$update)
        delete data.$update

    Object.keys(data).forEach(key => {
        if (isGlobal && !key.startsWith('$') || key === '_id')
            return

        if (isValidDate(data[key]))
            data[key] = new Date(data[key])

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
            if (key.endsWith('[]'))
                key = key.replace('[]', '');
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
                if (!operator)
                    operator = "$set"
                else {
                    if (operator === '$push')
                        updates[key] = [data[originalKey]]


                    let insert = { $each: updates[key] }
                    insert.$postion = index
                    if (index >= 0)
                        updates[arrayKey] = insert
                    else
                        updates[key] = insert
                }
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
            query = data.$filter.query

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
    }

    if (data['organization_id'])
        query['organization_id'] = data['organization_id']

    return { query, sort, index, limit, count }
}

function parseRegExp(regExpString) {
    let matches = regExpString.match(/\/(.*)\/(.*)/);
    return {
        pattern: matches[1],
        options: matches[2]
    };
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

    if (data[type] && data[type][0] && data[type][0].isFilter === 'isEmptyObjectFilter') {
        data[type].shift()
        data.isFilter = true
    }

    // if (!data.request)
    //     data.request = data[type] || {}

    let key = '_id'
    if (type !== 'object')
        key = 'name'

    // TODO: handle case where data[type] is not an array
    if (!Array.isArray(data[type]))
        console.log('data[type] is not an array', type)
    else {
        for (let i = 0; i < array.length; i++) {
            const matchIndex = data[type].findIndex((item) => item[key] === array[i][key]);
            if (matchIndex !== -1) {
                if (!data[type][matchIndex].$storage)
                    data[type][matchIndex].$storage = []
                if (!data[type][matchIndex].$database)
                    data[type][matchIndex].$database = []
                if (!data[type][matchIndex].$array)
                    data[type][matchIndex].$array = []
                data[type][matchIndex].$storage.push(array[i].$storage)
                delete array[i].$storage
                data[type][matchIndex].$database.push(array[i].$database)
                delete array[i].$database
                data[type][matchIndex].$array.push(array[i].$array)
                delete array[i].$array

                // TODO: compare dates and merge and and updates to keep all synced and up to date
                data[type][matchIndex] = { ...data[type][matchIndex], ...array[i] };
            } else
                data[type].push(array[i])
        }
    }

    return data
}

function getBytes(data) {
    const jsonString = JSON.stringify(data);
    return Buffer.byteLength(jsonString, 'utf8');
}

function errorHandler(data, error, database, array) {
    let errorMessage = typeof error === 'object' && error.message ? error.message : error;

    let errorObject = {
        message: errorMessage,
        storage: 'mongodb'
    };

    if (database)
        errorObject.database = database;
    if (array)
        errorObject.array = array;

    if (Array.isArray(data.error)) {
        data.error.push(errorObject);
    } else {
        data.error = [errorObject];
    }
}

module.exports = { send }
