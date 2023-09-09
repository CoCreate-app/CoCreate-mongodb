const { MongoClient, ObjectId } = require('mongodb');
const { dotNotationToObject, queryData, searchData, sortData } = require('@cocreate/utils')
const clients = new Map()


async function dbClient(data) {
    if (data.storageUrl) {
        let client = clients.get(data.storageUrl)
        if (!client) {
            try {
                client = MongoClient.connect(data.storageUrl, { useNewUrlParser: true, useUnifiedTopology: true });
                clients.set(data.storageUrl, client)
            } catch (error) {
                console.error(error)
                return { status: false }
            }
        }
        return client
    }
    return
}

async function databaseStats(data) {
    const client = await dbClient(data)
    if (!client) return
    const db = client.db(data.organization_id)
    const stats = await db.stats()
    stats.storage = data.storageName
    if (!data.stats)
        data.stats = [stats]
    else
        data.stats.push(stats)
    return data
}

function createDatabase(data) {
    return database('createDatabase', data)
}

function readDatabase(data) {
    return database('readDatabase', data)
}

function updateDatabase(data) {
    return database('updateDatabase', data)
}

function deleteDatabase(data) {
    return database('deleteDatabase', data)
}

function database(action, data) {
    return new Promise(async (resolve, reject) => {
        let type = 'database'
        let databaseArray = []
        let dataTransferedIn = 0
        let dataTransferedOut = 0

        try {
            const client = await dbClient(data)
            if (!client) return
            if (action == 'readDatabase') {
                const db = client.db().admin();

                // List all the available databases
                db.listDatabases(function (err, dbs) {
                    dataTransferedIn += getBytes(dbs)

                    for (let database of dbs.databases) {
                        if (data.filter && data.filter.query) {
                            let isFilter = queryData(database, data.filter.query)
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


function createCollection(data) {
    return array('createCollection', data)
}

function readCollection(data) {
    return array('readCollection', data)
}

function updateCollection(data) {
    return array('updateCollection', data)
}

function deleteCollection(data) {
    return array('deleteCollection', data)
}

function array(action, data) {
    return new Promise(async (resolve, reject) => {
        let type = 'array'
        let arrayArray = [];
        let dataTransferedIn = 0
        let dataTransferedOut = 0

        try {
            const client = await dbClient(data)
            if (!client) return

            if (data.request)
                data.array = data.request

            let databases = data.database;
            if (!Array.isArray(databases))
                databases = [databases]

            let databasesLength = databases.length
            for (let database of databases) {
                const db = client.db(database);

                if (action == 'readCollection') {

                    let { query, sort } = await getFilters(data);

                    db.listCollections().toArray(function (error, result) {
                        if (error)
                            errorHandler(data, error, database)

                        if (result) {
                            dataTransferedIn += getBytes(result)

                            for (let res of result) {
                                if (data.filter && data.filter.query) {
                                    let isFilter = queryData(res, data.filter.query)
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


function createObject(data) {
    return object('createObject', data)
}

function readObject(data) {
    return object('readObject', data)
}

function updateObject(data) {
    return object('updateObject', data)
}

function deleteObject(data) {
    return object('deleteObject', data)
}

function object(action, data) {
    return new Promise(async (resolve, reject) => {
        try {
            const client = await dbClient(data)
            if (!client) return
            let dataTransferedIn = 0
            let dataTransferedOut = 0
            let type = 'object'
            let documents = [];

            if (data.request)
                data[type] = data.request

            if (!data['timeStamp'])
                data['timeStamp'] = new Date().toISOString()

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

                    if (data[type] && !Array.isArray(data[type]))
                        data[type] = [data[type]]

                    if (action == 'createObject') {
                        for (let i = 0; i < data[type].length; i++) {
                            data[type][i] = replaceArray(data[type][i])
                            data[type][i] = dotNotationToObject(data[type][i])
                            data[type][i]['organization_id'] = data['organization_id'];

                            if (!data[type][i]._id)
                                data[type][i]._id = ObjectId()
                            else
                                data[type][i]._id = ObjectId(data[type][i]._id)
                            data[type][i]['created'] = { on: data.timeStamp, by: data.user_id || data.clientId }
                        }

                        try {
                            dataTransferedOut += getBytes(data[type])
                            const result = await arrayObj.insertMany(data[type]);
                            dataTransferedIn += getBytes(result)
                            for (let i = 0; i < data[type].length; i++)
                                documents.push({ storage: data.storageName, database, array, ...data[type][i] })
                        } catch (error) {
                            errorHandler(data[type], error, database, array)
                        }

                    } else {
                        let isFilter
                        if (data.filter && data.filter.query)
                            isFilter = true

                        let { query, sort, index, limit } = await getFilters(data, arrayObj);

                        if (action == 'readObject') {
                            let projection = {}

                            if (data[type]) {
                                for (let i = 0; i < data[type].length; i++) {
                                    let project = replaceArray(data[type][i], true)
                                    // TODO: object.$filter
                                    // if (object.$filter)
                                    //      let filter = await getFilters({filter: object.$filter}, arrayObj);

                                    if (data[type][i]._id) {
                                        query._id = ObjectId(data[type][i]._id)
                                        try {
                                            const result = await arrayObj.findOne(query, project);
                                            result._id = result._id.toString()
                                            documents.push(result)
                                        } catch (error) {
                                            errorHandler(data[type][i], error, database, array)
                                        }
                                    } else {
                                        projection = { ...projection, ...project }
                                    }
                                }
                                delete query._id
                            }

                            if (data.filter) {
                                try {
                                    dataTransferedOut += getBytes({ query, projection, sort, index, limit })
                                    const cursor = arrayObj.find(query, projection).sort(sort).skip(index).limit(limit);
                                    while (await cursor.hasNext()) {
                                        const document = await cursor.next();
                                        dataTransferedIn += getBytes(document)

                                        if (data.filter && data.filter.search) {
                                            let isMatch = searchData(document, data.filter.search)
                                            if (!isMatch)
                                                continue;
                                        }

                                        document.storage = data.storageName
                                        document.database = database
                                        document.array = array
                                        document._id = document._id.toString()

                                        documents.push(document)
                                    }
                                } catch (error) {
                                    errorHandler(data, error, database, array)
                                }
                            }
                        } else if (action == 'updateObject') {
                            for (let i = 0; i < data[type].length; i++) {
                                if (data[type][i]._id)
                                    query['_id'] = ObjectId(data[type][i]._id)

                                data[type][i] = replaceArray(data[type][i])
                                let { update, options } = createUpdate(data, type, i)

                                if (query['_id']) {

                                    try {
                                        dataTransferedOut += getBytes({ query, update, options })
                                        const result = await arrayObj.updateOne(query, update, options);
                                        dataTransferedIn += getBytes(result)
                                        documents.push({ _id: query['_id'].toString(), storage: data.storageName, database, array, ...update['$set'] })
                                    } catch (error) {
                                        errorHandler(data[type][i], error, database, array)
                                    }

                                } else if (update && isFilter) {
                                    dataTransferedOut += getBytes({ query, sort, index, limit })
                                    // TODO: handle projection
                                    const cursor = arrayObj.find(query).sort(sort).skip(index).limit(limit);
                                    while (await cursor.hasNext()) {
                                        const document = await cursor.next();
                                        dataTransferedIn += getBytes(document)

                                        if (data.filter && data.filter.search) {
                                            let isMatch = searchData(document, data.filter.search)
                                            if (!isMatch)
                                                continue;
                                        }

                                        dataTransferedOut += getBytes({ _id: document._id, update, options })
                                        try {
                                            const result = await arrayObj.updateOne({ _id: document._id });
                                            dataTransferedIn += getBytes(result)
                                            if (data.returnObject != false)
                                                documents.push({ _id: document._id, storage: data.storageName, database, array, ...update['$set'] })
                                        } catch (error) {
                                            errorHandler(data[type][i], error, database, array)
                                        }
                                    }
                                }
                            }
                        } else if (action == 'deleteObject') {
                            for (let i = 0; i < data[type].length; i++) {
                                if (data[type][i]._id) {
                                    query._id = ObjectId(data[type][i]._id)
                                    try {
                                        dataTransferedOut += getBytes(query)
                                        const result = await arrayObj.deleteOne(query);
                                        dataTransferedIn += getBytes(result)
                                        documents.push({ _id: data[type][i]._id, storage: data.storageName, database, array })
                                    } catch (error) {
                                        errorHandler(data[type][i], error, database, array)
                                    }

                                }
                            }
                            delete query._id

                            if (isFilter) {
                                dataTransferedOut += getBytes({ query, sort, index, limit })

                                const cursor = arrayObj.find(query).sort(sort).skip(index).limit(limit);
                                while (await cursor.hasNext()) {
                                    const document = await cursor.next();
                                    dataTransferedIn += getBytes(document)

                                    if (data.filter && data.filter.search) {
                                        let isMatch = searchData(document, data.filter.search)
                                        if (!isMatch)
                                            continue;
                                    }

                                    dataTransferedOut += getBytes({ _id: document._id })
                                    try {
                                        const result = await arrayObj.deleteOne({ _id: document._id });
                                        dataTransferedIn += getBytes(result)
                                        documents.push({ _id: document._id, storage: data.storageName, database, array })
                                    } catch (error) {
                                        errorHandler(data[type][i], error, database, array)
                                    }
                                }

                            }
                        }
                    }

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

function createUpdate(data, type, index) {
    let update = {}, options = {};
    if (data[type][index]) {
        update['$set'] = data[type][index]
        update['$set']['modified'] = { on: data.timeStamp, by: data.user_id || data.clientId }
        update['$set']['organization_id'] = data.organization_id

        if (update['$set']['_id'])
            delete update['$set']['_id']

        Object.keys(update['$set']).forEach(key => {
            if (key.includes('$rename') || key.includes('$update')) {

            } else if (key.includes('$delete')) {

            } else if (key.includes('$inc')) {
                update['$inc'] = update['$set'][key]
                delete update['$set'][key]
            } else if (key.endsWith(']')) {
                // Use a regular expression to extract the key and index (including a potentially undefined or keyword index)
                const regex = /^(.*(?:\[\d+\].*?)?)\[(.*?)\](?:\[\])?$/;
                const match = inputString.match(regex);
                const index = parseInt(match[2], 10);

                if (index && index !== -1 && index !== 1 && update['$set'][key] === '$delete') {
                    match[1] = replaceArray({ [update['$set'][key]]: update['$set'][key] });
                    match[1] = Object.keys(match[1])[0]

                    if (!update['$unset'])
                        update['$unset'] = {}

                    update.$unset[match[1]] = 1;

                    if (!update['$pull'])
                        update['$pull'] = {}

                    update.$pull[match[1]] = null;
                } else {
                    match[1] = replaceArray({ [match[1]]: update['$set'][key] });
                    match[1] = Object.keys(match[1])[0]

                    if (update['$set'][key] === '$pop' || update['$set'][key] === '$delete') {
                        if (!update['$pop'])
                            update['$pop'] = {}

                        update.$pop[match[1]] = index || 1
                    } else if (match[2] === '$addToSet') {
                        if (!update['$addToSet'])
                            update['$addToSet'] = {}

                        update.$addToSet[match[1]] = update['$set'][key]
                    } else if (match[2] === '$pull') {
                        if (!update['$pull'])
                            update['$pull'] = {}

                        update.$pull[match[1]] = update['$set'][key]
                    } else {
                        if (!Array.isArray(update['$set'][key]))
                            update['$set'][key] = [update['$set'][key]]

                        let insert = { $each: update['$set'][key] }
                        if (index)
                            insert.$postion = match[2]

                        if (!update['$push'])
                            update['$push'] = {}

                        update.$push[match[1]] = insert

                    }
                }

                delete update['$set'][key]

            }
        })
    }

    if (data['deleteName']) {
        update['$unset'] = replaceArray(data['deleteName']);
    }

    if (data['updateName']) {
        update['$rename'] = replaceArray(data['updateName'])
        for (const [key, value] of Object.entries(update['$rename'])) {
            if (/\.([0-9]*)/g.test(key) || /\[([0-9]*)\]/g.test(value)) {
                console.log('key is array', /\[([0-9]*)\]/g.test(value), /\.([0-9]*)/g.test(key))
            } else {
                let newValue = replaceArray({ [value]: value })
                let oldkey = key;
                for (const [key] of Object.entries(newValue)) {
                    update['$rename'][oldkey] = key
                }
            }
        }
    }

    if (data.upsert)
        options.upsert = data.upsert

    return { update, options }

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

    if (data.filter && data.filter.sort)
        data[type] = sortData(array, data.filter.sort)
    else
        data[type] = array

    if (data.returnLog) {
        if (!data.log)
            data.log = []
        data.log.push(...data[type])
    }

    return data
}

async function getFilters(data, arrayObj) {
    let query = {}, sort = {}, index = 0, limit = 0, count
    if (data.filter) {

        if (data.filter.query)
            query = createQuery(data.filter.query);


        if (data.filter.sort) {
            for (let i = 0; i < data.filter.sort.length; i++) {
                let direction = data.filter.sort[i].direction
                if (direction == 'desc' || direction == -1)
                    direction = -1;
                else
                    direction = 1;

                sort[data.filter.sort[i].key] = direction
            }
        }

        if (arrayObj) {
            count = await arrayObj.estimatedDocumentCount()
            data.filter.count = count
        }

        if (data.filter.index)
            index = data.filter.index
        if (data.filter.limit)
            limit = data.filter.limit
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
                item.value = ObjectId(item.value)
            else
                continue
        }

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

function replaceArray(data, isProjection) {
    let keys = Object.keys(data);
    let objectData = {};
    let projection = {}

    keys.forEach((k) => {
        let nk = k
        if (/\[([0-9]*)\]/g.test(k)) {
            nk = nk.replace(/\[/g, '.');
            if (nk.endsWith(']'))
                nk = nk.slice(0, -1)
            nk = nk.replace(/\]./g, '.');
            nk = nk.replace(/\]/g, '.');
        }

        objectData[nk] = data[k];

        if (isProjection)
            projection[nk] = 1

        if (isProjection && !['_id', 'organization_id'].includes(nk))
            isProjection = 'true'
    });

    if (isProjection) {
        if (!isProjection !== 'true')
            projection = {}
        return projection
    }
    return objectData;
}

function getBytes(data) {
    const jsonString = JSON.stringify(data);
    return Buffer.byteLength(jsonString, 'utf8');
}


module.exports = {
    databaseStats,
    createDatabase,
    readDatabase,
    updateDatabase,
    deleteDatabase,

    createCollection,
    readCollection,
    updateCollection,
    deleteCollection,

    createObject,
    readObject,
    updateObject,
    deleteObject,
}
