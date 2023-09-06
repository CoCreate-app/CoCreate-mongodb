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

        try {
            const client = await dbClient(data)
            if (!client) return
            if (action == 'readDatabase') {
                const db = client.db().admin();

                // List all the available databases
                db.listDatabases(function (err, dbs) {

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
                db.dropDatabase().then(response => {
                    resolve(response)
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
                            data = createData(data, arrayArray, type)
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
                            db.createCollection(array, function (error, result) {
                                if (error)
                                    errorHandler(data, error, database, array)

                                if (result)
                                    arrayArray.push({ name: array, database, storage: data.storageName })

                                arraysLength -= 1
                                if (!arraysLength)
                                    databasesLength -= 1

                                if (!databasesLength && !arraysLength) {
                                    data = createData(data, arrayArray, type)
                                    resolve(data)
                                }
                            })
                        } else {
                            if (action == 'updateCollection') {
                                [array, value] = array
                            }

                            const arrayObj = db.collection(array);

                            if (action == 'updateCollection') {
                                arrayObj.rename(value, function (error, result) {
                                    if (error)
                                        errorHandler(data, error, database, array)

                                    if (result)
                                        arrayArray.push({ name: value, oldName: array, database, storage: data.storageName })

                                    arraysLength -= 1
                                    if (!arraysLength)
                                        databasesLength -= 1

                                    if (!databasesLength && !arraysLength) {
                                        data = createData(data, arrayArray, type)
                                        resolve(data)
                                    }

                                })
                            }

                            if (action == 'deleteCollection') {
                                arrayObj.drop(function (error, result) {
                                    if (error)
                                        errorHandler(data, error, database, array)

                                    if (result)
                                        arrayArray.push({ name: array, database, storage: data.storageName })

                                    arraysLength -= 1
                                    if (!arraysLength)
                                        databasesLength -= 1

                                    if (!databasesLength && !arraysLength) {
                                        data = createData(data, arrayArray, type)
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

            let type = 'object'
            let documents = [];

            if (data.request)
                data[type] = data.request

            if (!data['timeStamp'])
                data['timeStamp'] = new Date().toISOString()

            let databases = data.database;
            if (!Array.isArray(databases))
                databases = [databases]

            let databasesLength = databases.length
            for (let database of databases) {
                let arrays = data.array;
                if (!Array.isArray(arrays))
                    arrays = [arrays]

                let arraysLength = arrays.length
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

                        arrayObj.insertMany(data[type], function (error, result) {
                            if (error)
                                errorHandler(data, error, database, array)

                            for (let i = 0; i < data[type].length; i++)
                                documents.push({ storage: data.storageName, database, array, ...data[type][i] })

                            arraysLength -= 1
                            if (!arraysLength)
                                databasesLength -= 1

                            if (!databasesLength && !arraysLength) {
                                data = createData(data, documents, type)
                                resolve(data)
                            }
                        });
                    } else {
                        let isFilter
                        if (data.filter && data.filter.query)
                            isFilter = true

                        let { query, sort, index, limit } = await getFilters(data, arrayObj);

                        if (action == 'readObject') {
                            if (data[type]) {
                                let _ids = []
                                for (let i = 0; i < data[type].length; i++) {
                                    data[type][i] = replaceArray(data[type][i])
                                    if (data[type][i]._id) {
                                        _ids.push(ObjectId(data[type][i]._id))
                                    }
                                }

                                if (_ids.length == 1)
                                    query['_id'] = _ids[0]
                                else if (_ids.length > 0)
                                    query['_id'] = { $in: _ids }

                            }

                            try {
                                const cursor = arrayObj.find(query).sort(sort).skip(index).limit(limit);
                                while (await cursor.hasNext()) {
                                    const document = await cursor.next();

                                    if (data.filter && data.filter.search) {
                                        let isMatch = searchData(document, data.filter.search)
                                        if (!isMatch)
                                            continue;
                                    }

                                    document.storage = 'mongodb'
                                    document.database = database
                                    document.array = array
                                    document._id = document._id.toString()

                                    if (data.returnObject == false) {
                                        let tempDoc = {};
                                        let docs = new Map(data[type].map((obj) => [obj._id, obj]));
                                        let doc1 = docs.get(document._id)
                                        if (doc1) {
                                            tempDoc._id = tempDoc
                                            for (let key of Object.keys(doc1)) {
                                                tempDoc[key] = document[key]
                                            }
                                            document = tempDoc
                                        }
                                    }

                                    documents.push(document)
                                }
                            } catch (error) {
                                errorHandler(data, error, database, array)
                            }

                            arraysLength -= 1
                            if (!arraysLength)
                                databasesLength -= 1

                            if (!databasesLength && !arraysLength) {
                                data = createData(data, documents, type)
                                resolve(data)
                            }
                        } else if (action == 'updateObject') {

                            if (!data[type].length) {
                                arraysLength -= 1

                                if (!arraysLength)
                                    databasesLength -= 1

                                if (!databasesLength && !arraysLength) {
                                    data = createData(data, documents, type)
                                    resolve(data)
                                }
                            }

                            let docsLength = data[type].length
                            for (let i = 0; i < data[type].length; i++) {
                                if (data[type][i]._id)
                                    query['_id'] = ObjectId(data[type][i]._id)

                                data[type][i] = replaceArray(data[type][i])
                                let { update, projection } = createUpdate(data, type, i)

                                if (query['_id']) {
                                    arrayObj.updateOne(query, update, {
                                        upsert: data.upsert,
                                        projection
                                    }).then((result) => {
                                        documents.push({ _id: query['_id'].toString(), storage: data.storageName, database, array, ...update['$set'] })
                                    }).catch((error) => {
                                        errorHandler(data, error, database, array)
                                        console.log(action, 'error', error);
                                    }).finally(() => {
                                        docsLength -= 1
                                        if (!docsLength)
                                            arraysLength -= 1

                                        if (!arraysLength)
                                            databasesLength -= 1

                                        if (!databasesLength && !arraysLength) {
                                            data = createData(data, documents, type)
                                            resolve(data)
                                        }
                                    })
                                } else if (update && isFilter) {
                                    const cursor = arrayObj.find(query).sort(sort).skip(index).limit(limit);
                                    while (await cursor.hasNext()) {
                                        const document = await cursor.next();

                                        if (data.filter && data.filter.search) {
                                            let isMatch = searchData(document, data.filter.search)
                                            if (!isMatch)
                                                continue;
                                        }

                                        arrayObj.updateOne({ _id: document._id }, update, {
                                            upsert: data.upsert,
                                            projection
                                        }).then((result) => {
                                            if (data.returnObject != false)
                                                documents.push({ _id: document._id, storage: data.storageName, database, array, ...update['$set'] })
                                        }).catch((error) => {
                                            errorHandler(data, error, database, array)
                                            console.log(action, 'error', error);
                                        })
                                    }
                                }
                            }
                        } else if (action == 'deleteObject') {
                            if (isFilter && data.returnObject != false) {
                                const cursor = arrayObj.find(query).sort(sort).skip(index).limit(limit);
                                while (await cursor.hasNext()) {
                                    const document = await cursor.next();

                                    if (data.filter && data.filter.search) {
                                        let isMatch = searchData(document, data.filter.search)
                                        if (!isMatch)
                                            continue;
                                    }

                                    arrayObj.deleteOne({
                                        _id: document._id
                                    }).then((result) => {
                                        documents.push({ _id: document._id, storage: data.storageName, database, array })
                                    }).catch((error) => {
                                        errorHandler(data, error, database, array)
                                        console.log(action, 'error', error);
                                    });
                                }

                            }

                            let _ids = [];
                            for (let i = 0; i < data[type].length; i++) {
                                if (data[type][i]._id) {
                                    _ids.push(ObjectId(data[type][i]._id))
                                    documents.push({ _id: data[type][i]._id, storage: data.storageName, database, array })
                                }
                            }

                            if (_ids.length == 1)
                                query['_id'] = _ids[0]
                            else if (_ids.length)
                                query['_id'] = { $in: _ids }

                            arrayObj.deleteMany(query, function (error, result) {
                                arraysLength -= 1
                                if (!arraysLength)
                                    databasesLength -= 1

                                if (!databasesLength && !arraysLength) {
                                    data = createData(data, documents, type)
                                    resolve(data)
                                }

                            })
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

function createUpdate(data, type, index) {
    let update = {}, projection = {};
    if (data[type][index]) {
        update['$set'] = data[type][index]
        update['$set']['modified'] = { on: data.timeStamp, by: data.user_id || data.clientId }
        update['$set']['organization_id'] = data.organization_id

        if (update['$set']['_id'])
            delete update['$set']['_id']

        Object.keys(update['$set']).forEach(key => {
            if (key.includes('$inc')) {
                update['$inc'] = update['$set'][key]
                delete update['$set'][key]
            } else if (key.includes('[u]')) {
                update['$addToSet'] = { [key.replace('[u]', '')]: update['$set'][key] }
                delete update['$set'][key]
            } else if (key.includes('[]')) {
                if (!Array.isArray(update['$set'][key]))
                    update['$set'][key] = [update['$set'][key]]

                update['$push'] = {
                    [key.replace('[]', '')]: { $each: update['$set'][key] }
                }
                delete update['$set'][key]
            }
            // { $push: { "skills": { $each: ["Sports", "Acting"] } } })
            // { $addToSet: { "skills": "GST" } }) // adds "GST"to all arrays if the item does not exist
            projection[key] = 1
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

    return { update, projection }

}

function createData(data, array, type) {
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
    let query =
        {}, sort = {}, index = 0, limit = 0, count
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

function replaceArray(data) {
    let keys = Object.keys(data);
    let objectData = {};

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
    });

    return objectData;
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
