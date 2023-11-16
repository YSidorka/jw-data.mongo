const { SECOND } = require('@jw/const');
const { Schema, createConnection } = require('mongoose');
const uuid = require('uuid');

const CONNECTIONS = {};

async function getDocumentList(options, connection) {
  const result = await findMany(options, connection);
  return result || [];
}

async function getDocumentById(id, connection) {
  if (!id) throw new Error('Invalid id');
  const result = await findById(id, connection);
  return result || null;
}

async function getDocument(options, connection) {
  const result = await findOne(options, connection);
  return result || null;
}

async function createDocument(doc) {
  const { id, ..._doc } = doc;
  const result = await createNewOne({
    _id: id || uuid.v4().replaceAll('-', ''),
    ..._doc
  });
  return result || null;
}

async function updateDocument(doc) {
  const { id, ..._doc } = doc;
  const result = await findOneAndUpdate({ _id: id, ..._doc });
  return result || null;
}

async function sleep(time) {
  return new Promise((resolve) => setTimeout(() => resolve(), time)); // eslint-disable-line
}
function initConnection(options) {
  const { dbUrl, dbName, uri, secret, ..._options } = options;

  const url = uri || `${dbUrl}${dbName}`;
  if (!dbUrl || !dbName) return null;

  let connection = getConnectionByURI(url);
  if (connection) return connection;

  connection = createConnection(url, _options);
  const id = connection.id;

  connection.on('connecting', () => { console.log(id, 'Connecting...') });
  connection.on('connected', () => { console.log(id, 'Connected') });

  connection.on('disconnecting', () => { console.log(id, 'Disconnecting...') });
  connection.on('disconnected', () => { console.log(id, 'Disconnected') });

  connection.on('close', () => { console.log(id, 'Close') });
  connection.on('reconnected', () => { console.log(id, 'Reconnected') });

  CONNECTIONS[id] = { id, url, connection };

  return connection;
}

function getAllConnections() {
  return Object.values(CONNECTIONS);
}

function getConnectionById(id) {
  return CONNECTIONS[id]?.connection;
}

function getConnectionByURI(url) {
  const result = Object.values(CONNECTIONS).find((item) => item.url === url);
  return result?.connection;
}

function getDefaultConnection() {
  const result = Object.keys(CONNECTIONS)[0];
  return getConnectionById(result);
}

function closeConnectionById(id) {
  const connection = getConnectionById(id);
  if (connection) connection.close();
  // delete CONNECTIONS[id];
}

function closeAllConnections() {
  Object.keys(CONNECTIONS).forEach((id) => {
    closeConnectionById(id)
  });
}

async function connect(dbConnection) {
  try {
    const connection = dbConnection || getDefaultConnection();
    if (!connection) throw new Error('Invalid connection');

    // 2 = connecting
    // 3 = disconnecting
    while (connection.readyState === 2 || connection.readyState === 3) {
      console.log(`sleeping...`);
      await sleep(SECOND);
    }

    // 1 = connected
    if (connection.readyState === 1) return connection;

    // 0 = disconnected
    // 99 = uninitialized
    await connection.openUri(connection['_connectionString']); // not stable
    // await connection.openUri(CONNECTIONS[connection.id]?.url); //

    console.log(`Database connected...`);
    return connection;
  } catch (err) {
    console.log(`Error connect: ${err?.message || err}`);
    throw null;
  }
}

function assignSchema(key, schema, connection) {
  try {
    if (!key) throw new Error('Invalid key');
    if (!schema || !(schema instanceof Schema)) throw new Error('Invalid schema');
    if (!connection) throw new Error('Invalid connection');

    const result = connection.model(key, schema);
    return result;
  } catch(err) {
    console.log(`Error assignSchema: ${err?.message || err}`);
    return null;
  }
}

function getModelByType(type, connection) {
  try {
    if (!type) throw new Error('Invalid schema type');
    if (!connection) throw new Error('Invalid connection');

    const models = connection.modelNames();
    const modelName = models.find((item) => `${item}`.toUpperCase() === `${type}`.toUpperCase());
    if (!modelName) throw new Error('Not supported type of documents');

    const result = connection.model(modelName)
    return result;
  } catch(err) {
    console.log(`Error getSchemaByType: ${err?.message || err}`);
    return null;
  }
}

async function findMany(options = {}, connection) {
  try {
    const _connection = await connect(connection);
    if (!_connection) return null;

    const model = getModelByType(options?.type, _connection);
    if (!model) return null;

    const docs = await model.find(options).lean();
    const result = docs.map(({ _id: id, __v, active, type, ...doc }) => ({ id, ...doc }));
    // DTO to remove specific fields
    return result;
  } catch (err) {
    console.log(`Error findMany: ${JSON.stringify(options)} - ${err?.message || err}`);
    return null;
  }
}

async function findOne(options = {}, connection) {
  try {
    const _connection = await connect(connection);
    if (!_connection) return null;

    const model = getModelByType(options?.type, _connection);
    if (!model) return null;

    const result = await model.findOne(options).lean();
    return result;
  } catch (err) {
    console.log(`Error findOne: ${JSON.stringify(options)} - ${err?.message || err}`);
    return null;
  }
}

async function findById(id, connection) {
  try {
    const _connection = await connect(connection);
    if (!_connection) return null;

    const namesArr = _connection.modelNames();
    const promiseList = namesArr.map((name) => _connection.model(name).findById(id).lean());

    const result = (await Promise.all(promiseList)).filter(Boolean)[0];
    return result;
  } catch (err) {
    console.log(`Error findById: ${id} - ${err?.message || err}`);
    return null;
  }
}

async function createNewOne(doc, connection) {
  try {
    const _connection = await connect(connection);
    if (!_connection) return null;

    const model = getModelByType(doc?.type, _connection);
    if (!model) return null;

    const options = {
      validateBeforeSave: true,
      validateModifiedOnly: false
    };

    const result = await model.create([doc], options);
    return await model.findOne({ _id: result[0]._id }).lean();
  } catch (err) {
    console.log(`Error createNewOne: _id:${JSON.stringify(doc)} - ${err?.message || err}`);
    return null;
  }
}

async function findOneAndUpdate(doc, connection) {
  try {
    const _connection = await connect(connection);
    if (!_connection) return null;

    const model = getModelByType(doc?.type, _connection);
    if (!model) return null;

    const options = {
      new: true,
      upsert: true,
      runValidators: true,
      setDefaultsOnInsert: true,
      lean: true
    };

    return await model.findOneAndUpdate({ _id: doc._id }, doc, options);
  } catch (err) {
    console.log(`Error findOneAndUpdate: _id:${doc._id} - ${err?.message || err}`);
    return null;
  }
}

async function deleteOne(doc, connection) {
  try {
    const _connection = await connect(connection);
    if (!_connection) return null;

    const model = getModelByType(doc?.type, _connection);
    if (!model) return null;

    const result = await model.deleteOne({ _id: doc._id });
    return result.deletedCount > 0;
  } catch (err) {
    console.log(`Error deleteOne: _id:${doc._id} - ${err?.message || err}`);
    return null;
  }
}

module.exports = {
  getDocumentList,
  getDocument,
  getDocumentById,
  createDocument,
  updateDocument,

  // getAllConnections,

  // getConnectionById,
  // getConnectionByURI,
  // getDefaultConnection,
  // closeConnectionById,
  // closeAllConnections,

  initConnection,
  // connect,
  assignSchema,
  // getModelByType,

  // findMany, // with specific DTO format
  // findOne,
  // findById,
  // createOne: createNewOne,
  // updateOne: findOneAndUpdate,
  // deleteOne
}
