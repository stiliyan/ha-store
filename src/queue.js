/**
 * Queue processing
 */

/* Requires ------------------------------------------------------------------*/

const { basicParser, deferred, contextKey, recordKey } = require('./utils.js');

/* Local variables -----------------------------------------------------------*/

const notFoundSymbol = Symbol('Not Found');
const contextRecordKey = key => id => recordKey(key, id);

/* Methods -------------------------------------------------------------------*/

function queue(config, emitter, targetStore) {

  // Local variables
  const contexts = new Map();

  /**
   * Attempts to read a query item from cache
   * If no records are found, a deferred handle is created
   * @param {string} key The context key
   * @param {string} id The record id
   * @param {object} context The context object
   * @returns {*|null} The cache result
   */
  async function lookupCache(key, id, context) {
    if (targetStore !== null) {
      const record = await targetStore.get(recordKey(key, id));
      
      if (record !== undefined) {
        emitter.emit('cacheHit', { key, id, params: context.params });
        return record;
      }

      emitter.emit('cacheMiss', { key, id, params: context.params });
    }

    const expectation = context.promises.get(id);
    if (expectation !== undefined) {
      emitter.emit('coalescedHit', { key, id, params: context.params });
      return expectation.promise;
    }
    else {
      context.promises.set(id, deferred());
      context.ids.push(id);
    }
    
    return notFoundSymbol;
  }

  /**
   * Creates or returns a context object
   * @param {string} key The context key
   * @param {*} params The parameters for the context
   * @param {boolean} ephemeral If we should not store the request context (disabled batching)
   * @returns {object} The context object
   */
  function resolveContext(key, params, ephemeralKey) {
    if (config.batch === null) {
      key = ephemeralKey;
    }
    let context = contexts.get(key);
    if (context === undefined) {
      context = {
        key,
        ids: [],
        promises: new Map(),
        params,
        batchData: {},
        timer: null,
      };
      contexts.set(key, context);
    }
    return context;
  }

  /**
   * Gathers the ids in preparation for a data-source query
   * @param {string} type The type of query (direct or batch)
   * @param {string} key The context key
   * @param {object} context The context object
   */
  function batch(type, key, context) {
    clearTimeout(context.timer);
    context.timer = null;

    const ids = context.ids.splice(0, context.ids.length);
    const maxBatches = Math.ceil(ids.length / (config.batch && config.batch.max || ids.length));
    const optimalBatchSize = Math.ceil(ids.length / maxBatches);
    while (ids.length > 0) {
      query(type, key, ids.splice(0, optimalBatchSize), context);
    }

    contexts.delete(context.key);
  }

  /**
   * Main queue function
   * - Resolves context object and deferred handlers
   * - Looks-up cache
   * - Prepares data-source query timer/invocation
   * @param {string} id The id of the record to query
   * @param {*} params The parameters for the query
   * @param {*} agg A value to add to the list of the context's persisted batch state
   * @param {string} uid The request uuid, for multi-get disabled batches
   * @param {boolean} startQueue Wether to start the queue immediately or not
   */
  async function push(id, params, agg, startQueue, uid) {
    const key = contextKey(config.uniqueParams, params);
    const context = resolveContext(key, params, uid);
    let entity = await lookupCache(key, id, context);
    if (agg !== null) {
      if (!(id in context.batchData)) context.batchData[id] = [];
      context.batchData[id].push(agg);
    }
    if (entity !== notFoundSymbol) {
      if (!config.batch && startQueue === true) batch('direct', key, context);
      return entity;
    }
    entity = context.promises.get(id).promise;

    if (config.batch) {
      if (context.timer === null) {
        context.timer = setTimeout(() => batch('batch', key, context), config.batch.tick);
      }
    }
    else {
      if (startQueue === true) batch('direct', key, context);
    }
    
    return entity;
  }

  /**
   * Performs the query to the data-source
   * @param {string} type The type of query (direct or batch)
   * @param {string} key The context key
   * @param {array} ids The ids to query
   * @param {object} context The context object
   */
  function query(type, key, ids, context, bd) {
    bd = bd || ids.reduce((acc, id) => {
      if (id in context.batchData) {
        if ([Number, String, Boolean].includes(context.batchData[id].constructor)) {
          acc[id] = context.batchData[id]
        }
        else if (typeof context.batchData[id] === 'object'){
          acc[id] = JSON.parse(JSON.stringify(context.batchData[id]));
        }
        else acc[id] = null;
        delete context.batchData[id];
      }
      return acc;
    }, {});

    function handleQuerySuccess(results) {
      emitter.emit('querySuccess', { type, key, ids, params: context.params, batchData: bd });
      complete(key, ids, context, results);
    }

    function handleQueryError(err) {
      emitter.emit('queryFailed', { type, key, ids, params: context.params, error: err, batchData: bd });
      for (let i = 0; i < ids.length; i++) {
        const expectation = context.promises.get(ids[i]);
        if (expectation !== undefined) {
          expectation.reject(err);
          context.promises.delete(ids[i]);
        }
      }
      if (context.promises.size === 0) {
        contexts.delete(context.key);
      }
    }

    emitter.emit('query', { type, key, ids, params: context.params, batchData: bd });
    Promise.resolve()
      .then(() => config.resolver(ids, context.params, bd))
      .then(handleQuerySuccess, handleQueryError);
  }

  /**
   * Query success handler
   * Assures the results are properly parsed, promises resolved and contexts cleaned up
   * @param {string} key The context key
   * @param {array} ids The list of ids to query
   * @param {object} context The context object
   * @param {*} results The query results
   */
  function complete(key, ids, context, results) {
    const parser = config.responseParser || basicParser;
    const records = parser(results, ids, context.params);

    if (targetStore !== null) {
      targetStore.set(contextRecordKey(key), ids.filter(id => records[id] !== null && records[id] !== undefined), records, { step: 0 });
    }

    for (let i = 0; i < ids.length; i++) {
      const expectation = context.promises.get(ids[i]);
      if (expectation !== undefined) {
        expectation.resolve(records[ids[i]]);
        context.promises.delete(ids[i]);
      }
    }

    if (context.ids.length === 0) {
      if (context.promises.size === 0) contexts.delete(context.key);
    }
  }

  /**
   * The number of active contexts
   * @returns {number} The number of active contexts
   */
  function size() {
    return contexts.size;
  }

  return { batch, push, size, query, resolveContext, complete };
}

/* Exports -------------------------------------------------------------------*/

module.exports = queue;
