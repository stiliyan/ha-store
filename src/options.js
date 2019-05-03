/**
 * Options
 */

'use strict';

/* Requires ------------------------------------------------------------------*/

const {exp} = require('./utils.js');

/* Local variables -----------------------------------------------------------*/

const defaultConfig = {
  batch: {
    tick: 50,
    max: 100,
  },
  cache: {
    limit: 60000,
    ttl: 60000,
  },
};

/* Methods -------------------------------------------------------------------*/

function hydrateIfNotNull(baseConfig, defaultConfig) {
  if (baseConfig === null) {
    return null;
  }

  if (!baseConfig) {
    return {...defaultConfig};
  }

  return {
    ...defaultConfig,
    ...baseConfig,
  };
}

function hydrateConfig(config = {}) {
  return {
    ...config,
    batch: hydrateIfNotNull(config.batch, defaultConfig.batch),
    retry: hydrateIfNotNull(config.retry, defaultConfig.retry),
    cache: hydrateIfNotNull(config.cache, defaultConfig.cache),
  };
}

/* Exports -------------------------------------------------------------------*/

module.exports = {hydrateConfig};
