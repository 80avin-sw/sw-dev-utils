#!/usr/bin/env node
require("dotenv").config();
const fs = require("fs");
const knex = require("knex");
const { randomUUID } = require("crypto");
const path = require("path");
const vm = require("vm");

const getRSConfig = (env) => ({
  client: "redshift",
  searchPath: [env.REDSHIFT_DB_SCHEMA],
  connection: {
    host: env.REDSHIFT_DB_URI || "localhost",
    user: env.REDSHIFT_DB_USER,
    password: env.REDSHIFT_DB_PASSWORD,
    database: env.REDSHIFT_DB_NAME,
    port: env.REDSHIFT_DB_PORT || 5439,
  },
  pool: {
    min: env.pool?.min ?? 1,
    max: env.pool?.max ?? 2,
    propagateCreateError: false,
    afterCreate: (...args) => {
      // console.log("afterCreate0", args);
      // console.dir(args[0].query);
      const [res, done] = args;
      done();
      // res.query("SET mv_enable_aqmv_for_session to off", (...qargs) => {
      //   const [err] = qargs;
      //   if (err) console.log("afterCreate: E", err);
      //   console.log("ran afterCreate");
      //   done();
      // });
    },
  },
  acquireConnectionTimeout: 600000,
});

const envConfigs = {
  dryrun: {
    REDSHIFT_DB_NAME: process.env.DRYRUN_REDSHIFT_DB_NAME,
    REDSHIFT_DB_PORT: process.env.DRYRUN_REDSHIFT_DB_PORT,
    REDSHIFT_DB_URI: process.env.DRYRUN_REDSHIFT_DB_URI,
    REDSHIFT_DB_USER: process.env.DRYRUN_REDSHIFT_DB_USER,
    REDSHIFT_DB_PASSWORD: process.env.DRYRUN_REDSHIFT_DB_PASSWORD,
    REDSHIFT_DB_SCHEMA: process.env.DRYRUN_REDSHIFT_DB_SCHEMA,
    pool: { max: 10 },
  },
  prod: {
    REDSHIFT_DB_NAME: process.env.REDSHIFT_DB_NAME,
    REDSHIFT_DB_PORT: process.env.REDSHIFT_DB_PORT,
    REDSHIFT_DB_URI: process.env.REDSHIFT_DB_URI,
    REDSHIFT_DB_USER: process.env.REDSHIFT_DB_USER,
    REDSHIFT_DB_PASSWORD: process.env.REDSHIFT_DB_PASSWORD,
    REDSHIFT_DB_SCHEMA: process.env.REDSHIFT_DB_SCHEMA,
  },
};

const rsInstances = {};

const getRedshiftInstance = (type = "prod") => {
  if (!rsInstances[type]) {
    rsInstances[type] = knex(getRSConfig(envConfigs[type || "prod"]));
    // patchEmit(rsInstances[type], "knex");
    // patchEmit(rsInstances[type].client, "client");
    // patchEmit(rsInstances[type].client.driver[0], "driver[0]");
  }
  return rsInstances[type];
};

/**
 * @param {string} q
 * @returns {keyof typeof envConfigs}
 */
const getDbType = (q) => {
  if (q.startsWith("--dryrun")) return "dryrun";
  if (q.startsWith("--prod")) return "prod";
  return "prod";
};

/**
 * In our custom format, we can add block comment in sql to define parameters.
 * Add a markdown code block in a block comment to define query parameters.
 * Inside the code block, write javascript object(for named parameters) or array(for positional parameters)
 * Example file:
 * ```
 * \```sql-params
 * [a, 'l', true]
 * \```
 * ```
 */
function extractQueryParameters(sql) {
  let queryParams;
  let query = sql.replace(/\/\*.*?\*\//gs, (comment) => {
    comment.match(/(?<=```params).*?(?=```)/s).forEach((codeBlock) => {
      const curParams = vm.runInContext(`(${codeBlock})`, vm.createContext({}));
      if (
        queryParams &&
        (!curParams ||
          typeof queryParams !== typeof curParams ||
          Array.isArray(curParams) !== Array.isArray(queryParams))
      ) {
        throw new Error("Invalid query params:\n---\n" + codeBlock);
      }
      queryParams = queryParams
        ? Array.isArray(queryParams)
          ? [...queryParams, ...curParams]
          : { ...queryParams, ...curParams }
        : curParams;
    });
    return "";
  });
  // console.log("params", queryParams);
  return [query, queryParams];
}

/**
 * TODO: instead of using `queryTag`, register the query to redshift and get stats using query id (`query` field)
 */
function queryBatch(count, sql) {
  let query = `SET enable_result_cache_for_session TO off;`;
  const queryTags = [];
  for (let i = 0; i < count; i++) {
    const queryTag = `-- avinash${randomUUID()}`;
    queryTags.push(queryTag);
    query += `${queryTag}\n${sql};`;
  }
  const timeBeforeStart = new Date(+new Date() - 5000).toISOString(); // any suitable time before running query. Offset by -5000ms to cover time inconsistencies b/w redshift & local clock
  query += `SELECT * FROM sys_query_history WHERE ${queryTags
    .map((qt) => `(query_text like '${qt}%' AND start_time > timestamp '${timeBeforeStart}')`)
    .join(" OR ")}`;
  return query;
}

function getQueries(sql) {
  const queryTag = `-- avinash${randomUUID()}`;
  const timeBeforeStart = new Date(+new Date() - 5000).toISOString(); // any suitable time before running query. Offset by -5000ms to cover time inconsistencies b/w redshift & local clock
  return [
    `SET enable_result_cache_for_session TO off;`,
    `${queryTag}\n${sql}`,
    `SELECT * FROM sys_query_history WHERE 
        query_text like '${queryTag}%' AND start_time > timestamp '${timeBeforeStart}'`,
  ];
}

const MAX_BATCH_SIZE = 5;

function* getSqlFiles(args, depth = 3) {
  for (const arg of args) {
    const filename = arg;
    if (fs.existsSync(filename)) {
      // console.log('exists', filename)
      const stats = fs.statSync(filename);
      if (stats.isDirectory()) {
        // console.log('dir', filename, fs.readdirSync(filename))
        yield* getSqlFiles(
          fs.readdirSync(filename).map((fn) => path.join(filename, fn)),
          depth - 1
        );
      } else if (stats.isFile() && filename.endsWith(".sql") && !filename.includes(".ignore")) {
        // console.log('file', filename)
        yield filename;
      }
    }
  }
}

/**
 * Similar to `Promise.all` but doesn't run all promises at once. Also, the argument should be array of functions that return promises.
 * 
 * @param {(() => Promise<void>)[]} fns List of functions to execute in a pool
 * @param {number} queueSize 
 * @returns 
 */
function Promise_pool(fns, queueSize = 10) {
  return new Promise((resolve, reject) => {
    const results = Array(fns.length);
    let resolveCount = 0;
    let initCount = 0;
    function execNext() {
      const i = initCount;
      initCount += 1;
      const it = fns[i]();
      it.then((res) => {
        resolveCount += 1;
        results[i] = res;
        if (resolveCount === fns.length) resolve(results);
        if (initCount < fns.length) {
          execNext(initCount + 1);
        }
      }).catch((err) => reject(err));
    }
    for (let i = 0; i < queueSize && i < fns.length; i++) {
      execNext();
    }
  });
}

class BenchLog {
  constructor(items) {
    this.maxFilenameLength = Math.max(...items.map((it) => it.file.length));
    this.items = items;
    this.colWidths = [this.maxFilenameLength + 5, 20, 20, 20, 20, 20, 20];
  }
  logHeaders(result) {
    const { colWidths } = this;
    console.log(
      "file".padEnd(colWidths[0], " "),
      "queryId".padEnd(15, " "),
      ...Object.keys(result.times).map((col) => col.padEnd(15, " "))
    );
  }
  logItem(result, headers = true) {
    const { colWidths } = this;
    if (headers) {
      this.logHeaders(result);
    }
    console.log(
      result.file.padEnd(colWidths[0], " "),
      result.qlog.query_id.padEnd(15, " "),
      ...Object.values(result.times).map((time) => (time / 1e6).toFixed(3).padEnd(15, " "))
    );
  }
  logBatch(batchResult, headers = true) {
    batchResult.forEach((result, i) => {
      this.logItem(result, headers && i === 0);
    });
  }
  logAll(batchResults) {
    printResults(items, batchResults);
  }
}

function printResults(items, benchResults) {
  console.log("\n");
  items.forEach((it, i) => {
    const file = it.file;
    console.log("file: ", file);
    console.log(["#\t", ...Object.keys(benchResults[0][0].times).map((col) => col.padEnd(20, " "))].join(""));
    benchResults.forEach((benchResult, j) => {
      console.log(
        [`${j}#\t`, ...Object.values(benchResult[i].times).map((time) => (time / 1e6).toFixed(3).padEnd(20, " "))].join(
          ""
        )
      );
    });
    const meanTimes = benchResults.reduce((acc, br) => {
      Object.entries(br[i].times).forEach(([col, time]) => {
        acc[col] = (acc[col] || 0) + time / 1e6 / benchResults.length;
      });
      return acc;
    }, {});

    console.log(`Avg`);
    Object.entries(meanTimes).forEach(([col, time]) => console.log(`\t${col.padEnd(20, " ")}: ${time.toFixed(3)}`));
    console.log("-".repeat(30));
  });
}

const pev2_path = "file://"+path.dirname(require.main.filename)+"/pev2.html";

function setPipeFile(tee = true) {}

module.exports = {
  getRedshiftInstance,
  getDbType,
  getSqlFiles,
  MAX_BATCH_SIZE,
  pev2_path,
  setPipeFile,
  extractQueryParameters,
  getQueries,
  queryBatch,
  Promise_pool,
  BenchLog,
};
