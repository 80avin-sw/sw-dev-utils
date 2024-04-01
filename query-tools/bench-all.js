#!/usr/bin/env node
const { readFileSync } = require("fs");
const {
  getRedshiftInstance,
  getDbType,
  getSqlFiles,
  extractQueryParameters,
  getQueries,
  Promise_pool,
  BenchLog,
} = require("./utils");

const args = process.argv.slice(2);
const numRuns = args.reduce((acc, arg) => (+arg ? +arg : acc), 5);
// setPipeFile('./pipe.txt')
// console.log("numRuns", numRuns);
runBenchmarks(args, numRuns);

async function runBenchmarks(args, numRuns) {
  try {
    const files = Array.from(getSqlFiles(args));
    const items = files.map((f) => {
      const querySql = readFileSync(f, "utf-8");
      return {
        file: f,
        querySql,
        RS: getRedshiftInstance(getDbType(querySql)),
      };
    });
    const logger = new BenchLog(items);
    const batches = Array(numRuns)
      .fill()
      .map((_) => ({ items }));
    let firstResult = true;
    await Promise_pool(
      batches.flatMap((b) =>
        b.items.map((it, iti) => async () => {
          const result = await runBenchItem(it);
          logger.logItem(result, firstResult);
          firstResult = false;
          b.results = b.results || [];
          b.results[iti] = result;
          b.resultCount = (b.resultCount || 0) + 1;
          if (b.resultCount === b.items.length) {
            // logger.logBatch(b.results);
          }
        }),
      ),
      2,
    );
    process.exit(0);

    // for (let i = 0; i < numRuns; i++) {
    //   const benchResult = await runBenchmarksOnce(items);
    //   console.log("-".repeat(colWidths.reduce((acc, c) => acc + c, 0)));
    //   benchResults.push(benchResult);
    // }
    // logger.logAll(batches.map((b) => b.results));
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

async function runBenchItem(it) {
  const [cleanQuery, params] = extractQueryParameters(it.querySql);
  // const benchQuery = it.RS.raw(queryBatch(1, cleanQuery), params);
  const queries = getQueries(cleanQuery);
  await it.RS.raw(queries[0]);
  const start = Date.now();
  const resultQuery = await it.RS.raw(queries[1], params);
  const reqTime = (Date.now() - start) * 1000;
  const resultStats = await it.RS.raw(queries[2]);
  // const resp = await benchQuery;
  const resp = [resultQuery, resultStats];
  const query_history = resp[resp.length - 1];
  const qlog = query_history.rows[0];
  return {
    file: it.file,
    qlog,
    times: {
      req_time: reqTime,
      elapsed_time: qlog.elapsed_time,
      queue_time: qlog.queue_time,
      execution_time: qlog.execution_time,
      compile_time: qlog.compile_time,
      planning_time: qlog.planning_time,
      lock_wait_time: qlog.lock_wait_time,
    },
  };
}
