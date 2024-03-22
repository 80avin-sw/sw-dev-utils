#!/usr/bin/env node

const fs = require("fs");
const util = require("util");
const exec = util.promisify(require("child_process").exec);
const {
  getDbType,
  getRedshiftInstance,
  getSqlFiles,
  pev2_path,
  extractQueryParameters,
} = require("./utils");

async function openPlans(args) {
  // console.log('opening', files)
  for (const file of getSqlFiles(args)) {
    console.log("opening", file);
    const querySql = fs.readFileSync(file, "utf-8");
    const [cleanQuery, params] = extractQueryParameters(querySql);
    const dbType = getDbType(querySql);
    const redshift = getRedshiftInstance(dbType);
    const planResult = await redshift.raw(`EXPLAIN ${cleanQuery}`, params);
    // console.log(planResult)
    const plan = planResult.rows.map((p) => p["QUERY PLAN"]).join("\n");
    const planFile = `${file}.plan.txt`;
    const sp = new URLSearchParams({
      q: JSON.stringify([file, plan, querySql]),
    });
    const planUrl = `${pev2_path}?${sp.toString()}`;
    const cmd = `open -a "Google Chrome" '${pev2_path}?${sp.toString()}'`;
    fs.writeFileSync(planFile, planUrl);
    // console.log(cmd);
    // await exec(cmd)
  }
  process.exit(0);
}

const files = process.argv.slice(2);
openPlans(files);
