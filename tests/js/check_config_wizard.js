/*
 * Runs the config wizard page script against the golden examples of its
 * table and the shared URL cases. Run it through `just docs-wizard-check`
 * (a container; no JS runtime on the host).
 *
 *   check_config_wizard.js          compare and exit 1 on any difference
 *   check_config_wizard.js --print  print the YAML and env of every example
 */
'use strict';

const fs = require('fs');
const path = require('path');

const repo = path.resolve(__dirname, '..', '..');
const wizard = require(path.join(repo, 'docs', 'javascripts', 'config-wizard.js'));
const table = JSON.parse(fs.readFileSync(path.join(repo, 'docs', 'data', 'config-wizard.json'), 'utf8'));
const urlCases = JSON.parse(
  fs.readFileSync(path.join(repo, 'tests', 'fixtures', 'config_wizard_url_cases.json'), 'utf8')
).cases;

function same(a, b) { return JSON.stringify(a) === JSON.stringify(b); }

if (process.argv.includes('--print')) {
  table.examples.forEach((example) => {
    const output = wizard.buildOutput(table, wizard.parseQuery(table, example.query).answers);
    process.stdout.write(JSON.stringify({ name: example.name, yaml: output.yaml, env: output.env }) + '\n');
  });
  process.exit(0);
}

const failures = [];
table.examples.forEach((example) => {
  const parsed = wizard.parseQuery(table, example.query);
  const output = wizard.buildOutput(table, parsed.answers);
  if (parsed.invalid.length) { failures.push(`example ${example.name}: refused keys ${parsed.invalid}`); }
  if (wizard.canonicalQuery(table, parsed.answers) !== example.query) { failures.push(`example ${example.name}: query is not canonical`); }
  if (output.yaml !== example.yaml) { failures.push(`example ${example.name}: YAML differs\n--- page\n${output.yaml}--- table\n${example.yaml}`); }
  if (!same(output.env, example.env)) { failures.push(`example ${example.name}: env ${output.env} != ${example.env}`); }
});
urlCases.forEach((testCase) => {
  const parsed = wizard.parseQuery(table, testCase.query);
  const canonical = wizard.canonicalQuery(table, parsed.answers);
  if (canonical !== testCase.canonical) { failures.push(`URL case ${testCase.name}: ${canonical} != ${testCase.canonical}`); }
  if (!same(parsed.invalid, testCase.invalid)) { failures.push(`URL case ${testCase.name}: refused ${parsed.invalid} != ${testCase.invalid}`); }
});

if (failures.length) {
  process.stdout.write(failures.join('\n') + '\n');
  process.stdout.write(`${failures.length} failure(s)\n`);
  process.exit(1);
}
process.stdout.write(`${table.examples.length} examples and ${urlCases.length} URL cases match\n`);
