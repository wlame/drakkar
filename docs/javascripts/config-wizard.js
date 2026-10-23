/*
 * Config wizard of the calculator page (calculator/).
 *
 * The page asks for hardware and timing answers and, for a fleet, where the
 * workers run and which services they reach. It prints one YAML block, the
 * environment variables that hold the secrets, and a link that carries every
 * answer in its query.
 *
 * The provider rules live in data/config-wizard.json. This script only reads
 * that table, matches its rows, merges its fragments and serializes the
 * result; it holds no provider rule of its own. The pure functions are
 * exported (drakkarConfigWizard, or module.exports) so that a plain JS
 * runtime can run them against the golden examples of the table.
 */
(function (root) {
  'use strict';

  var STEPS = ['workload', 'hosts', 'services', 'choose', 'result'];
  var INTEGER_TEXT = /^[0-9]{1,7}$/;
  // A string is printed bare when it starts with a letter, '_' or '/', holds only these
  // characters and does not end with ':' (a plain scalar that ends with ':' starts a mapping).
  // The first-character rule also keeps every number-like text quoted.
  var BARE_STRING = /^[A-Za-z_\/][A-Za-z0-9_.\/:-]*$/;
  var RESERVED_WORDS = ['true', 'false', 'yes', 'no', 'on', 'off', 'null', '~'];
  var CALCULATOR_SECTIONS = ['sources', 'executor', 'ui'];
  var TABLE_URL = '../data/config-wizard.json';
  // A table that has not arrived by then counts as not loaded, so the calculator still appears.
  var TABLE_TIMEOUT_MS = 5000;

  // ------------------------------------------------------------ calculator

  // Event-loop throughput ceiling: one Python event loop orchestrates every
  // completion, so past ~400 tasks/sec the loop is the bottleneck and extra
  // slots add queue, not throughput (see performance.md, "Bottleneck: Event
  // Loop"). The pool is capped at 400 x task duration.
  var LOOP_CEILING_TASKS_PER_SEC = 400;

  /** The calculator values for the numbers of the workload step. */
  function computeCalculator(numbers) {
    var cpu = numbers.cpu;
    var p80 = numbers.p80;
    var stdoutKb = numbers.stdout;
    var workers = numbers.workers;
    var partitions = numbers.partitions;
    var p80s = p80 / 1000;
    var c = { cpu: cpu, p80: p80, p80s: p80s, stdoutKb: stdoutKb, workers: workers };

    // max_executors: cores - 20% reserved, capped by partitions per worker
    c.reservedCores = Math.max(2, Math.ceil(cpu * 0.2));
    var maxWorkers = Math.max(1, cpu - c.reservedCores);
    c.partPerWorker = Math.ceil(partitions / workers);
    maxWorkers = Math.min(maxWorkers, c.partPerWorker * 4);
    maxWorkers = Math.max(1, maxWorkers);
    c.loopCapSlots = Math.max(1, Math.round(LOOP_CEILING_TASKS_PER_SEC * p80s));
    c.loopCapped = c.loopCapSlots < maxWorkers;
    if (c.loopCapped) { maxWorkers = c.loopCapSlots; }
    c.maxWorkers = maxWorkers;

    // window_size: target 2-5 seconds of work per window
    c.windowTargetSec = p80s < 0.1 ? 5 : p80s < 1 ? 3 : 2;
    c.windowSize = Math.min(Math.max(1, Math.round(c.windowTargetSec / p80s)), 500);

    // task_timeout: 10x p80 (floor 30s, cap 600s)
    c.taskTimeout = Math.max(30, Math.min(600, Math.round(p80s * 10)));

    // max_poll_records: fill one window cycle
    c.pollRecords = Math.max(50, Math.min(1000, c.windowSize * Math.min(c.partPerWorker, 10)));

    // backpressure multipliers
    if (p80s < 0.1) { c.highMult = 8; c.lowMult = 2; }
    else if (p80s < 1) { c.highMult = 16; c.lowMult = 4; }
    else if (p80s < 10) { c.highMult = 32; c.lowMult = 4; }
    else { c.highMult = 8; c.lowMult = 2; }

    // drain_timeout: 2x p80 (floor 30s to match framework default, cap 120s)
    c.drainTimeout = Math.max(30, Math.min(120, Math.ceil(p80s * 2)));
    c.maxRetries = p80s < 1 ? 2 : 3;

    // Kafka timeouts
    c.sessionTimeout = p80s < 1 ? 10000 : p80s < 10 ? 45000 : 60000;
    c.heartbeatInterval = Math.max(1000, Math.round(c.sessionTimeout / 5));
    c.worstWindowSec = c.windowSize * c.taskTimeout;
    c.maxPollInterval = Math.max(300000, Math.min(900000, c.worstWindowSec * 1000 * 2));

    // UI duration thresholds. ws_min_duration_ms has a floor of 30ms: below
    // that, fast task WS events cause buggy timeline rendering and browser
    // performance issues.
    if (p80 < 50) {
      c.wsMin = Math.max(30, Math.round(p80 * 3)); c.eventMin = Math.round(p80 / 2);
      c.outputMin = Math.round(p80 * 2); c.logMin = Math.round(p80 * 2);
      c.storeOutput = false;
    } else if (p80 < 500) {
      c.wsMin = Math.max(30, Math.round(p80)); c.eventMin = 0;
      c.outputMin = Math.round(p80 / 2); c.logMin = Math.round(p80 / 2);
      c.storeOutput = true;
    } else {
      c.wsMin = 0; c.eventMin = 0; c.outputMin = 0; c.logMin = 0; c.storeOutput = true;
    }

    // estimates
    c.perTaskMb = (5 + stdoutKb) / 1024;
    c.peakQueuedMb = maxWorkers * c.highMult * c.perTaskMb;
    c.recorderBufferMb = 50000 * 0.5 / 1024;
    c.baseMb = 150;
    c.totalEstMb = c.baseMb + c.peakQueuedMb + c.recorderBufferMb;
    c.tasksPerSec = maxWorkers / p80s;
    c.tasksPerSecCluster = c.tasksPerSec * workers;
    return c;
  }

  /** The YAML part that the calculator values set. */
  function calculatorFragment(c) {
    return {
      sources: { kafka: {
        max_poll_records: c.pollRecords,
        session_timeout_ms: c.sessionTimeout,
        heartbeat_interval_ms: c.heartbeatInterval,
        max_poll_interval_ms: c.maxPollInterval
      } },
      executor: {
        max_executors: c.maxWorkers,
        window_size: c.windowSize,
        task_timeout_seconds: c.taskTimeout,
        max_retries: c.maxRetries,
        drain_timeout_seconds: c.drainTimeout,
        backpressure_high_multiplier: c.highMult,
        backpressure_low_multiplier: c.lowMult
      },
      ui: {
        ws_min_duration_ms: c.wsMin,
        log_min_duration_ms: c.logMin,
        recorder: {
          event_min_duration_ms: c.eventMin,
          output_min_duration_ms: c.outputMin,
          store_output: c.storeOutput
        }
      }
    };
  }

  /** How each calculator value was found, and the estimates: [{title, items: [{term, text}]}]. */
  function calculatorExplanations(c) {
    var p80 = c.p80, p80s = c.p80s, maxWorkers = c.maxWorkers;
    var items = [];
    function add(term, text) { items.push({ term: term, text: text }); }

    add('max_executors: ' + maxWorkers, c.loopCapped
      ? c.cpu + ' cores would allow more, but the event-loop ceiling binds first: ~' + LOOP_CEILING_TASKS_PER_SEC + ' tasks/sec * p80 ' + p80 + 'ms = ' + c.loopCapSlots + ' slots. More slots would complete tasks faster than the single Python event loop can orchestrate. To use the remaining cores, run more workers per host instead. CPU-bound work in arrange()/hooks lowers the real ceiling further.'
      : c.cpu + ' cores - ' + c.reservedCores + ' reserved (20%) = ' + (c.cpu - c.reservedCores) + ' available. Capped at ' + c.partPerWorker + ' partitions/worker * 4 = ' + (c.partPerWorker * 4) + '. Each slot runs one subprocess. More slots than cores causes context-switch overhead without throughput gain.');
    add('window_size: ' + c.windowSize, 'Target ' + c.windowTargetSec + 's of work per window. At p80=' + p80 + 'ms, that is ' + c.windowTargetSec + '/' + p80s.toFixed(3) + ' = ' + c.windowSize + ' messages. Larger windows reduce arrange() call overhead and enable batching. Smaller windows reduce commit latency — offsets commit only when the slowest task in the window finishes.');
    add('task_timeout_seconds: ' + c.taskTimeout, '10x p80 = ' + Math.round(p80s * 10) + 's (floor 30s, cap 600s). Tasks exceeding this are killed. Set above p99 to avoid killing valid tasks, but low enough to detect stuck processes.');
    add('max_poll_records: ' + c.pollRecords, 'window_size * min(partitions/worker, 10) = ' + c.windowSize + ' * ' + Math.min(c.partPerWorker, 10) + '. Enough messages per poll to keep partition queues fed for one window cycle. Too low starves the pipeline; too high wastes memory.');
    var bpText = p80s < 0.1 ? 'Fast tasks drain quickly — small buffer sufficient.' : p80s < 10 ? 'Moderate tasks need room to queue while in-flight work completes.' : 'Long tasks: small buffer prevents over-fetching messages that sit for minutes.';
    add('backpressure high/low: ' + c.highMult + '/' + c.lowMult, 'High watermark = max_executors * ' + c.highMult + ' = ' + (maxWorkers * c.highMult) + '. Low = max_executors * ' + c.lowMult + ' = ' + Math.max(1, maxWorkers * c.lowMult) + '. ' + bpText + ' The gap prevents pause/resume flapping.');
    add('drain_timeout_seconds: ' + c.drainTimeout, '2x p80 = ' + c.drainTimeout + 's. On shutdown, in-flight tasks get this long to finish. If exceeded, their offsets remain uncommitted and Kafka redelivers after restart.');
    var stText = p80s < 1 ? 'Fast tasks: 10s for quick dead-worker detection.' : p80s < 10 ? 'Moderate: 45s balances detection speed vs false positives.' : 'Slow tasks: 60s prevents spurious rebalances.';
    add('session_timeout_ms: ' + c.sessionTimeout, stText + ' Heartbeat every ' + c.heartbeatInterval + 'ms (session/5). Kafka declares consumer dead if no heartbeat for this duration.');
    add('max_poll_interval_ms: ' + c.maxPollInterval, 'Worst-case window: ' + c.windowSize + ' tasks * ' + c.taskTimeout + 's = ' + c.worstWindowSec + 's. 2x margin = ' + (c.worstWindowSec * 2) + 's. Kafka kicks the consumer if no poll() within this interval.');
    add('ws_min_duration_ms: ' + c.wsMin, c.wsMin > 0 ? 'Tasks faster than ' + c.wsMin + 'ms hidden from live UI. Floor is 30ms — below that, fast task WebSocket events cause buggy timeline rendering and browser performance issues. Failed tasks always shown regardless.' : '0 = show all tasks. With p80=' + p80 + 'ms, task throughput is low enough for the UI to handle.');
    add('event_min_duration_ms: ' + c.eventMin, c.eventMin > 0 ? 'Tasks faster than ' + c.eventMin + 'ms skip SQLite storage. At ~' + Math.round(c.tasksPerSec) + ' tasks/sec, filtering prevents DB bottleneck.' : '0 = store all events. SQLite handles ~' + Math.round(c.tasksPerSec) + ' tasks/sec comfortably.');
    add('store_output: ' + c.storeOutput, !c.storeOutput ? 'Disabled — fast task stdout is small and uninteresting. Saves ~' + Math.round(c.stdoutKb * c.tasksPerSec) + ' KB/sec of writes.' : 'Enabled — stdout/stderr valuable for debugging. Storage cost manageable at ' + Math.round(c.tasksPerSec) + ' tasks/sec.');

    var estimates = [
      { term: 'Throughput', text: '~' + c.tasksPerSec.toFixed(1) + ' tasks/sec per worker, ~' + c.tasksPerSecCluster.toFixed(1) + ' tasks/sec cluster (' + c.workers + ' workers * ' + maxWorkers + ' slots / ' + p80s.toFixed(3) + 's)'
        + (c.loopCapped ? ' Per-worker rate sits at the ~' + LOOP_CEILING_TASKS_PER_SEC + ' tasks/sec event-loop ceiling — to go faster, add workers, not slots.' : '') },
      { term: 'Memory (per worker)', text: '~' + Math.round(c.totalEstMb) + ' MB (' + c.baseMb + ' MB runtime + ' + Math.round(c.peakQueuedMb) + ' MB peak queue + ' + Math.round(c.recorderBufferMb) + ' MB recorder buffer)' },
      { term: 'Commit latency', text: '~' + (c.windowSize * p80s).toFixed(1) + 's worst case (' + c.windowSize + ' tasks * ' + p80 + 'ms each)' },
      { term: 'Backpressure threshold', text: 'Pauses at ' + (maxWorkers * c.highMult) + ' queued, resumes at ' + Math.max(1, maxWorkers * c.lowMult) }
    ];
    return [
      { title: 'How each parameter was calculated', items: items },
      { title: 'Estimates', items: estimates }
    ];
  }

  // --------------------------------------------------------- table logic

  function hasKey(object, key) { return Object.prototype.hasOwnProperty.call(object, key); }
  function isBranch(value) { return value !== null && typeof value === 'object' && Object.keys(value).length > 0; }
  function asList(value) { return typeof value === 'string' ? [value] : value; }
  function choiceValues(table, name) { return table.choices[name].values.map(function (item) { return item.value; }); }
  function hasRules(table) { return Boolean(table.rules); }

  /** The answers of an empty link. */
  function defaultAnswers(table) {
    var numbers = {};
    Object.keys(table.inputs).forEach(function (name) { numbers[name] = table.inputs[name]['default']; });
    return {
      numbers: numbers,
      hosts: hasRules(table) ? table.choices.hosts['default'] : 'one',
      services: hasRules(table) ? table.choices.services['default'].slice() : [],
      reg: null,
      elect: null,
      step: 'workload'
    };
  }

  /** The answers with the answers of skipped questions reset: one worker asks no hosts, one host asks no services. */
  function effective(answers) {
    var hosts = answers.numbers.workers >= 2 ? answers.hosts : 'one';
    return {
      numbers: answers.numbers,
      hosts: hosts,
      services: hosts === 'many' ? answers.services : [],
      reg: answers.reg,
      elect: answers.elect,
      step: answers.step
    };
  }

  /** Whether every key of a "when" object matches; a missing key matches anything. */
  function whenMatches(when, answers, resolved) {
    return Object.keys(when).every(function (key) {
      var options = asList(when[key]);
      if (key === 'workers') { return options.indexOf(answers.numbers.workers === 1 ? 'one' : 'many') >= 0; }
      if (key === 'hosts') { return options.indexOf(answers.hosts) >= 0; }
      if (key === 'services_include') { return options.some(function (o) { return answers.services.indexOf(o) >= 0; }); }
      if (key === 'services_exclude') { return !options.some(function (o) { return answers.services.indexOf(o) >= 0; }); }
      return options.indexOf(resolved[key]) >= 0;
    });
  }

  /** The providers of a preference list whose services are all checked, in preference order. */
  function allowedProviders(table, feature, preferences, services) {
    return preferences.filter(function (name) {
      return table.providers[feature][name].needs.every(function (need) { return services.indexOf(need) >= 0; });
    });
  }

  /** Deep-merge a fragment into the target; a leaf set twice is an error. An empty mapping is a leaf. */
  function mergeFragment(target, fragment, path) {
    Object.keys(fragment).forEach(function (key) {
      var here = path ? path + '.' + key : key;
      var value = fragment[key];
      if (!hasKey(target, key)) {
        target[key] = isBranch(value) ? {} : value;
        if (isBranch(value)) { mergeFragment(target[key], value, here); }
        return;
      }
      if (!isBranch(value) || !isBranch(target[key])) { throw new Error('two fragments set ' + here); }
      mergeFragment(target[key], value, here);
    });
  }

  /** The document with its top-level keys in the given order. */
  function ordered(document, order) {
    var result = {};
    Object.keys(document).forEach(function (key) {
      if (order.indexOf(key) < 0) { throw new Error('top-level key not in section_order: ' + key); }
    });
    order.forEach(function (key) { if (hasKey(document, key)) { result[key] = document[key]; } });
    return result;
  }

  /** Resolve the answers to providers, the fleet part of the YAML, the env names and the matching notes. */
  function resolve(table, rawAnswers) {
    var answers = effective(rawAnswers);
    var rule = table.rules.filter(function (row) { return whenMatches(row.when, answers, {}); })[0];
    var registryAllowed = allowedProviders(table, 'registry', rule.registry, answers.services);
    var electionAllowed = allowedProviders(table, 'election', rule.election, answers.services);
    var registry = registryAllowed.indexOf(answers.reg) >= 0 ? answers.reg : registryAllowed[0];
    var election = electionAllowed.indexOf(answers.elect) >= 0 ? answers.elect : electionAllowed[0];
    var resolved = { registry: registry, election: election };
    var notes = table.notes.filter(function (note) { return whenMatches(note.when, answers, resolved); });
    var needed = table.providers.registry[registry].needs.concat(table.providers.election[election].needs);
    var kinds = Object.keys(table.connections).filter(function (kind) { return needed.indexOf(kind) >= 0; });

    var fleet = {};
    mergeFragment(fleet, table.shared[rule.shared].fragment, '');
    kinds.forEach(function (kind) { mergeFragment(fleet, table.connections[kind].fragment, ''); });
    mergeFragment(fleet, table.providers.registry[registry].fragment, '');
    mergeFragment(fleet, table.providers.election[election].fragment, '');
    notes.forEach(function (note) { mergeFragment(fleet, note.fragment || {}, ''); });
    var env = [];
    kinds.forEach(function (kind) { table.connections[kind].env.forEach(function (entry) { env.push(entry); }); });
    return {
      registryAllowed: registryAllowed,
      electionAllowed: electionAllowed,
      registry: registry,
      election: election,
      shared: rule.shared,
      kinds: kinds,
      notes: notes,
      fleet: fleet,
      env: env
    };
  }

  /** The steps the answers show, in order. */
  function shownSteps(table, rawAnswers) {
    var answers = effective(rawAnswers);
    if (!hasRules(table)) { return ['workload', 'result']; }
    var resolution = resolve(table, answers);
    var shown = {
      workload: true,
      hosts: answers.numbers.workers >= 2,
      services: answers.hosts === 'many',
      choose: resolution.registryAllowed.length > 1 || resolution.electionAllowed.length > 1,
      result: true
    };
    return STEPS.filter(function (step) { return shown[step]; });
  }

  // ------------------------------------------------------------ YAML

  /** One scalar as the page prints it. */
  function yamlScalar(value) {
    if (typeof value === 'boolean') { return value ? 'true' : 'false'; }
    if (typeof value === 'number') { return String(value); }
    if (typeof value === 'string') {
      if (BARE_STRING.test(value) && value.charAt(value.length - 1) !== ':' && RESERVED_WORDS.indexOf(value.toLowerCase()) < 0) {
        return value;
      }
      return JSON.stringify(value);
    }
    throw new Error('no YAML form for ' + value);
  }

  /** Two-space indent, a mapping key on its own line, {} for an empty mapping, one trailing newline. */
  function serializeYaml(document) {
    var lines = [];
    function emit(mapping, pad) {
      Object.keys(mapping).forEach(function (key) {
        var value = mapping[key];
        if (isBranch(value)) {
          lines.push(pad + yamlScalar(key) + ':');
          emit(value, pad + '  ');
        } else if (value !== null && typeof value === 'object') {
          lines.push(pad + yamlScalar(key) + ': {}');
        } else {
          lines.push(pad + yamlScalar(key) + ': ' + yamlScalar(value));
        }
      });
    }
    emit(document, '');
    return lines.join('\n') + '\n';
  }

  /** The printed YAML, the calculator values and the resolution (null without a table) for the answers. */
  function buildOutput(table, rawAnswers) {
    var answers = effective(rawAnswers);
    var calculator = computeCalculator(answers.numbers);
    var document = {};
    var resolution = null;
    var order = CALCULATOR_SECTIONS;
    if (hasRules(table)) {
      resolution = resolve(table, answers);
      order = table.section_order;
      mergeFragment(document, table.base, '');
    }
    mergeFragment(document, calculatorFragment(calculator), '');
    if (resolution) { mergeFragment(document, resolution.fleet, ''); }
    return {
      yaml: serializeYaml(ordered(document, order)),
      env: resolution ? resolution.env.map(function (entry) { return entry.name; }) : [],
      calculator: calculator,
      resolution: resolution
    };
  }

  // ------------------------------------------------------------ URL

  function queryKeyOrder(table) {
    return ['v', 'step'].concat(Object.keys(table.inputs), ['hosts', 'svc', 'reg', 'elect']);
  }

  /** Read the answers from a query string; return them with the keys whose values were refused. */
  function parseQuery(table, search) {
    var params = new URLSearchParams(search);
    var defaults = defaultAnswers(table);
    if (params.has('v') && params.get('v') !== '1') { return { answers: defaults, invalid: ['v'] }; }
    var invalid = {};
    var numbers = {};
    Object.keys(table.inputs).forEach(function (name) {
      var spec = table.inputs[name];
      numbers[name] = defaults.numbers[name];
      if (!params.has(name)) { return; }
      var raw = params.get(name);
      if (INTEGER_TEXT.test(raw) && Number(raw) >= spec.min && Number(raw) <= spec.max) {
        numbers[name] = Number(raw);
      } else {
        invalid[name] = true;
      }
    });

    var answers = { numbers: numbers, hosts: defaults.hosts, services: defaults.services, reg: null, elect: null, step: 'workload' };
    if (hasRules(table)) {
      if (numbers.workers >= 2 && params.has('hosts')) {
        if (choiceValues(table, 'hosts').indexOf(params.get('hosts')) >= 0) { answers.hosts = params.get('hosts'); }
        else { invalid.hosts = true; }
      }
      if (answers.hosts === 'many' && params.has('svc')) {
        var values = choiceValues(table, 'services');
        var items = params.get('svc') ? params.get('svc').split(',') : [];
        if (items.every(function (item) { return values.indexOf(item) >= 0; })) {
          answers.services = values.filter(function (value) { return items.indexOf(value) >= 0; });
        } else {
          invalid.svc = true;
        }
      }
      var resolution = resolve(table, answers);
      [['reg', resolution.registryAllowed], ['elect', resolution.electionAllowed]].forEach(function (pair) {
        var key = pair[0], allowed = pair[1];
        if (allowed.length < 2 || !params.has(key)) { return; }
        var raw = params.get(key);
        if (allowed.indexOf(raw) >= 0) { answers[key] = raw === allowed[0] ? null : raw; }
        else { invalid[key] = true; }
      });
    }
    if (params.has('step')) {
      var rawStep = params.get('step');
      if (STEPS.indexOf(rawStep) >= 0) {
        var shown = shownSteps(table, answers);
        answers.step = STEPS.slice(STEPS.indexOf(rawStep)).filter(function (step) { return shown.indexOf(step) >= 0; })[0];
      } else {
        invalid.step = true;
      }
    }
    return { answers: answers, invalid: queryKeyOrder(table).filter(function (key) { return invalid[key]; }) };
  }

  /** The canonical query: v=1, then every answer that differs from its default and is not skipped. */
  function canonicalQuery(table, rawAnswers) {
    var answers = effective(rawAnswers);
    var defaults = defaultAnswers(table);
    var parts = ['v=1'];
    if (answers.step !== 'workload') { parts.push('step=' + answers.step); }
    Object.keys(table.inputs).forEach(function (name) {
      if (answers.numbers[name] !== defaults.numbers[name]) { parts.push(name + '=' + answers.numbers[name]); }
    });
    if (!hasRules(table)) { return parts.join('&'); }
    if (answers.hosts !== defaults.hosts) { parts.push('hosts=' + answers.hosts); }
    if (answers.services.length) {
      parts.push('svc=' + choiceValues(table, 'services').filter(function (v) { return answers.services.indexOf(v) >= 0; }).join(','));
    }
    var resolution = resolve(table, answers);
    if (resolution.registryAllowed.indexOf(answers.reg) > 0) { parts.push('reg=' + answers.reg); }
    if (resolution.electionAllowed.indexOf(answers.elect) > 0) { parts.push('elect=' + answers.elect); }
    return parts.join('&');
  }

  /** A docs link of the table ("fleet.md#consul") as a link from this page ("../fleet/#consul"). */
  function docsHref(link) {
    var parts = link.split('#');
    return '../' + parts[0].replace(/\.md$/, '/') + (parts[1] ? '#' + parts[1] : '');
  }

  var core = {
    STEPS: STEPS,
    computeCalculator: computeCalculator,
    calculatorFragment: calculatorFragment,
    calculatorExplanations: calculatorExplanations,
    defaultAnswers: defaultAnswers,
    effective: effective,
    resolve: resolve,
    shownSteps: shownSteps,
    yamlScalar: yamlScalar,
    serializeYaml: serializeYaml,
    buildOutput: buildOutput,
    parseQuery: parseQuery,
    canonicalQuery: canonicalQuery,
    docsHref: docsHref
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = core;
    return;
  }
  root.drakkarConfigWizard = core;

  // ------------------------------------------------------------ page

  var tablePromise = null;

  /** The table, fetched once per page load of the site; null when it cannot be loaded. */
  function loadTable() {
    if (!tablePromise) {
      var controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
      var timer = controller ? setTimeout(function () { controller.abort(); }, TABLE_TIMEOUT_MS) : null;
      tablePromise = fetch(new URL(TABLE_URL, root.location.href).href, controller ? { signal: controller.signal } : {})
        .then(function (response) {
          if (!response.ok) { throw new Error('status ' + response.status); }
          return response.json();
        })
        .then(function (table) { clearTimeout(timer); return table; })
        .catch(function (error) {
          clearTimeout(timer);
          root.console.warn('config wizard: the rules file could not be loaded:', error);
          tablePromise = null;
          return null;
        });
    }
    return tablePromise;
  }

  /** The number inputs as the page markup declares them, for when the table cannot be loaded. */
  function inputsFromPage(container) {
    var inputs = {};
    Array.prototype.forEach.call(container.querySelectorAll('input[type="number"][data-input]'), function (input) {
      inputs[input.getAttribute('data-input')] = {
        'default': Number(input.defaultValue), min: Number(input.min), max: Number(input.max)
      };
    });
    return inputs;
  }

  function element(tag, attributes, text) {
    var node = document.createElement(tag);
    Object.keys(attributes || {}).forEach(function (name) { node.setAttribute(name, attributes[name]); });
    if (text !== undefined) { node.textContent = text; }
    return node;
  }

  function startWizard(container, loadedTable) {
    var table = loadedTable || { inputs: inputsFromPage(container) };
    var parsed = parseQuery(table, root.location.search);
    var state = parsed.answers;
    var byId = function (id) { return container.querySelector('#' + id); };
    // Run the table paths once before any listener is attached: a broken table then fails
    // here, and the caller can start again without it.
    buildOutput(table, state);
    shownSteps(table, state);
    var refused = {};

    if (parsed.invalid.length) {
      var notice = byId('wizard-notice');
      notice.textContent = 'The link had a value the page does not accept for: ' + parsed.invalid.join(', ') + '. The defaults are used.';
      notice.hidden = false;
    }
    byId('wizard-rules-error').hidden = hasRules(table);

    function writeUrl() {
      // Without the table the page cannot read the fleet answers of the link. It leaves the
      // link as it came, so a reload after the table is back still has every answer.
      if (!hasRules(table)) { return; }
      var before = root.location.href.split('#')[0];
      var url = root.location.pathname + '?' + canonicalQuery(table, state) + root.location.hash;
      root.history.replaceState(root.history.state, '', url);
      // The theme writes the in-page links (table of contents) as absolute URLs when the page
      // loads; move them to the new query so a click does not reload the page's old answers.
      var after = root.location.href.split('#')[0];
      if (after === before) { return; }
      Array.prototype.forEach.call(document.links, function (link) {
        if (link.href.indexOf(before + '#') === 0) { link.href = after + link.href.slice(before.length); }
      });
    }

    function dropPicksNotAllowed() {
      if (!hasRules(table)) { return; }
      var resolution = resolve(table, state);
      if (resolution.registryAllowed.indexOf(state.reg) < 0) { state.reg = null; }
      if (resolution.electionAllowed.indexOf(state.elect) < 0) { state.elect = null; }
    }

    function updateNavigation() {
      var shown = shownSteps(table, state);
      // While a typed number is refused, the other steps would show values for the last
      // accepted number, not the one on screen, so only the workload step can be reached.
      var blocked = Object.keys(refused).length > 0;
      Array.prototype.forEach.call(container.querySelectorAll('.wizard-steps button'), function (button) {
        var step = button.getAttribute('data-step');
        button.hidden = shown.indexOf(step) < 0;
        button.disabled = blocked && step !== 'workload';
        if (step === state.step) { button.setAttribute('aria-current', 'step'); }
        else { button.removeAttribute('aria-current'); }
      });
      var index = shown.indexOf(state.step);
      byId('wizard-back').hidden = index <= 0;
      byId('wizard-next').hidden = index >= shown.length - 1;
      byId('wizard-next').disabled = blocked;
    }

    function changed() {
      dropPicksNotAllowed();
      writeUrl();
      updateNavigation();
    }

    // --- question steps, built from the table

    function radioGroup(target, idPrefix, legend, help, options, current, onPick) {
      target.textContent = '';
      var fieldset = element('fieldset', { 'aria-describedby': idPrefix + '-help' });
      fieldset.appendChild(element('legend', {}, legend));
      fieldset.appendChild(element('p', { id: idPrefix + '-help', 'class': 'wizard-help' }, help));
      options.forEach(function (option) {
        var row = element('div', { 'class': 'wizard-option' });
        var id = idPrefix + '-' + option.value;
        var input = element('input', { type: 'radio', id: id, name: idPrefix, value: option.value });
        if (option.help) { input.setAttribute('aria-describedby', id + '-help'); }
        input.checked = option.value === current;
        input.addEventListener('change', function () { onPick(option.value); });
        row.appendChild(input);
        row.appendChild(element('label', { 'for': id }, option.label));
        if (option.help) { row.appendChild(element('span', { id: id + '-help', 'class': 'hint' }, option.help)); }
        fieldset.appendChild(row);
      });
      target.appendChild(fieldset);
    }

    function buildHosts() {
      var choice = table.choices.hosts;
      radioGroup(byId('wizard-hosts'), 'wizard-hosts', choice.label, choice.help, choice.values, state.hosts,
        function (value) { state.hosts = value; changed(); });
    }

    function buildServices() {
      var choice = table.choices.services;
      var target = byId('wizard-services');
      target.textContent = '';
      var fieldset = element('fieldset', { 'aria-describedby': 'wizard-services-help' });
      fieldset.appendChild(element('legend', {}, choice.label));
      fieldset.appendChild(element('p', { id: 'wizard-services-help', 'class': 'wizard-help' }, choice.help));
      choice.values.forEach(function (option) {
        var row = element('div', { 'class': 'wizard-option' });
        var id = 'wizard-svc-' + option.value;
        var input = element('input', { type: 'checkbox', id: id, value: option.value, 'aria-describedby': id + '-help' });
        input.checked = state.services.indexOf(option.value) >= 0;
        input.addEventListener('change', function () {
          state.services = choice.values.map(function (item) { return item.value; }).filter(function (value) {
            return byId('wizard-svc-' + value).checked;
          });
          changed();
        });
        row.appendChild(input);
        row.appendChild(element('label', { 'for': id }, option.label));
        row.appendChild(element('span', { id: id + '-help', 'class': 'hint' }, option.help));
        fieldset.appendChild(row);
      });
      target.appendChild(fieldset);
    }

    function providerOptions(feature, allowed) {
      return allowed.map(function (name, index) {
        var provider = table.providers[feature][name];
        return { value: name, label: provider.label + (index === 0 ? ' (recommended)' : ''), help: provider.explain };
      });
    }

    function buildChoose() {
      var resolution = resolve(table, state);
      [['registry', 'reg', resolution.registryAllowed, resolution.registry],
        ['election', 'elect', resolution.electionAllowed, resolution.election]].forEach(function (group) {
        var feature = table.features[group[0]];
        var key = group[1];
        radioGroup(byId('wizard-' + key), 'wizard-' + key, feature.label, feature.help,
          providerOptions(group[0], group[2]), group[3],
          function (value) { state[key] = value === group[2][0] ? null : value; changed(); });
      });
    }

    // --- result step

    function addLink(parent, link, topic) {
      parent.appendChild(document.createTextNode(' '));
      parent.appendChild(element('a', { href: docsHref(link), 'aria-label': 'More about ' + topic }, 'More'));
    }

    function addDefinitions(target, title, items) {
      target.appendChild(element('h4', {}, title));
      var list = element('dl');
      items.forEach(function (item) {
        list.appendChild(element('dt', {}, item.term));
        var definition = element('dd', {}, item.text);
        if (item.docs) { addLink(definition, item.docs, item.term); }
        list.appendChild(definition);
      });
      target.appendChild(list);
    }

    function fleetExplanations(resolution) {
      var items = [];
      var shared = table.shared[resolution.shared];
      if (shared.explain) { items.push({ term: 'shared.dir', text: shared.explain, docs: shared.docs }); }
      resolution.kinds.forEach(function (kind) {
        var connection = table.connections[kind];
        items.push({ term: 'connections.' + kind + '.main', text: connection.explain, docs: connection.docs });
      });
      var registry = table.providers.registry[resolution.registry];
      var election = table.providers.election[resolution.election];
      items.push({ term: 'fleet.registry.provider: ' + resolution.registry, text: registry.explain, docs: registry.docs });
      items.push({ term: 'periodic.election.provider: ' + resolution.election, text: election.explain, docs: election.docs });
      resolution.env.forEach(function (entry) { items.push({ term: entry.name, text: entry.explain }); });
      return items;
    }

    function buildResult() {
      var output = buildOutput(table, state);
      var resolution = output.resolution;
      byId('calc-yaml').textContent = output.yaml;

      var warnings = byId('wizard-warnings');
      warnings.textContent = '';
      (resolution ? resolution.notes : []).forEach(function (note) {
        var box = element('div', { 'class': 'admonition warning' });
        var paragraph = element('p', {}, 'Warning: ' + note.text);
        addLink(paragraph, note.docs, 'this warning: ' + note.text.split('. ')[0]);
        box.appendChild(paragraph);
        warnings.appendChild(box);
      });

      byId('wizard-env-section').hidden = !resolution;
      if (resolution) {
        byId('wizard-env-intro').hidden = !resolution.env.length;
        byId('wizard-env-none').hidden = resolution.env.length > 0;
        byId('wizard-env-block').hidden = !resolution.env.length;
        byId('wizard-env').textContent = resolution.env.map(function (entry) {
          return 'export ' + entry.name + "='...'";
        }).join('\n');
      }
      byId('wizard-link').textContent = root.location.href;

      var explain = byId('calc-explain');
      explain.textContent = '';
      calculatorExplanations(output.calculator).forEach(function (group) {
        addDefinitions(explain, group.title, group.items);
      });
      if (resolution) { addDefinitions(explain, 'Fleet', fleetExplanations(resolution)); }
    }

    var builders = { hosts: buildHosts, services: buildServices, choose: buildChoose, result: buildResult };

    function showStep(step, moveFocus) {
      if (step !== 'workload' && Object.keys(refused).length) { return; }
      state.step = step;
      writeUrl();
      // The section is shown before it is filled, so its live region announces the warnings.
      Array.prototype.forEach.call(container.querySelectorAll('section[data-step]'), function (section) {
        section.hidden = section.getAttribute('data-step') !== step;
      });
      if (builders[step]) { builders[step](); }
      byId('wizard-status').textContent = '';
      updateNavigation();
      if (moveFocus) { container.querySelector('section[data-step="' + step + '"] h3').focus(); }
    }

    function stepBy(offset) {
      var shown = shownSteps(table, state);
      var index = shown.indexOf(state.step) + offset;
      if (index >= 0 && index < shown.length) { showStep(shown[index], true); }
    }

    // --- wiring

    Array.prototype.forEach.call(container.querySelectorAll('input[type="number"][data-input]'), function (input) {
      var name = input.getAttribute('data-input');
      var spec = table.inputs[name];
      input.value = String(state.numbers[name]);
      input.addEventListener('input', function () {
        var text = input.value.trim();
        var valid = INTEGER_TEXT.test(text) && Number(text) >= spec.min && Number(text) <= spec.max;
        input.setAttribute('aria-invalid', valid ? 'false' : 'true');
        byId('error-' + name).textContent = valid ? '' : 'Enter a whole number from ' + spec.min + ' to ' + spec.max
          + '. Until then the last accepted value, ' + state.numbers[name] + ', is used and the other steps are locked.';
        if (!valid) {
          refused[name] = true;
          updateNavigation();
          return;
        }
        delete refused[name];
        state.numbers[name] = Number(text);
        changed();
      });
    });
    Array.prototype.forEach.call(container.querySelectorAll('.wizard-steps button'), function (button) {
      button.addEventListener('click', function () { showStep(button.getAttribute('data-step'), true); });
    });
    byId('wizard-back').addEventListener('click', function () { stepBy(-1); });
    byId('wizard-next').addEventListener('click', function () { stepBy(1); });

    function copyFrom(id) {
      var text = id === 'wizard-link' ? root.location.href : byId(id).textContent;
      var status = byId('wizard-status');
      if (!root.navigator.clipboard) {
        status.textContent = 'Copy failed. Select the text and copy it by hand.';
        return;
      }
      root.navigator.clipboard.writeText(text).then(
        function () { status.textContent = 'Copied.'; },
        function () { status.textContent = 'Copy failed. Select the text and copy it by hand.'; }
      );
    }
    Array.prototype.forEach.call(container.querySelectorAll('button[data-copy]'), function (button) {
      button.addEventListener('click', function () { copyFrom(button.getAttribute('data-copy')); });
    });

    byId('wizard-body').hidden = false;
    showStep(state.step, false);
  }

  /** Start the wizard on a page that has it; the theme calls this on every page load, instant ones included. */
  function startOnPage() {
    var container = document.getElementById('calc');
    if (!container || container.getAttribute('data-started')) { return; }
    container.setAttribute('data-started', 'true');
    loadTable().then(function (table) {
      try {
        startWizard(container, table);
      } catch (error) {
        // A table that loads but does not fit the script: run the calculator without it.
        root.console.warn('config wizard: the rules file could not be used:', error);
        startWizard(container, null);
      }
    });
  }

  if (typeof root.document$ !== 'undefined') {
    root.document$.subscribe(startOnPage);
  } else if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', startOnPage);
  } else {
    startOnPage();
  }
})(typeof globalThis !== 'undefined' ? globalThis : this);
