/*
 * Pipeline map of the docs page pipeline-map/.
 *
 * The page lets a reader choose an input form (Kafka with arrange(), Kafka
 * with arrange_stream(), HTTP) and a step of the path of a message. It shows
 * which hooks run for the chosen form and what can fail at the chosen step.
 *
 * Every fact comes from data/pipeline-map.json, which a test checks against
 * the code. This script only reads that file and draws it; it holds no rule
 * of its own. The view (form and step) is kept in the query of the address,
 * so a link opens the same view.
 */
(function (root) {
  'use strict';

  var DATA_URL = '../data/pipeline-map.json';
  // The steps drawn beside the main path: they are reached from any step, not one after another.
  var SIDE_STEPS = ['dlq-write', 'stop'];
  var DEFAULT_STEP = 'arrange';

  // ------------------------------------------------------------ helpers

  /** A new element with a class and optional text. */
  function el(tag, className, text) {
    var node = document.createElement(tag);
    if (className) { node.className = className; }
    if (text !== undefined) { node.textContent = text; }
    return node;
  }

  /** Fill an element with text in which `backticks` mark code. Text only, never HTML. */
  function rich(node, text) {
    node.textContent = '';
    String(text).split('`').forEach(function (part, index) {
      if (!part) { return; }
      node.appendChild(index % 2 ? el('code', '', part) : document.createTextNode(part));
    });
    return node;
  }

  function byId(list, id) {
    for (var i = 0; i < list.length; i += 1) { if (list[i].id === id) { return list[i]; } }
    return null;
  }

  /** The forms an outcome applies to: its own list, else every form of its step. */
  function outcomeForms(step, outcome) { return outcome.forms || step.forms; }

  function appliesTo(entry, formId) { return !entry.forms || entry.forms.indexOf(formId) >= 0; }

  /** Every hook name in the order of first appearance over the forms. */
  function allHookNames(data) {
    var names = [];
    data.forms.forEach(function (form) {
      form.hooks.forEach(function (hook) { if (names.indexOf(hook.name) < 0) { names.push(hook.name); } });
    });
    return names;
  }

  function hookOf(form, name) {
    for (var i = 0; i < form.hooks.length; i += 1) { if (form.hooks[i].name === name) { return form.hooks[i]; } }
    return null;
  }

  function stepTitle(step, formId) { return (step.titles_by_form && step.titles_by_form[formId]) || step.title; }

  /** The offset words of a step's outcomes for a form, in a fixed order. */
  function offsetsOf(data, step, formId) {
    var words = Object.keys(data.offset_words);
    var seen = {};
    step.outcomes.forEach(function (outcome) {
      if (outcomeForms(step, outcome).indexOf(formId) >= 0 && outcome.offset && formId !== 'http') {
        seen[outcome.offset] = true;
      }
    });
    return words.filter(function (word) { return seen[word]; });
  }

  // ------------------------------------------------------------ the view state in the address

  function readState(data) {
    var query = new URLSearchParams(root.location.search);
    var form = query.get('form');
    var step = query.get('step');
    return {
      form: byId(data.forms, form) ? form : data.forms[0].id,
      step: byId(data.steps, step) ? step : DEFAULT_STEP
    };
  }

  function writeState(state) {
    var url = new URL(root.location.href);
    url.searchParams.set('form', state.form);
    url.searchParams.set('step', state.step);
    root.history.replaceState(root.history.state, '', url.toString());
  }

  // ------------------------------------------------------------ drawing

  function buildFormChooser(data, onChange) {
    var fieldset = el('fieldset', 'pmap-forms');
    fieldset.appendChild(el('legend', '', 'Input form'));
    data.forms.forEach(function (form) {
      var label = el('label', 'pmap-form');
      var input = el('input');
      input.type = 'radio';
      input.name = 'pmap-form';
      input.value = form.id;
      input.addEventListener('change', function () { if (input.checked) { onChange(form.id); } });
      var text = el('span', 'pmap-form-text');
      text.appendChild(rich(el('span', 'pmap-form-title'), form.title));
      text.appendChild(el('span', 'pmap-form-unit', 'Unit: ' + form.unit));
      label.appendChild(input);
      label.appendChild(text);
      fieldset.appendChild(label);
    });
    return fieldset;
  }

  function buildStepButton(step, onSelect) {
    var button = el('button', 'pmap-step');
    button.type = 'button';
    button.setAttribute('data-step', step.id);
    button.setAttribute('aria-pressed', 'false');
    button.appendChild(el('span', 'pmap-step-title'));
    button.appendChild(el('span', 'pmap-dots'));
    button.appendChild(el('span', 'pmap-chips'));
    button.addEventListener('click', function () { onSelect(step.id, false); });
    return button;
  }

  function buildPath(data, onSelect) {
    var wrap = el('div', 'pmap-flow');
    var path = el('ol', 'pmap-path');
    path.setAttribute('aria-label', 'Path of a message');
    path.appendChild(el('li', 'pmap-cap pmap-cap-start'));
    data.steps.forEach(function (step) {
      if (SIDE_STEPS.indexOf(step.id) >= 0) { return; }
      var item = el('li', 'pmap-node');
      item.appendChild(buildStepButton(step, onSelect));
      path.appendChild(item);
    });
    path.appendChild(el('li', 'pmap-cap pmap-cap-end'));
    wrap.appendChild(path);

    var side = el('div', 'pmap-side');
    side.appendChild(el('span', 'pmap-side-label', 'Can happen at any step:'));
    data.steps.forEach(function (step) {
      if (SIDE_STEPS.indexOf(step.id) >= 0) { side.appendChild(buildStepButton(step, onSelect)); }
    });
    wrap.appendChild(side);

    // The arrow keys move between the step buttons, as Tab does.
    wrap.addEventListener('keydown', function (event) {
      var forward = event.key === 'ArrowDown' || event.key === 'ArrowRight';
      if (!forward && event.key !== 'ArrowUp' && event.key !== 'ArrowLeft') { return; }
      var buttons = Array.prototype.slice.call(wrap.querySelectorAll('.pmap-step'));
      var index = buttons.indexOf(document.activeElement);
      if (index < 0) { return; }
      event.preventDefault();
      var next = (index + (forward ? 1 : buttons.length - 1)) % buttons.length;
      buttons[next].focus();
    });
    return wrap;
  }

  function buildLegend(data) {
    var legend = el('dl', 'pmap-legend');
    Object.keys(data.offset_words).forEach(function (word) {
      var term = el('dt');
      term.appendChild(el('span', 'pmap-badge pmap-offset-' + word, word));
      legend.appendChild(term);
      legend.appendChild(el('dd', '', data.offset_words[word]));
    });
    return legend;
  }

  function drawPath(container, data, state) {
    var form = byId(data.forms, state.form);
    container.querySelector('.pmap-cap-start').textContent = form.start;
    container.querySelector('.pmap-cap-end').textContent = form.end;
    Array.prototype.forEach.call(container.querySelectorAll('.pmap-step'), function (button) {
      var step = byId(data.steps, button.getAttribute('data-step'));
      var title = stepTitle(step, form.id);
      button.querySelector('.pmap-step-title').textContent = title;
      button.setAttribute('aria-pressed', step.id === state.step ? 'true' : 'false');

      var dots = button.querySelector('.pmap-dots');
      dots.textContent = '';
      var words = offsetsOf(data, step, form.id);
      words.forEach(function (word) { dots.appendChild(el('span', 'pmap-dot pmap-offset-' + word, word)); });

      var chips = button.querySelector('.pmap-chips');
      chips.textContent = '';
      var hooks = form.hooks.filter(function (hook) { return hook.step === step.id; });
      hooks.forEach(function (hook) { chips.appendChild(el('span', 'pmap-chip', hook.name)); });

      var label = title + '. ' + (hooks.length ? 'Hooks: ' + hooks.map(function (h) { return h.name; }).join(', ') + '. ' : '')
        + (words.length ? 'Offset outcomes: ' + words.join(', ') + '.' : '');
      button.setAttribute('aria-label', label);
    });
  }

  function drawDetail(panel, data, state) {
    var form = byId(data.forms, state.form);
    var step = byId(data.steps, state.step);
    panel.textContent = '';

    var heading = el('h3', 'pmap-detail-title', stepTitle(step, form.id));
    heading.tabIndex = -1;
    panel.appendChild(heading);
    panel.appendChild(el('p', 'pmap-detail-form', 'For: ' + form.title));
    panel.appendChild(rich(el('p'), step.summary));

    var decided = step.decided_by.filter(function (entry) { return appliesTo(entry, form.id); });
    if (decided.length) {
      var row = el('p', 'pmap-decided');
      row.appendChild(el('span', 'pmap-decided-label', 'Decided by: '));
      decided.forEach(function (entry) {
        var chip = el('span', 'pmap-ref pmap-ref-' + entry.kind);
        chip.appendChild(el('span', 'pmap-ref-kind', entry.kind === 'config' ? 'config' : 'hook'));
        chip.appendChild(el('code', '', entry.ref));
        row.appendChild(chip);
      });
      panel.appendChild(row);
    }

    var list = el('ul', 'pmap-outcomes');
    step.outcomes.forEach(function (outcome) {
      if (outcomeForms(step, outcome).indexOf(form.id) < 0) { return; }
      var isHttp = form.id === 'http';
      var item = el('li', 'pmap-outcome ' + (isHttp ? 'pmap-offset-http' : 'pmap-offset-' + outcome.offset));
      item.appendChild(rich(el('p', 'pmap-when'), outcome.when));
      var result = (outcome.forms_result && outcome.forms_result[form.id]) || outcome.result;
      item.appendChild(rich(el('p', 'pmap-result'), result));
      var foot = el('p', 'pmap-foot');
      if (isHttp) {
        foot.appendChild(el('span', 'pmap-badge pmap-offset-http', 'response'));
        foot.appendChild(rich(el('span', 'pmap-foot-text'), outcome.response));
      } else {
        var badge = el('span', 'pmap-badge pmap-offset-' + outcome.offset, 'offset ' + outcome.offset);
        badge.title = data.offset_words[outcome.offset];
        foot.appendChild(badge);
      }
      item.appendChild(foot);
      var record = el('p', 'pmap-record');
      record.appendChild(el('span', 'pmap-record-label', 'Record: '));
      record.appendChild(rich(el('span'), outcome.record));
      item.appendChild(record);
      list.appendChild(item);
    });
    panel.appendChild(list);

    // The meaning of each offset word of this step, once, below its outcomes.
    var words = offsetsOf(data, step, form.id);
    if (words.length) {
      var legend = el('p', 'pmap-step-legend');
      words.forEach(function (word) {
        var line = el('span');
        line.appendChild(el('span', 'pmap-badge pmap-offset-' + word, word));
        line.appendChild(document.createTextNode(' ' + data.offset_words[word]));
        legend.appendChild(line);
      });
      panel.appendChild(legend);
    }
  }

  function drawHooks(grid, data, state, onSelect) {
    var form = byId(data.forms, state.form);
    grid.textContent = '';
    // The hooks that run with the chosen form come first, the others after them, dimmed.
    var names = allHookNames(data);
    var ordered = names.filter(function (name) { return hookOf(form, name); })
      .concat(names.filter(function (name) { return !hookOf(form, name); }));
    ordered.forEach(function (name) {
      var hook = hookOf(form, name);
      var card = el('article', 'pmap-hook' + (hook ? ' is-on' : ' is-off'));
      if (hook && hook.step === state.step) { card.className += ' is-here'; }
      card.appendChild(el('h4', 'pmap-hook-name', name));
      if (hook) {
        var facts = el('dl', 'pmap-hook-facts');
        [['Runs', hook.runs], ['Receives', hook.receives], ['Kept in memory until it runs', hook.kept_until_it_runs]]
          .forEach(function (pair) {
            facts.appendChild(el('dt', '', pair[0]));
            facts.appendChild(rich(el('dd'), pair[1]));
          });
        card.appendChild(facts);
        var go = el('button', 'pmap-hook-go', 'Show the step: ' + stepTitle(byId(data.steps, hook.step), form.id));
        go.type = 'button';
        go.addEventListener('click', function () { onSelect(hook.step, true); });
        card.appendChild(go);
      } else {
        var others = data.forms.filter(function (f) { return hookOf(f, name); }).map(function (f) { return f.short; });
        card.appendChild(el('p', 'pmap-hook-off', 'Does not run with this form. Runs with: ' + others.join(', ') + '.'));
      }
      grid.appendChild(card);
    });
  }

  function drawNotes(box, data, state) {
    var form = byId(data.forms, state.form);
    box.textContent = '';
    box.appendChild(rich(el('p', 'pmap-summary'), form.summary));
    var list = el('ul');
    form.notes.forEach(function (note) { list.appendChild(rich(el('li'), note)); });
    box.appendChild(list);
  }

  // ------------------------------------------------------------ start

  function startMap(container, data) {
    var state = readState(data);

    function select(stepId, moveFocus) {
      state.step = stepId;
      render();
      if (moveFocus) {
        container.querySelector('.pmap-detail-title').focus();
      } else if (isOneColumn()) {
        // On a narrow page the outcomes are below the whole path: bring them into view.
        detail.scrollIntoView({ block: 'start' });
      }
    }

    function isOneColumn() {
      var stage = container.querySelector('.pmap-stage');
      return root.getComputedStyle(stage).gridTemplateColumns.split(' ').length < 2;
    }

    function chooseForm(formId) {
      state.form = formId;
      render();
    }

    var chooser = buildFormChooser(data, chooseForm);
    var notes = el('div', 'pmap-notes');
    var flow = buildPath(data, select);
    var detail = el('section', 'pmap-detail');
    detail.setAttribute('aria-live', 'polite');
    detail.setAttribute('aria-label', 'What can fail at the chosen step');
    var hooksSection = el('section', 'pmap-hooks');
    hooksSection.appendChild(el('h3', '', 'Hooks of the chosen form'));
    hooksSection.appendChild(el('p', 'pmap-hooks-help', 'Bright cards run with the chosen form; dim cards do not. A marked card runs at the chosen step.'));
    var grid = el('div', 'pmap-hook-grid');
    hooksSection.appendChild(grid);

    container.appendChild(chooser);
    container.appendChild(notes);
    // The path and the chosen step sit side by side when the page is wide enough.
    var stage = el('div', 'pmap-stage');
    stage.appendChild(flow);
    stage.appendChild(detail);
    container.appendChild(stage);
    container.appendChild(hooksSection);
    var legendBox = el('details', 'pmap-legend-box');
    legendBox.appendChild(el('summary', '', 'What the offset words mean'));
    legendBox.appendChild(buildLegend(data));
    container.appendChild(legendBox);

    function render() {
      writeState(state);
      Array.prototype.forEach.call(chooser.querySelectorAll('input'), function (input) {
        input.checked = input.value === state.form;
        input.parentNode.classList.toggle('is-chosen', input.checked);
      });
      container.setAttribute('data-form', state.form);
      drawNotes(notes, data, state);
      drawPath(container, data, state);
      drawDetail(detail, data, state);
      drawHooks(grid, data, state, select);
    }

    render();
    container.hidden = false;
  }

  /** Start the map on a page that has it; the theme calls this on every page load, instant ones included. */
  function startOnPage() {
    var container = document.getElementById('pipeline-map-app');
    if (!container || container.getAttribute('data-started')) { return; }
    container.setAttribute('data-started', 'true');
    root.fetch(DATA_URL)
      .then(function (response) {
        if (!response.ok) { throw new Error('HTTP ' + response.status); }
        return response.json();
      })
      .then(function (data) { startMap(container, data); })
      .catch(function (error) {
        // The diagram and the tables of the page still show every fact.
        root.console.warn('pipeline map: the data file could not be used:', error);
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
