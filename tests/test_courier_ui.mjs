import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const code = readFileSync(new URL('../app/static/courier.js', import.meta.url), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));

// A small DOM double exercises the shipped controller without opening a browser
// or writing a waybill to a real order.
async function setup(options = {}) {
  class Element {
    constructor(tag = 'div') { this.tagName = tag.toUpperCase(); this.children = []; this.hidden = false; this.value = ''; this.textContent = ''; }
    append(...items) { this.children.push(...items); }
    replaceChildren(...items) { this.children = items; }
    setAttribute(name, value) { this[name] = value; }
    get childElementCount() { return this.children.length; }
    showModal() { this.open = true; }
    close() { this.open = false; this.onclose?.(); }
    focus() { this.focused = true; }
    querySelector(selector) { return selector === 'button' ? elements.get('save') : null; }
  }
  const ids = ['scanner','login','queue','detail','notice','rows','prev','next','order-title','destination','items',
    'waybill-actions','scan','manual','video','identity','confirm','file-label','scan-note','ttn','ttn-help','barcode-file',
    'close','again','refresh','back','pair','code','save'];
  const elements = new Map(ids.map(id => [id, new Element(id === 'video' ? 'video' : 'div')]));
  for (const id of ['login','queue','detail']) elements.get(id).hidden = true;
  const listeners = new Map();
  const document = {
    hidden: false,
    getElementById: id => elements.get(id),
    createElement: tag => new Element(tag),
    querySelectorAll: () => [],
    addEventListener: (event, fn) => listeners.set(event, fn),
    head: { append(script) { queueMicrotask(() => script.onload()); } }
  };
  const state = { posts: [], readerLoads: 0, stops: 0, controlStops: 0, archive: false };
  const card = { id: 101, name: 'Тестовий отримувач', phone: '123', payment: '34.20', address: 'Тестове відділення',
    service: 'Відділення', version: 'card-version', ttn: '', items: [] };
  const controls = { stop() { state.controlStops++; } };
  class Reader {
    constructor() { state.readerLoads++; }
    async decodeFromConstraints(constraints, video, callback) {
      state.constraints = constraints; state.callback = callback;
      if (options.cameraError) throw Error('NotAllowedError');
      if (options.cameraPending) await new Promise(resolve => { state.allowCamera = resolve; });
      video.srcObject = { getTracks: () => [{ stop() { state.stops++; } }] };
      return controls;
    }
    async decodeFromImageUrl() { return { getText: () => options.photoValue || '20450000001111' }; }
  }
  const fetch = async (url, init) => {
    if (init?.method === 'POST') {
      state.posts.push({ url, body: JSON.parse(init.body), headers: init.headers });
      if (options.savePending) await new Promise(resolve => { state.finishSave = resolve; });
      if (options.saveError) return { status: 409, ok: false, json: async () => ({ error: 'Дані замовлення змінилися.' }) };
      state.archive = true;
      return { status: 200, ok: true, json: async () => ({ ok: true }) };
    }
    if (url.startsWith('/courier/list')) return { status: 200, ok: true,
      json: async () => ({ csrf: 'csrf-token', rows: [{ id: 101, name: card.name, qty: 1, items: [], ttn: state.archive ? '20450000001111' : '' }], more: false }) };
    return { status: 200, ok: true, json: async () => ({ csrf: 'csrf-token', card }) };
  };
  const context = {
    document, navigator: options.noCamera ? {} : { mediaDevices: { getUserMedia() {} } },
    window: { scrollTo() {} }, location: { search: '', hash: '', pathname: '/courier/' }, history: { replaceState() {} },
    URLSearchParams, URL: { createObjectURL: () => 'blob:fixture', revokeObjectURL() {} },
    fetch, setInterval() {}, setTimeout, console, CourierBarcode: {createReader: () => new Reader()},
    ZXingBrowser: { BrowserMultiFormatReader: Reader, BarcodeFormat: { CODE_128: 4, ITF: 8, CODE_39: 2, QR_CODE: 11 } }
  };
  vm.runInNewContext(code, context);
  await tick();
  await elements.get('rows').children[0].onclick();
  const click = id => elements.get(id).onclick();
  const input = value => { const field = elements.get('ttn'); field.value = value; field.oninput(); };
  const submit = () => elements.get('confirm').onsubmit({ preventDefault() {} });
  const visibility = hidden => { document.hidden = hidden; listeners.get('visibilitychange')(); };
  const scanResult = value => state.callback({ getText: () => value }, null, controls);
  return { elements, state, click, input, submit, visibility, scanResult };
}

test('manual entry works without camera, normalizes separators and saves only after confirmation', async () => {
  const ui = await setup({ noCamera: true });
  ui.click('manual');
  assert.equal(ui.elements.get('ttn').focused, true);
  ui.input('2045 0000-001111');
  assert.equal(ui.elements.get('ttn').value, '20450000001111');
  assert.equal(ui.state.readerLoads, 0);
  assert.equal(ui.state.posts.length, 0);
  await ui.submit();
  assert.equal(ui.state.posts.length, 1);
  assert.deepEqual(ui.state.posts[0].body, { ttn: '20450000001111', version: 'card-version' });
  assert.equal(ui.state.posts[0].headers['X-Courier-CSRF'], 'csrf-token');
  assert.equal(ui.elements.get('scanner').open, false);
  assert.match(ui.elements.get('notice').textContent, /перенесено в архів/);
});

test('wrong length and non-digits cannot be submitted', async () => {
  const ui = await setup(); ui.click('manual');
  for (const number of ['123', '204500000011111', '2045000000111A']) {
    ui.input(number);
    assert.equal(ui.elements.get('save').disabled, true);
    await ui.submit();
  }
  assert.equal(ui.state.posts.length, 0);
});

test('camera result fills the editable field and requires confirmation', async () => {
  const ui = await setup(); await ui.click('scan');
  ui.scanResult('20450000001111');
  assert.equal(ui.elements.get('ttn').value, '20450000001111');
  assert.equal(ui.elements.get('save').disabled, false);
  assert.equal(ui.elements.get('video').hidden, true);
  assert.equal(ui.state.posts.length, 0);
  assert.equal(ui.state.constraints.video.facingMode.ideal, 'environment');
  assert.ok(ui.state.controlStops > 0);
  await ui.submit(); assert.equal(ui.state.posts.length, 1);
});

test('Android permission visibility transition keeps the pending scanner open', async () => {
  const ui = await setup({ cameraPending: true }); const starting = ui.click('scan'); await tick();
  ui.visibility(true);
  assert.equal(ui.elements.get('scanner').open, true);
  ui.visibility(false); ui.state.allowCamera(); await starting;
  ui.scanResult('20450000001111');
  assert.equal(ui.elements.get('ttn').value, '20450000001111');
});

test('photo picker can hide the page and return a decoded waybill to the same form', async () => {
  const ui = await setup(); await ui.click('scan'); ui.visibility(true);
  assert.equal(ui.elements.get('scanner').open, true);
  assert.ok(ui.state.controlStops > 0);
  ui.visibility(false);
  await ui.elements.get('barcode-file').onchange({ target: { files: [{}], value: 'file' } });
  assert.equal(ui.elements.get('ttn').value, '20450000001111');
  assert.equal(ui.elements.get('save').disabled, false);
});

test('denied or unavailable camera leaves manual entry usable', async () => {
  for (const options of [{ cameraError: true }, { noCamera: true }]) {
    const ui = await setup(options); await ui.click('scan');
    assert.equal(ui.elements.get('scanner').open, true);
    assert.match(ui.elements.get('scan-note').textContent, /вручну/);
    ui.input('20450000001111'); await ui.submit();
    assert.equal(ui.state.posts.length, 1);
  }
});

test('manual edits invalidate late camera callbacks', async () => {
  const ui = await setup(); await ui.click('scan'); ui.input('20450000002222');
  ui.scanResult('20450000001111');
  assert.equal(ui.elements.get('ttn').value, '20450000002222');
});

test('closing the form while permission is pending ignores late scan results', async () => {
  const ui = await setup({ cameraPending: true }); const starting = ui.click('scan'); await tick();
  ui.click('close'); ui.state.allowCamera(); await starting; ui.scanResult('20450000001111');
  assert.equal(ui.elements.get('scanner').open, false);
  assert.equal(ui.elements.get('ttn').value, '');
  assert.ok(ui.state.controlStops > 0);
});

test('one pending save blocks duplicate submissions and input changes', async () => {
  const ui = await setup({ savePending: true }); ui.click('manual'); ui.input('20450000001111');
  const saving = ui.submit(); await tick(); await ui.submit();
  assert.equal(ui.state.posts.length, 1);
  assert.equal(ui.elements.get('ttn').disabled, true);
  assert.equal(ui.elements.get('save').disabled, true);
  ui.state.finishSave(); await saving;
  assert.equal(ui.elements.get('ttn').disabled, false);
});

test('server conflict keeps the entered waybill available for review', async () => {
  const ui = await setup({ saveError: true }); ui.click('manual'); ui.input('20450000001111'); await ui.submit();
  assert.equal(ui.elements.get('scanner').open, true);
  assert.equal(ui.elements.get('ttn').value, '20450000001111');
  assert.equal(ui.elements.get('ttn').disabled, false);
  assert.match(ui.elements.get('scan-note').textContent, /змінилися/);
});
