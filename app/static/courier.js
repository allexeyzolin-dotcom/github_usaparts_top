(() => {
  const el = id => document.getElementById(id);
  const node = (tag, text, css) => {
    const element = document.createElement(tag);
    if (text) element.textContent = text;
    if (css) element.className = css;
    return element;
  };
  const dialog = el('scanner');
  let tab = 'active', page = 1, csrf = '', current, serial = 0;
  let readerPromise, controls, scanSession = 0, candidate = '', busy = false;
  const params = new URLSearchParams(location.search);
  const notice = text => { el('notice').textContent = text; el('notice').hidden = !text; };
  const view = id => ['login','queue','detail'].forEach(x => el(x).hidden = x !== id);
  async function api(path, data) {
    const res = await fetch('/courier/' + path, data ? {method:'POST', headers:{
      'Content-Type':'application/json', 'X-Courier-CSRF':csrf}, body:JSON.stringify(data)} : {cache:'no-store'});
    if (res.status === 401) { if (dialog.open) dialog.close(); current = null; view('login'); throw Error('Введіть код підключення від менеджера.'); }
    if (!res.ok) {
      const problem = await res.json().catch(() => ({}));
      throw Error(problem.error || (res.status === 403 ? 'Доступ не підтверджено. Перевірте запрошення.' :
        res.status === 404 ? 'Відправлення більше недоступне. Оновіть список.' : 'Не вдалося отримати дані. Спробуйте ще раз.'));
    }
    const body = await res.json(); if (body.csrf) csrf = body.csrf; return body;
  }
  async function showList() {
    const ticket = ++serial;
    const data = await api('list?tab=' + tab + '&page=' + page);
    if (ticket !== serial) return;
    current = null; view('queue');
    document.querySelectorAll('[data-tab]').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.tab === tab)));
    const rows = data.rows.map(r => {
      const btn = node('button', '', 'row');
      btn.append(node('strong','№' + r.id + ' · ' + (r.name || 'Отримувача не вказано')),
        node('small',[r.city, r.qty + ' шт.', r.ttn && 'ТТН ' + r.ttn].filter(Boolean).join(' · ')));
      btn.onclick = () => openCard(r.id).catch(e => notice(e.message));
      return btn;
    });
    el('rows').replaceChildren(...(rows.length ? rows : [node('p', tab === 'archive' ? 'Архів порожній.' : 'Немає відправлень для кур’єра.')]));
    el('prev').hidden = page === 1; el('next').hidden = !data.more;
  }
  function itemBlock(i) {
    const container = node('article','','item'), info = node('div');
    const fallback = node('span','Фото відсутнє','no-photo');
    if (i.photo) {
      const img = node('img'); img.alt = i.oem; img.loading = 'lazy'; img.referrerPolicy = 'no-referrer';
      const full = node('a'); full.href = i.photo; full.target = '_blank'; full.rel = 'noopener noreferrer'; full.append(img);
      img.onerror = () => full.replaceWith(fallback); img.src = i.photo; container.append(full);
    } else container.append(fallback);
    info.append(node('strong','OEM ' + i.oem + ' · ' + i.qty + ' шт.'),node('p',i.name),
      node('span',[i.warehouse, i.location || 'Місце не вказано'].filter(Boolean).join(' · '),'place'));
    container.append(info);
    if (i.description && i.description !== i.name) {
      const d = node('details'); d.append(node('summary','Опис товару'),node('p',i.description)); container.append(d);
    }
    return container;
  }
  async function openCard(id, autoScan=false) {
    const ticket = ++serial;
    const data = await api('orders/' + id);
    if (ticket !== serial) return;
    current = data.card; view('detail'); notice('');
    el('order-title').textContent = 'Відправлення №' + current.id;
    const dl = node('dl');
    for (const [label, value] of [['Отримувач',current.name],['Телефон',current.phone],
      [current.service,current.address],['Контроль оплати',current.payment + ' грн'], ...(current.ttn ? [['ТТН',current.ttn]] : [])]) {
      const val = node('dd',value || 'Уточніть у менеджера', label === 'Контроль оплати' ? 'money' : '');
      if (label === 'Телефон' && value) { const a = node('a',value); a.href = 'tel:' + value.replace(/[^\d+]/g,''); val.replaceChildren(a); }
      dl.append(node('dt',label),val);
    }
    el('destination').replaceChildren(dl);
    el('items').replaceChildren(...current.items.map(itemBlock)); el('scan').hidden = !!current.ttn;
    window.scrollTo(0,0);
    if (autoScan && !current.ttn) await startScan();
  }
  function reader() {
    if (!readerPromise) readerPromise = new Promise((resolve,reject) => {
      const script = node('script'); script.src = '/static/vendor/zxing-browser-0.1.5.min.js';
      script.onload = () => resolve(new ZXingBrowser.BrowserMultiFormatReader());
      script.onerror = () => {readerPromise = null; reject(Error('Не вдалося завантажити сканер.'));};
      document.head.append(script);
    });
    return readerPromise;
  }
  function stopCamera() {
    scanSession++; if (controls) controls.stop(); controls = null;
    const video = el('video'); if (video.srcObject) video.srcObject.getTracks().forEach(t => t.stop());
    video.srcObject = null;
  }
  function found(value) {
    const ttn = value.trim();
    if (!/^[0-9]{14}$/.test(ttn)) { el('scan-note').textContent = 'Потрібен штрихкод ТТН Нової пошти з 14 цифрами.'; return; }
    candidate = ttn; stopCamera();
    el('decoded').textContent = ttn; el('confirm').hidden = false;
    el('video').hidden = el('file-label').hidden = true;
    el('scan-note').textContent = 'Звірте номер із накладною та отримувачем.';
  }
  async function startScan() {
    if (!current || current.ttn || busy) return;
    stopCamera(); candidate = ''; const ticket = scanSession;
    el('identity').textContent = '№' + current.id + ' · ' + current.name + ' · ' + current.payment + ' грн';
    el('confirm').hidden = true; el('file-label').hidden = el('video').hidden = false;
    el('scan-note').textContent = 'Наведіть задню камеру на штрихкод накладної.';
    if (!dialog.open) dialog.showModal();
    try {
      const scanner = await reader(); if (ticket !== scanSession) return;
      const active = await scanner.decodeFromConstraints({audio:false, video:{facingMode:{ideal:'environment'}, width:{ideal:1280}}},
        el('video'), (result, error, ctl) => {
          if (ticket === scanSession && result) {controls = ctl; found(result.getText());}
        });
      if (ticket !== scanSession) active.stop(); else controls = active;
    } catch (_) {
      if (ticket === scanSession) {el('video').hidden = true; el('scan-note').textContent = 'Камеру не відкрито. Дозвольте доступ до камери або зчитайте штрихкод із фото.';}
    }
  }
  el('barcode-file').onchange = async event => {
    const file = event.target.files[0]; event.target.value = ''; if (!file) return;
    stopCamera(); const ticket = scanSession;
    const image = URL.createObjectURL(file);
    try { const result = await (await reader()).decodeFromImageUrl(image); if (ticket === scanSession) found(result.getText()); }
    catch (_) { if (ticket === scanSession) el('scan-note').textContent = 'Штрихкод не зчитано. Виберіть чіткіше фото або натисніть «Сканувати ще раз».'; }
    finally { URL.revokeObjectURL(image); }
  };
  dialog.onclose = stopCamera;
  el('close').onclick = () => {if (!busy) dialog.close();};
  dialog.oncancel = e => {if (busy) e.preventDefault();};
  el('scan').onclick = el('again').onclick = startScan;
  el('confirm').onsubmit = async event => {
    event.preventDefault(); if (busy || !candidate || !current) return;
    const id = current.id, number = candidate; busy = true;
    const button = el('confirm').querySelector('button'); button.disabled = true;
    try {
      await api('orders/' + id,{ttn:number, version:current.version});
      dialog.close(); tab = 'archive'; page = 1; await showList();
      notice('Відправлення №' + id + ' перенесено в архів. ТТН ' + number + '.');
    } catch (error) {el('scan-note').textContent = error.message;}
    finally {busy = false; button.disabled = false;}
  };
  function loadList() {showList().catch(e => notice(e.message));}
  el('refresh').onclick = el('back').onclick = loadList;
  el('prev').onclick = () => {page--; loadList();}; el('next').onclick = () => {page++; loadList();};
  document.querySelectorAll('[data-tab]').forEach(b => b.onclick = () => {tab=b.dataset.tab; page=1; notice(''); loadList();});
  async function initial() {
    await showList();
    if (/^\d+$/.test(params.get('order') || '')) await openCard(params.get('order'),params.get('scan') === '1');
  }
  el('pair').onsubmit = async event => {
    event.preventDefault();
    try {await api('login',{code:el('code').value}); el('code').value=''; notice(''); await initial();}
    catch (error) {notice(error.message);}
  };
  async function launch() {
    const hash = new URLSearchParams(location.hash.slice(1));
    const code = hash.get('invite'), telegram = hash.get('tgWebAppData');
    if (telegram) {
      const sdk = node('script');
      sdk.src = 'https://telegram.org/js/telegram-web-app.js';
      await new Promise(resolve => {
        sdk.onload = sdk.onerror = resolve;
        document.head.append(sdk);
        setTimeout(resolve, 3500);
      });
      window.Telegram?.WebApp?.ready(); window.Telegram?.WebApp?.expand();
    }
    if (code || telegram) {
      history.replaceState(null,'',location.pathname+location.search);
      try {await api('login',code ? {code} : {telegram});}
      catch (error) {view('login'); notice(error.message); return;}
    }
    await initial();
  }
  launch().catch(e => notice(e.message));
  setInterval(() => {if (!document.hidden && !el('queue').hidden) loadList();},30000);
  document.addEventListener('visibilitychange', () => {
    if (document.hidden && dialog.open && !busy && !candidate) dialog.close();
    if (!document.hidden && !el('queue').hidden) loadList();
  });
})();
