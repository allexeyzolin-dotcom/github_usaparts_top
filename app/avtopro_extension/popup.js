const element = (id) => document.getElementById(id);
const base = "https://usaparts.top";
const endpoint = `${base}/api/integrations/avtopro/order`;
let captured;
let key;

function feedback(message, error = false) {
  element("feedback").textContent = message;
  element("feedback").dataset.error = String(error);
}

async function initialize() {
  await chrome.storage.local.setAccessLevel({ accessLevel: "TRUSTED_CONTEXTS" });
  ({ key } = await chrome.storage.local.get("key"));
  element("settings").open = !key;
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  const url = new URL(tab.url || "about:blank");
  const permitted = ["avtopro.ua", "www.avtopro.ua", "avto.pro", "www.avto.pro"].includes(url.hostname);
  if (url.protocol !== "https:" || !permitted || !/^\/orders\/\d{3,20}\/?$/.test(url.pathname)) {
    return feedback("Відкрийте окрему картку в розділі «Продаж» на Автопро.");
  }
  const [{ result }] = await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ["capture.js"] });
  if (!result || result.error) return feedback(result?.error || "Не вдалося зчитати картку.", true);
  captured = result;
  element("number").textContent = `Замовлення №${result.orderId}`;
  element("summary").textContent = `${result.status} · ${result.totalUah} грн`;
  element("buyer").textContent = [result.customer, result.phone, result.delivery, result.payment].filter(Boolean).join(" · ");
  for (const item of result.items) {
    const li = document.createElement("li");
    li.textContent = `${item.partNumber} · ${item.name} · ${item.qty} шт. × ${item.unitUah} грн`;
    element("items").append(li);
  }
  element("preview").hidden = false;
  element("send").disabled = !key;
  feedback(key ? "Перевірте дані й передайте замовлення." : "Збережіть ключ із кабінету USAparts.top.");
}

element("key-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const value = element("key").value.trim();
  if (!/^[A-Za-z0-9_-]{43}$/.test(value)) return feedback("Перевірте ключ підключення.", true);
  try {
    await chrome.storage.local.set({ key: value });
    key = value;
    element("key").value = "";
    element("settings").open = false;
    element("send").disabled = !captured;
    feedback(captured ? "Перевірте дані й передайте замовлення." : "Ключ збережено. Відкрийте картку продажу.");
  } catch {
    feedback("Не вдалося зберегти ключ.", true);
  }
});

element("send").addEventListener("click", async () => {
  const button = element("send");
  if (!captured || !key || button.disabled) return;
  button.disabled = true;
  feedback("Передаю в кабінет…");
  try {
    const response = await fetch(endpoint, {
      method: "POST", credentials: "omit", redirect: "error", cache: "no-store",
      signal: AbortSignal.timeout(15000),
      headers: { "Authorization": `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify(captured),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || "Кабінет відхилив імпорт.");
    const url = new URL(result.orderUrl);
    if (url.origin !== base || url.pathname !== "/admin/orders") throw new Error("Некоректна відповідь кабінету.");
    const messages = { created: "Замовлення додано у видачу.", updated: "Дані Автопро оновлено для звірки.", unchanged: "Цю картку вже передано." };
    feedback(messages[result.result] || "Передачу завершено.");
    element("order").href = url.href;
    element("order").hidden = false;
  } catch (error) {
    feedback(`Передачу не завершено. ${error.message}`, true);
  } finally {
    button.disabled = false;
  }
});

initialize().catch(() => feedback("Не вдалося відкрити картку. Перезавантажте вкладку й повторіть.", true));
