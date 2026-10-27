// The pair form's venue table, made live. Served by the backoffice at /static/pair.js and
// loaded only by pair_form.html; without it the form still works as plain HTML.
//
// Three things happen here, all against the same server that serves the page:
//   1. Once both token addresses are typed, GET /pairs/lookup fills the table: which venues
//      list the pair, how each spells it, a weight from volume, last price and volume.
//   2. Each ticked venue gets a share column (its weight over the sum), and a live mid over
//      a websocket to /pairs/preview: the page says which venues are ticked as that
//      changes, and the updater connects to exactly those, with the code that will quote
//      them. The composite line is what would publish.
//   3. On submit, unticked rows are disabled so they are not posted, which is how a venue
//      is left out of the saved list.
(() => {
  const form = document.querySelector("form.card");
  const table = document.querySelector("table.venues");
  if (!form || !table) return;
  const token0 = form.querySelector('[name="token0"]');
  const token1 = form.querySelector('[name="token1"]');
  const rows = [...table.querySelectorAll("tbody tr")].map((tr) => ({
    tr,
    venue: tr.querySelector("td.mono").textContent.trim(),
    symbol: tr.querySelector('input[name^="symbol_"]'),
    weight: tr.querySelector('input[name^="weight_"]'),
    quote: tr.querySelector('select[name^="quote_"]'),
    note: tr.querySelector("td.note"),
  }));
  const isAddress = (s) => /^0x[0-9a-fA-F]{40}$/.test((s || "").trim());

  // Columns the static page does not have: a tick box, the share, the live mid.
  const head = table.querySelector("thead tr");
  head.insertAdjacentHTML("afterbegin", "<th>");
  head.querySelector("th:nth-child(4)").insertAdjacentHTML("afterend", "<th>share<th>live mid<th>24h volume");
  for (const row of rows) {
    row.tr.insertAdjacentHTML("afterbegin", '<td class=tick><input type=checkbox></td>');
    row.tick = row.tr.querySelector("td.tick input");
    row.tick.checked = row.symbol.value.trim() !== "";
    row.weight.parentElement.insertAdjacentHTML("afterend", "<td class=share></td><td class=live></td><td class=volume></td>");
    row.share = row.tr.querySelector("td.share");
    row.live = row.tr.querySelector("td.live");
    row.volumeCell = row.tr.querySelector("td.volume");
    // The static note column is the no-script fallback; the script's columns replace it.
    row.note.textContent = "";
    row.listed = row.tick.checked;
  }

  // The status line, the composite line and the controls, above the table.
  table.insertAdjacentHTML(
    "beforebegin",
    '<div class=venue-tools>' +
      '<span id=venue-status class=fieldhint></span>' +
      '<span class=venue-buttons>' +
      '<button type=button class=btn id=weights-volume>weights by volume</button>' +
      '<button type=button class=btn id=weights-equal>equal weights</button>' +
      '<label class=check><input type=checkbox id=show-unlisted> show unlisted venues</label>' +
      "</span></div>" +
      '<p id=composite-line class=notice hidden></p>'
  );
  const status = document.getElementById("venue-status");
  const compositeLine = document.getElementById("composite-line");
  const showUnlisted = document.getElementById("show-unlisted");

  const fmt = (n, digits) => (n === null || n === undefined ? "" : Number(n).toLocaleString("en-US", { maximumFractionDigits: digits }));
  const money = (v) => (v >= 1e9 ? `$${(v / 1e9).toFixed(1)}B` : v >= 1e6 ? `$${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `$${(v / 1e3).toFixed(0)}k` : `$${v.toFixed(0)}`);

  function visibility() {
    // Never hide everything: a pair nobody lists still shows the table to fill by hand.
    const anyListed = rows.some((r) => r.listed);
    for (const row of rows) {
      const hide = anyListed && !row.listed && !row.tick.checked && !showUnlisted.checked && lookedUp;
      row.tr.hidden = hide;
    }
  }

  function shares() {
    const ticked = rows.filter((r) => r.tick.checked);
    const sum = ticked.reduce((acc, r) => acc + (parseFloat(r.weight.value) || 1), 0);
    for (const row of rows) {
      row.share.textContent = row.tick.checked && sum > 0 ? `${(((parseFloat(row.weight.value) || 1) / sum) * 100).toFixed(0)}%` : "";
      row.symbol.disabled = false;
      row.weight.disabled = false;
    }
  }

  let lookedUp = false;
  let lastLookup = "";
  // The venues the pair already has, when editing one: the first lookup keeps exactly
  // those ticked rather than picking its own, so opening a pair and saving it changes
  // nothing. A new pair, or new tokens typed in afterwards, get the top venues instead.
  const saved = new Set(rows.filter((r) => r.tick.checked).map((r) => r.venue));
  // How many venues a lookup ticks: the ones with the most 24h volume.
  const TICK_TOP = 3;
  async function lookup() {
    const a = token0.value.trim(), b = token1.value.trim();
    if (!isAddress(a) || !isAddress(b)) return;
    const key = `${a}-${b}`.toLowerCase();
    if (key === lastLookup) return;
    const firstLookup = lastLookup === "";
    lastLookup = key;
    status.textContent = "looking up…";
    const ask = async (x, y) => {
      const res = await fetch(`/pairs/lookup?token0=${encodeURIComponent(x)}&token1=${encodeURIComponent(y)}`);
      return res.json();
    };
    let data;
    try {
      data = await ask(a, b);
    } catch (err) {
      status.textContent = `lookup failed: ${err}`;
      return;
    }
    if (!data.ok) {
      status.textContent = `lookup failed: ${data.error}. Fill the table by hand.`;
      return;
    }
    lookedUp = true;
    // The server found the pair listed only the other way round (a token priced in USDC,
    // never USDC priced in the token) and answered for that orientation: put the addresses
    // that way too, since on a pair being set up nothing else depends on the order yet.
    const swapped = data.swapped === true;
    if (swapped) {
      token0.value = b;
      token1.value = a;
    }
    const keepSaved = firstLookup && saved.size > 0;
    const top = new Set(
      data.venues
        .filter((v) => v.listed)
        .sort((a, b) => (b.volume_usd || 0) - (a.volume_usd || 0))
        .slice(0, TICK_TOP)
        .map((v) => v.venue)
    );
    for (const v of data.venues) {
      const row = rows.find((r) => r.venue === v.venue);
      if (!row) continue;
      row.listed = v.listed;
      row.volume = v.volume_usd;
      // A new pair starts the ticks over: what the previous lookup filled in is not an
      // operator's choice, so an unlisted venue loses its auto-filled symbol and its tick.
      row.tick.dataset.touched = "";
      if (!v.listed) {
        if (row.symbol.dataset.auto === "1") { row.symbol.value = ""; row.weight.value = ""; }
        if (!row.symbol.value.trim()) row.tick.checked = false;
      }
      // Fill what the operator has not typed; never overwrite an edited symbol.
      if (v.listed) {
        if (!row.symbol.value.trim() || row.symbol.dataset.auto === "1") {
          row.symbol.value = v.symbol;
          row.symbol.dataset.auto = "1";
        }
        if (!row.weight.value.trim() || row.weight.dataset.auto === "1") {
          row.weight.value = v.weight;
          row.weight.dataset.auto = "1";
        }
        if (!row.tick.dataset.touched) row.tick.checked = keepSaved ? saved.has(v.venue) : top.has(v.venue);
      }
      // An exchange that did not answer may well list the pair; say that, not "not listed".
      row.volumeCell.textContent = v.volume_usd ? money(v.volume_usd) : v.listed ? "" : v.unreachable ? "couldn't reach it" : "not listed";
    }
    const listedCount = data.venues.filter((v) => v.listed).length;
    status.textContent = `${data.base}/${data.quote}` +
      (swapped ? ` (swapped: venues price ${data.base} in ${data.quote}, not the other way round)` : "") +
      (listedCount === 0 ? " (no venue lists this pair; fill the table by hand)" : "");
    visibility();
    shares();
    preview();
  }

  // Live mids, over one websocket for as long as the page is open. The page tells the
  // server which venues are ticked, one change at a time ("set" a venue's symbol and
  // weight, "remove" it), and the server connects to exactly those: unticking a venue
  // disconnects that venue and nothing else. Twice a second the server sends every
  // ticked venue's mid and the composite.
  let socket = null;
  let socketTokens = "";
  let reopenTimer = null;
  // What the server has been told, by venue: {symbol, weight}.
  let sent = new Map();
  let syncTimer = null;

  const tokensKey = () => `${token0.value.trim()}-${token1.value.trim()}`.toLowerCase();

  function openSocket() {
    clearTimeout(reopenTimer);
    if (socket) { socket.onclose = null; socket.close(); socket = null; }
    sent = new Map();
    for (const row of rows) row.live.textContent = "";
    compositeLine.hidden = true;
    if (!isAddress(token0.value) || !isAddress(token1.value)) { socketTokens = ""; return; }
    socketTokens = tokensKey();
    const scheme = location.protocol === "https:" ? "wss" : "ws";
    const url = `${scheme}://${location.host}/pairs/preview?token0=${encodeURIComponent(token0.value.trim())}&token1=${encodeURIComponent(token1.value.trim())}`;
    const ws = new WebSocket(url);
    socket = ws;
    ws.onopen = () => sync(0);
    ws.onmessage = (e) => {
      const data = JSON.parse(e.data);
      if (data.error) {
        compositeLine.hidden = false;
        compositeLine.textContent = `live prices: ${data.error}`;
        return;
      }
      const seen = new Set();
      for (const v of data.venues) {
        const row = rows.find((r) => r.venue === v.venue);
        if (!row) continue;
        seen.add(v.venue);
        // The preview has no FX legs: a USD venue's mid is shown, and averaged, as read.
        const usd = row.quote && row.quote.value === "USD" ? " (USD, unconverted)" : "";
        row.live.textContent = v.mid === null ? (v.age_ms === null ? "connecting…" : "no price") : fmt(v.mid, 6) + usd + (v.fresh ? "" : " (stale)");
        row.live.classList.toggle("stale", v.mid !== null && !v.fresh);
      }
      for (const row of rows) if (!seen.has(row.venue)) row.live.textContent = sent.has(row.venue) ? "connecting…" : "";
      compositeLine.hidden = data.venues.length === 0;
      const unconverted = rows.some((r) => r.tick.checked && r.quote && r.quote.value === "USD");
      compositeLine.textContent = data.composite === null
        ? `composite: waiting for a fresh venue (${data.fresh} fresh)`
        : `composite mid ${fmt(data.composite, 6)} from ${data.fresh} fresh venue${data.fresh === 1 ? "" : "s"}: ` +
          (unconverted
            ? "USD venues averaged unconverted here; the run divides them by the [pairs.fx] rate"
            : "this is the number that would be published");
    };
    // Dropped, or refused (too many preview pages open): try again in a few seconds and
    // tell the server the ticked venues afresh. Opening the socket connects to no venue.
    ws.onclose = () => {
      if (socket !== ws) return;
      socket = null;
      compositeLine.hidden = false;
      compositeLine.textContent = "live prices dropped; reconnecting in 5s";
      reopenTimer = setTimeout(openSocket, 5000);
    };
  }

  // Tells the server what changed since the last time: new or edited venues are "set",
  // unticked ones "remove". Waits for typing to stop, so a symbol typed letter by letter
  // is sent once.
  function sync(delay = 600) {
    clearTimeout(syncTimer);
    syncTimer = setTimeout(() => {
      if (tokensKey() !== socketTokens) { openSocket(); return; }
      if (!socket || socket.readyState !== WebSocket.OPEN) return;
      const want = new Map();
      for (const row of rows) {
        const symbol = row.symbol.value.trim();
        if (row.tick.checked && symbol) want.set(row.venue, { symbol, weight: row.weight.value.trim() || "1" });
      }
      for (const venue of sent.keys()) {
        if (!want.has(venue)) {
          socket.send(JSON.stringify({ type: "remove", venue }));
          const row = rows.find((r) => r.venue === venue);
          if (row) row.live.textContent = "";
        }
      }
      for (const [venue, v] of want) {
        const was = sent.get(venue);
        if (!was || was.symbol !== v.symbol || was.weight !== v.weight) {
          socket.send(JSON.stringify({ type: "set", venue, symbol: v.symbol, weight: v.weight }));
        }
      }
      sent = want;
    }, delay);
  }
  const preview = () => sync();

  // Presets.
  document.getElementById("weights-equal").addEventListener("click", () => {
    for (const row of rows) if (row.tick.checked) { row.weight.value = "1"; row.weight.dataset.auto = ""; }
    shares(); preview();
  });
  document.getElementById("weights-volume").addEventListener("click", () => {
    const ticked = rows.filter((r) => r.tick.checked && r.volume);
    const max = Math.max(0, ...ticked.map((r) => r.volume));
    for (const row of rows) if (row.tick.checked) {
      row.weight.value = row.volume && max > 0 ? String(Math.max(1, Math.round((row.volume / max) * 100) / 10)) : "1";
      row.weight.dataset.auto = "";
    }
    shares(); preview();
  });
  showUnlisted.addEventListener("change", visibility);

  for (const row of rows) {
    row.tick.addEventListener("change", () => { row.tick.dataset.touched = "1"; visibility(); shares(); preview(); });
    row.symbol.addEventListener("input", () => { row.symbol.dataset.auto = ""; if (row.symbol.value.trim()) { row.tick.checked = true; } shares(); preview(); });
    row.weight.addEventListener("input", () => { row.weight.dataset.auto = ""; shares(); preview(); });
  }
  for (const input of [token0, token1]) {
    input.addEventListener("change", lookup);
    input.addEventListener("blur", lookup);
  }

  // What is not ticked is not posted: the handler reads a blank symbol as "not used".
  form.addEventListener("submit", () => {
    for (const row of rows) if (!row.tick.checked) { row.symbol.value = ""; row.weight.value = ""; }
    if (socket) { socket.onclose = null; socket.close(); }
  });

  shares();
  lookup();
  preview();
})();
