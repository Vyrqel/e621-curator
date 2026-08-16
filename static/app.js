const $ = (id) => document.getElementById(id);

const els = {
  image: $("image"),
  placeholder: $("placeholder"),
  loading: $("loading"),
  error: $("error"),
  meta: $("meta"),
  metaQuery: $("meta-query"),
  metaLink: $("meta-link"),
  tagsArtists: $("tags-artists"),
  tagsCharacters: $("tags-characters"),
  btnNext: $("btn-next"),
  btnFav: $("btn-fav"),
  btnBack: $("btn-back"),
  favLabel: $("fav-label"),
  overrideInput: $("override-input"),
  overridePersist: $("override-persist"),
  persistToggleWrap: $("persist-toggle-wrap"),
  overrideClear: $("override-clear"),
  suggest: $("suggest"),
  reviewMode: $("review-mode"),
  reviewCounter: $("review-counter"),
  reviewIndexInput: $("review-index-input"),
  reviewCountTotal: $("review-count-total"),
  statSeen: $("stat-seen"),
  statFav: $("stat-fav"),
  statAdd: $("stat-add"),
  statQueries: $("stat-queries"),
  statBlacklist: $("stat-blacklist"),
  statExhausted: $("stat-exhausted"),
  statPrimed: $("stat-primed"),
};

let current = null;
let buffer = null;        // The next post, prefetched and image-preloaded
let bufferLoading = null; // Promise resolving when buffer is ready
let busy = false;
let inHistory = false;    // True when we're stepping back through seen history

// Review mode state — index-based navigation through ordered list of seen IDs
let reviewList = null;    // Array of post IDs, in chronological seen_at order
let reviewIndex = 0;      // Current position in reviewList
let reviewLoading = false; // True while building/rebuilding reviewList

async function refreshStats() {
  try {
    const r = await fetch("/api/stats");
    const s = await r.json();
    els.statSeen.textContent = s.seen.toLocaleString();
    els.statFav.textContent = s.favorites.toLocaleString();
    els.statAdd.textContent = s.additions.toLocaleString();
    els.statQueries.textContent = s.queries.toLocaleString();
    els.statBlacklist.textContent = s.blacklist.toLocaleString();
    els.statExhausted.textContent = s.exhausted.toLocaleString();
    els.statPrimed.textContent = s.primed.toLocaleString();
  } catch (e) { /* ignore */ }
}

function showError(msg) {
  els.error.textContent = msg;
  els.error.hidden = false;
  els.image.hidden = true;
  els.placeholder.hidden = true;
  els.loading.hidden = true;
}

function showLoading() {
  els.loading.hidden = false;
  els.error.hidden = true;
  els.image.hidden = true;
  els.placeholder.hidden = true;
  els.meta.hidden = true;
}

function setFavoriteState(isFavorited) {
  if (isFavorited) {
    els.btnFav.classList.add("is-favorited");
    els.favLabel.textContent = "favorited";
  } else {
    els.btnFav.classList.remove("is-favorited");
    els.favLabel.textContent = "favorite";
  }
}

function renderTags(container, tagList, category) {
  container.innerHTML = "";
  if (!tagList.length) {
    container.innerHTML = '<span class="meta-value" style="opacity:0.5">— none —</span>';
    return;
  }
  for (const t of tagList) {
    const el = document.createElement("span");
    el.className = "tag";
    el.textContent = t.tag;
    if (t.known) {
      el.classList.add("known");
      el.title = "Already tracked";
    } else {
      el.title = `Click to add to ${category} additions (click again to remove)`;
      el.addEventListener("click", () => toggleAddition(el, t.tag, category));
    }
    container.appendChild(el);
  }
}

async function toggleAddition(el, tag, category) {
  if (el.classList.contains("known")) return;

  const isAdded = el.classList.contains("added");
  const endpoint = isAdded ? "/api/addition/remove" : "/api/addition";

  try {
    const r = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ tag, category }),
    });
    const data = await r.json();

    if (isAdded) {
      if (data.ok) {
        el.classList.remove("added");
        refreshStats();
      }
    } else {
      if (data.ok) {
        el.classList.add("added");
        refreshStats();
      } else if (data.reason === "already_known") {
        el.classList.add("known");
      }
    }
  } catch (e) {
    console.error(e);
  }
}

function buildNextUrl() {
  const override = els.overrideInput.value.trim();
  const params = new URLSearchParams();
  if (override) {
    params.set("override", override);
    params.set("persist", els.overridePersist.checked ? "1" : "0");
  }
  // Exclude the currently-buffered post so we don't double-serve it
  if (buffer && buffer.id) {
    params.set("exclude", String(buffer.id));
  }
  // Also exclude the current post (it might not be marked seen yet if
  // mark-seen request is still in flight)
  if (current && current.id) {
    const existing = params.get("exclude");
    params.set("exclude", existing ? `${existing},${current.id}` : String(current.id));
  }
  const qs = params.toString();
  return qs ? `/api/next?${qs}` : "/api/next";
}

/**
 * Show or hide the review counter UI based on whether we're in review mode
 * and have a list loaded.
 */
function updateReviewCounterUI() {
  if (isReviewMode() && reviewList && reviewList.length > 0) {
    els.reviewCounter.hidden = false;
    els.reviewIndexInput.value = String(reviewIndex + 1);
    els.reviewIndexInput.max = String(reviewList.length);
    els.reviewCountTotal.textContent = String(reviewList.length);
  } else {
    els.reviewCounter.hidden = true;
  }
}

/**
 * Fetch the ordered review list from the backend, applying the current
 * override (if any) as a filter.
 */
async function loadReviewList() {
  reviewLoading = true;
  els.reviewCountTotal.textContent = "…";
  els.reviewCounter.hidden = false;

  try {
    const override = els.overrideInput.value.trim();
    const params = new URLSearchParams();
    if (override) params.set("override", override);
    const qs = params.toString();
    const url = qs ? `/api/review/list?${qs}` : "/api/review/list";

    const r = await fetch(url);
    const data = await r.json();
    if (!r.ok) {
      reviewList = null;
      reviewIndex = 0;
      showError(data.error || "Failed to load review list.");
      els.reviewCounter.hidden = true;
      return false;
    }

    reviewList = data.ids;
    reviewIndex = 0;
    if (reviewList.length === 0) {
      let msg = override
        ? "No seen posts match that override query."
        : "No seen posts to review yet.";
      if (override && data.uncached > 0) {
        msg += ` (${data.uncached} seen posts have no cached tags yet — browse them once to enable filtering.)`;
      }
      showError(msg);
      els.reviewCounter.hidden = true;
      return false;
    }
    updateReviewCounterUI();

    // If there's an override and uncached posts exist, log it to console
    // so the user knows the filter result is partial.
    if (override && data.uncached > 0) {
      console.info(
        `Review filter: ${data.ids.length} matched, ${data.uncached} ` +
        `seen posts uncached (browse them to populate the cache).`
      );
    }
    return true;
  } finally {
    reviewLoading = false;
  }
}

function isReviewMode() {
  return els.reviewMode.checked;
}

/**
 * Tell the backend to mark a post as seen. Fire-and-forget; we don't
 * block the UI on this — if it fails the post just stays unseen and
 * might come back later, which is fine.
 */
function markSeen(postId) {
  if (!postId) return;
  const body = { post_id: postId };
  if (current && current.query) body.query = current.query;
  if (current && current.from_primed) body.from_primed = true;
  fetch("/api/seen", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).catch((e) => console.error("mark seen failed:", e));
}

/**
 * Fetch a post and preload its image. Returns {data, img} or {error}.
 * Does NOT modify the page state — caller decides when to render.
 *
 * In review mode: fetches the post at reviewIndex from the loaded reviewList.
 * In normal mode: routes through /api/next with override/exclude params.
 */
async function fetchAndPreload() {
  try {
    let r;
    if (isReviewMode()) {
      if (!reviewList || reviewList.length === 0) {
        return { error: "Review list not loaded." };
      }
      const postId = reviewList[reviewIndex];
      r = await fetch("/api/review", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ post_id: postId }),
      });
    } else {
      r = await fetch(buildNextUrl());
    }
    const data = await r.json();
    if (!r.ok) return { error: data.error || "Unknown error" };

    // Preload the image off-screen so it's ready when rendered
    await new Promise((resolve, reject) => {
      const img = new Image();
      img.onload = resolve;
      img.onerror = () => reject(new Error("Failed to load image."));
      img.src = data.sample_url;
    });

    return { data };
  } catch (e) {
    return { error: `Network error: ${e.message}` };
  }
}

/**
 * Render an already-fetched post into the UI immediately.
 *
 * Marking seen happens at display time so close-the-tab behavior is correct:
 * if you see it on screen, it's seen; if you don't, it isn't.
 *
 * EXCEPT: review-mode posts are tracked in a session-scoped Set rather
 * than the persistent seen table. History posts skip marking too — they're
 * already seen and we don't want to overwrite seen_at.
 */
function renderPost(data) {
  current = data;
  els.image.src = data.sample_url;
  els.image.hidden = false;
  els.loading.hidden = true;
  els.error.hidden = true;
  els.placeholder.hidden = true;

  els.metaQuery.textContent = data.query;
  let prefix = "";
  if (data.is_history) prefix = "↶ ";
  else if (data.is_review) prefix = "↻ ";
  const primedSuffix = data.from_primed ? "  ★ new" : "";
  els.metaLink.textContent = `${prefix}#${data.id}${primedSuffix}`;
  els.metaLink.href = data.post_url;
  renderTags(els.tagsArtists, data.artists, "artist");
  renderTags(els.tagsCharacters, data.characters, "character");
  els.meta.hidden = false;

  setFavoriteState(!!data.is_favorited);

  if (data.is_review) {
    // Index navigation is managed externally; just keep the counter UI fresh
    updateReviewCounterUI();
  } else if (!data.is_history) {
    // Normal forward navigation: mark seen
    markSeen(data.id);
  }
  // History posts: don't mark — they're already seen and INSERT OR IGNORE
  // would be a no-op anyway, but the explicit branch makes intent clear.
}

/**
 * Kick off a background fetch for the next post, store it in `buffer`.
 * Safe to call concurrently — multiple calls share the same in-flight fetch.
 */
function preloadNext() {
  if (bufferLoading) return bufferLoading;
  bufferLoading = (async () => {
    const result = await fetchAndPreload();
    if (result.data) {
      buffer = result.data;
    } else {
      // Don't store errors — let the foreground fetch surface them
      buffer = null;
    }
    bufferLoading = null;
  })();
  return bufferLoading;
}

/**
 * In review mode, fetch and render the post at the given index.
 * Handles bounds checking and updates the counter.
 */
async function reviewGoToIndex(targetIndex) {
  if (!reviewList || reviewList.length === 0) return;
  // Clamp to valid range
  const clamped = Math.max(0, Math.min(reviewList.length - 1, targetIndex));
  reviewIndex = clamped;
  updateReviewCounterUI();

  showLoading();
  const result = await fetchAndPreload();
  if (result.error) {
    showError(result.error);
    return;
  }
  renderPost(result.data);
  refreshStats();
}

async function fetchNext() {
  if (busy) return;
  busy = true;
  els.btnNext.disabled = true;

  try {
    // Review mode: step forward in the index
    if (isReviewMode()) {
      if (!reviewList) {
        // List not loaded yet — load it now
        const ok = await loadReviewList();
        if (!ok) return;
        await reviewGoToIndex(0);
        return;
      }
      if (reviewIndex >= reviewList.length - 1) {
        showError("End of review list reached.");
        return;
      }
      await reviewGoToIndex(reviewIndex + 1);
      return;
    }

    // History mode: try to step forward in history first
    if (inHistory) {
      showLoading();
      const fromId = current ? current.id : "";
      const r = await fetch(`/api/history_forward?from_id=${fromId}`);
      if (r.ok) {
        const data = await r.json();
        await new Promise((resolve) => {
          const img = new Image();
          img.onload = resolve;
          img.onerror = resolve;
          img.src = data.sample_url;
        });
        renderPost(data);
        return;
      } else if (r.status === 404) {
        inHistory = false;
        // Fall through to normal next handling below
      } else {
        const err = await r.json().catch(() => ({}));
        showError(err.error || "Failed to step forward.");
        return;
      }
    }

    // Normal forward navigation: use buffer if available
    if (buffer) {
      renderPost(buffer);
      buffer = null;
      refreshStats();
      preloadNext();
      return;
    }

    if (bufferLoading) {
      showLoading();
      await bufferLoading;
      if (buffer) {
        renderPost(buffer);
        buffer = null;
        refreshStats();
        preloadNext();
        return;
      }
    }

    showLoading();
    const result = await fetchAndPreload();
    if (result.error) {
      showError(result.error);
      return;
    }
    renderPost(result.data);
    refreshStats();
    preloadNext();
  } finally {
    busy = false;
    els.btnNext.disabled = false;
  }
}

async function goBack() {
  if (busy) return;
  busy = true;
  els.btnBack.disabled = true;

  try {
    // Review mode: step back in the index
    if (isReviewMode()) {
      if (!reviewList) {
        const ok = await loadReviewList();
        if (!ok) return;
        await reviewGoToIndex(0);
        return;
      }
      if (reviewIndex <= 0) {
        showError("Already at the beginning of the review list.");
        return;
      }
      await reviewGoToIndex(reviewIndex - 1);
      return;
    }

    // Normal mode: history-walk back through seen posts
    showLoading();
    const fromId = current ? current.id : "";
    const r = await fetch(`/api/previous?from_id=${fromId}`);
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      showError(err.error || "No earlier post in history.");
      return;
    }
    const data = await r.json();

    await new Promise((resolve) => {
      const img = new Image();
      img.onload = resolve;
      img.onerror = resolve;
      img.src = data.sample_url;
    });

    inHistory = true;
    renderPost(data);

    buffer = null;
    bufferLoading = null;
  } finally {
    busy = false;
    els.btnBack.disabled = false;
  }
}

async function toggleFavorite() {
  if (!current) return;
  const isFavorited = els.btnFav.classList.contains("is-favorited");
  const endpoint = isFavorited ? "/api/unfavorite" : "/api/favorite";
  try {
    const body = { post_id: current.id };
    const r = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await r.json();
    if (data.ok) {
      setFavoriteState(!isFavorited);
      refreshStats();
    } else {
      console.error("Favorite toggle failed:", data.reason);
    }
  } catch (e) {
    console.error(e);
  }
}

// Override input handlers — any change invalidates the buffer since it
// may have been fetched under different settings.
function invalidateBuffer() {
  buffer = null;
}

function updateOverrideUI() {
  const hasOverride = els.overrideInput.value.trim().length > 0;
  els.overrideClear.hidden = !hasOverride;
  els.overrideInput.classList.toggle("active", hasOverride);
  // Persist toggle: only visible when there's an override AND not in review mode
  els.persistToggleWrap.hidden = !hasOverride || isReviewMode();
  invalidateBuffer();
}


// ---------- Tag completion ----------
//
// Completes the token under the cursor, not the whole field: the override bar
// holds a multi-tag query, so `canine wol|` should complete `wol` and leave
// `canine` alone. A leading `-` is stripped before searching and restored on
// accept, so negated tags complete too.

let suggestItems = [];
let suggestActive = -1;
let suggestDebounce = null;
let suggestAbort = null;
// Set while we're writing an accepted suggestion into the field, so the
// resulting `input` event doesn't immediately reopen the dropdown.
let suggestApplying = false;

/**
 * Locate the whitespace-delimited token containing the caret.
 * Returns {start, end, text} — `text` still carries any leading `-`.
 */
function activeToken() {
  const value = els.overrideInput.value;
  const caret = els.overrideInput.selectionStart ?? value.length;
  let start = caret;
  while (start > 0 && !/\s/.test(value[start - 1])) start--;
  let end = caret;
  while (end < value.length && !/\s/.test(value[end])) end++;
  return { start, end, text: value.slice(start, end) };
}

function closeSuggest() {
  suggestItems = [];
  suggestActive = -1;
  els.suggest.hidden = true;
  els.suggest.innerHTML = "";
  els.overrideInput.setAttribute("aria-expanded", "false");
}

/** Escape for innerHTML — tag names are e621 data, not ours to trust. */
function escapeHtml(text) {
  return text.replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[ch]);
}

/** Bold the matched fragment wherever it lands inside the name. */
function highlight(name, fragment) {
  const at = fragment ? name.indexOf(fragment) : -1;
  if (at < 0) return escapeHtml(name);
  return (
    escapeHtml(name.slice(0, at)) +
    "<b>" + escapeHtml(name.slice(at, at + fragment.length)) + "</b>" +
    escapeHtml(name.slice(at + fragment.length))
  );
}

function formatCount(n) {
  if (n >= 1000000) return (n / 1000000).toFixed(1) + "m";
  if (n >= 1000) return Math.round(n / 1000) + "k";
  return String(n);
}

function renderSuggest(suggestions, fragment) {
  suggestItems = suggestions;
  suggestActive = -1;
  if (suggestions.length === 0) {
    closeSuggest();
    return;
  }

  els.suggest.innerHTML = suggestions
    .map((s, i) => {
      // Alias rows read `typed_spelling → canonical_tag`, the way e621 shows
      // them. The fragment matched the alias, not the canonical name, so the
      // highlight goes on the left half; accepting still inserts s.name.
      const label = s.alias_of
        ? `<span class="suggest-name is-alias">${highlight(s.alias_of, fragment)}</span>` +
          `<span class="suggest-arrow">→</span>` +
          `<span class="suggest-name">${escapeHtml(s.name)}</span>`
        : `<span class="suggest-name">${highlight(s.name, fragment)}</span>`;
      return (
        `<div class="suggest-item" data-index="${i}" data-cat="${s.category}" role="option">` +
        label +
        `<span class="suggest-count">${formatCount(s.post_count)}</span>` +
        `</div>`
      );
    })
    .join("");

  els.suggest.hidden = false;
  els.overrideInput.setAttribute("aria-expanded", "true");
}

function setSuggestActive(index) {
  const nodes = els.suggest.querySelectorAll(".suggest-item");
  if (nodes.length === 0) return;
  if (suggestActive >= 0) nodes[suggestActive].classList.remove("active");
  suggestActive = (index + nodes.length) % nodes.length;
  nodes[suggestActive].classList.add("active");
  nodes[suggestActive].scrollIntoView({ block: "nearest" });
}

/** Replace the token under the caret with the chosen tag, preserving `-`. */
function acceptSuggest(index) {
  const choice = suggestItems[index];
  if (!choice) return;
  const token = activeToken();
  const negated = token.text.startsWith("-");
  const value = els.overrideInput.value;
  const replacement = (negated ? "-" : "") + choice.name + " ";

  suggestApplying = true;
  els.overrideInput.value =
    value.slice(0, token.start) + replacement + value.slice(token.end);
  const caret = token.start + replacement.length;
  els.overrideInput.setSelectionRange(caret, caret);
  suggestApplying = false;

  closeSuggest();
  updateOverrideUI();
  els.overrideInput.focus();
}

async function requestSuggest() {
  const token = activeToken();
  let fragment = token.text;
  if (fragment.startsWith("-")) fragment = fragment.slice(1);

  // URLs are pasted whole and completed by nobody.
  if (!fragment || els.overrideInput.value.trim().toLowerCase().startsWith("http")) {
    closeSuggest();
    return;
  }

  // Supersede any request still in flight — out-of-order replies would
  // otherwise repopulate the list with results for an older fragment.
  if (suggestAbort) suggestAbort.abort();
  suggestAbort = new AbortController();

  try {
    const r = await fetch(
      `/api/tags/suggest?q=${encodeURIComponent(fragment)}`,
      { signal: suggestAbort.signal }
    );
    if (!r.ok) return closeSuggest();
    const data = await r.json();
    renderSuggest(data.suggestions || [], fragment.replace(/^.*:/, ""));
  } catch (e) {
    if (e.name !== "AbortError") console.error("suggest failed:", e);
  }
}

function scheduleSuggest() {
  clearTimeout(suggestDebounce);
  suggestDebounce = setTimeout(requestSuggest, 120);
}

els.suggest.addEventListener("mousedown", (e) => {
  // mousedown rather than click: the input's blur handler would close the
  // dropdown before a click ever landed.
  const item = e.target.closest(".suggest-item");
  if (!item) return;
  e.preventDefault();
  acceptSuggest(Number(item.dataset.index));
});

els.overrideInput.addEventListener("keydown", (e) => {
  if (els.suggest.hidden) {
    // Down-arrow on a non-empty token reopens without needing another edit.
    if (e.key === "ArrowDown") scheduleSuggest();
    return;
  }
  if (e.key === "ArrowDown") { e.preventDefault(); setSuggestActive(suggestActive + 1); }
  else if (e.key === "ArrowUp") { e.preventDefault(); setSuggestActive(suggestActive - 1); }
  else if (e.key === "Escape") { e.preventDefault(); closeSuggest(); }
  else if (e.key === "Tab") {
    // Tab always takes the top match, which is what makes fast typing work.
    e.preventDefault();
    acceptSuggest(suggestActive >= 0 ? suggestActive : 0);
  } else if (e.key === "Enter" && suggestActive >= 0) {
    e.preventDefault();
    acceptSuggest(suggestActive);
  }
});

els.overrideInput.addEventListener("blur", () => setTimeout(closeSuggest, 0));
els.overrideInput.addEventListener("click", scheduleSuggest);

// Debounce override changes so we don't rebuild the review list on every keystroke
let overrideDebounce = null;
els.overrideInput.addEventListener("input", () => {
  updateOverrideUI();
  if (!suggestApplying) scheduleSuggest();
  if (isReviewMode()) {
    clearTimeout(overrideDebounce);
    overrideDebounce = setTimeout(async () => {
      await loadReviewList();
      if (reviewList && reviewList.length > 0) {
        await reviewGoToIndex(0);
      }
    }, 600);
  }
});

els.overridePersist.addEventListener("change", invalidateBuffer);
els.overrideClear.addEventListener("click", async () => {
  els.overrideInput.value = "";
  closeSuggest();
  updateOverrideUI();
  if (isReviewMode()) {
    await loadReviewList();
    if (reviewList && reviewList.length > 0) {
      await reviewGoToIndex(0);
    }
  }
});

// Review mode toggle: enter = load list and jump to index 0; exit = clear state
els.reviewMode.addEventListener("change", async () => {
  invalidateBuffer();
  inHistory = false;
  updateOverrideUI(); // re-evaluate persist toggle visibility

  if (isReviewMode()) {
    const ok = await loadReviewList();
    if (ok) {
      await reviewGoToIndex(0);
    }
  } else {
    reviewList = null;
    reviewIndex = 0;
    els.reviewCounter.hidden = true;
  }
});

// Review index input: jump to entered position when committed (Enter or blur)
els.reviewIndexInput.addEventListener("change", async () => {
  if (!isReviewMode() || !reviewList) return;
  const raw = parseInt(els.reviewIndexInput.value, 10);
  if (Number.isNaN(raw)) {
    updateReviewCounterUI();  // restore display
    return;
  }
  // 1-indexed in UI, 0-indexed internally
  const target = raw - 1;
  if (busy) return;
  busy = true;
  try {
    await reviewGoToIndex(target);
  } finally {
    busy = false;
  }
});

els.reviewIndexInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    e.preventDefault();
    els.reviewIndexInput.blur();  // triggers the change handler
  }
});

els.btnNext.addEventListener("click", fetchNext);
els.btnFav.addEventListener("click", toggleFavorite);
els.btnBack.addEventListener("click", goBack);

document.addEventListener("keydown", (e) => {
  if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA") return;
  if (e.code === "Space") { e.preventDefault(); fetchNext(); }
  else if (e.key === "f" || e.key === "F") { toggleFavorite(); }
  else if (e.key === "b" || e.key === "B") { goBack(); }
});

refreshStats();

// ---------- Force rescan button (confirm pattern) ----------
(function initRescanButton() {
  const btn = document.getElementById('btn-rescan');
  let confirmPending = false;
  let revertTimer = null;

  function resetBtn() {
    confirmPending = false;
    clearTimeout(revertTimer);
    btn.textContent = '↺';
    btn.classList.remove('btn-confirm');
  }

  btn.addEventListener('click', async () => {
    if (!confirmPending) {
      // First click: enter confirm state
      confirmPending = true;
      btn.textContent = 'confirm?';
      btn.classList.add('btn-confirm');
      // Auto-revert after 3 seconds if no second click
      revertTimer = setTimeout(resetBtn, 3000);
      return;
    }
    // Second click: fire
    resetBtn();
    btn.disabled = true;
    btn.textContent = '…';
    try {
      await fetch('/api/force_rescan', { method: 'POST' });
    } catch (e) {
      console.error('Force rescan error:', e);
    } finally {
      btn.disabled = false;
      btn.textContent = '↺';
    }
  });

  // Clicking anywhere else while confirm is pending reverts the button
  document.addEventListener('click', (e) => {
    if (confirmPending && e.target !== btn) resetBtn();
  }, true);
})();

// ---------- Connection monitor ----------
(function startConnectionMonitor() {
  const INTERVAL = 4000;
  const TIMEOUT = 3000;
  const dot = document.getElementById('conn-dot');
  const label = document.getElementById('conn-label');

  async function checkConnection() {
    try {
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), TIMEOUT);
      const r = await fetch('/api/ping', { cache: 'no-store', signal: ctrl.signal });
      clearTimeout(tid);
      if (r.ok) {
        const data = await r.json();
        const status = data.status ?? 'live';
        document.body.classList.remove('disconnected', 'status-scanning', 'status-initializing');
        if (status === 'scanning') {
          document.body.classList.add('status-scanning');
          label.textContent = 'scanning';
        } else if (status === 'initializing') {
          document.body.classList.add('status-initializing');
          label.textContent = 'initializing';
        } else {
          label.textContent = 'live';
        }
        return;
      }
    } catch (_) { }
    document.body.classList.remove('status-scanning', 'status-initializing');
    document.body.classList.add('disconnected');
    label.textContent = 'dead';
  }

  setInterval(checkConnection, INTERVAL);
  checkConnection();
})();
