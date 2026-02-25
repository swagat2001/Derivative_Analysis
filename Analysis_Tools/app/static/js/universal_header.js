// ======================================================================
// UNIVERSAL HEADER INTERACTIVITY
// ScanX Style - Live AJAX search across all NSE stocks
// ======================================================================

document.addEventListener("DOMContentLoaded", function () {

  // -------------------------------
  // LIVE STOCK SEARCH (all stocks, no datalist)
  // -------------------------------
  const searchInput = document.getElementById("stockSearchInput");
  const suggestions = document.getElementById("stockSuggestions");

  let searchDebounce = null;
  let selectedIndex = -1;

  if (searchInput && suggestions) {

    searchInput.addEventListener("input", function () {
      const query = this.value.trim();
      clearTimeout(searchDebounce);
      selectedIndex = -1;

      if (query.length < 1) {
        hideSuggestions();
        return;
      }

      searchDebounce = setTimeout(() => {
        fetch(`/api/voice/search/stocks?q=${encodeURIComponent(query)}`)
          .then(r => r.json())
          .then(data => {
            renderSuggestions(data.symbols || []);
          })
          .catch(() => hideSuggestions());
      }, 200);
    });

    // Keyboard navigation
    searchInput.addEventListener("keydown", function (e) {
      const items = suggestions.querySelectorAll("li");
      if (e.key === "ArrowDown") {
        e.preventDefault();
        selectedIndex = Math.min(selectedIndex + 1, items.length - 1);
        highlightItem(items);
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        selectedIndex = Math.max(selectedIndex - 1, 0);
        highlightItem(items);
      } else if (e.key === "Enter") {
        e.preventDefault();
        if (selectedIndex >= 0 && items[selectedIndex]) {
          navigateTo(items[selectedIndex].dataset.ticker);
        } else {
          const ticker = this.value.trim().toUpperCase();
          if (ticker) navigateTo(ticker);
        }
      } else if (e.key === "Escape") {
        hideSuggestions();
      }
    });

    // Hide on outside click
    document.addEventListener("click", function (e) {
      if (!searchInput.contains(e.target) && !suggestions.contains(e.target)) {
        hideSuggestions();
      }
    });

    // Auto-select on focus
    searchInput.addEventListener("focus", function () {
      this.select();
    });
  }

  function renderSuggestions(symbols) {
    if (!symbols.length) { hideSuggestions(); return; }
    suggestions.innerHTML = "";
    symbols.forEach((sym, i) => {
      const li = document.createElement("li");
      li.dataset.ticker = sym;
      li.textContent = sym;
      li.style.cssText = "padding:8px 14px; cursor:pointer; font-size:13px; font-weight:600; color:#1f2937; transition:background 0.1s;";
      li.addEventListener("mouseenter", () => {
        selectedIndex = i;
        highlightItem(suggestions.querySelectorAll("li"));
      });
      li.addEventListener("click", () => navigateTo(sym));
      suggestions.appendChild(li);
    });
    suggestions.style.display = "block";
  }

  function highlightItem(items) {
    items.forEach((item, i) => {
      item.style.background = i === selectedIndex ? "#f0f4ff" : "";
      item.style.color = i === selectedIndex ? "#2563eb" : "#1f2937";
    });
    if (items[selectedIndex]) {
      searchInput.value = items[selectedIndex].dataset.ticker;
    }
  }

  function hideSuggestions() {
    if (suggestions) suggestions.style.display = "none";
    selectedIndex = -1;
  }

  function navigateTo(ticker) {
    hideSuggestions();
    window.location.href = "/stock/" + ticker.toUpperCase();
  }

  // -------------------------------
  // ACTIVE NAV LINK HIGHLIGHTING
  // -------------------------------
  const currentPath = window.location.pathname;
  const navLinks = document.querySelectorAll(".nav-link");

  navLinks.forEach(link => {
    const href = link.getAttribute("href");
    if (href && currentPath.startsWith(href) && href !== "/") {
      link.classList.add("active");
    }
  });

});
