// ======================================================================
// UNIVERSAL HEADER INTERACTIVITY
// ScanX Style - Simple, clean
// ======================================================================

document.addEventListener("DOMContentLoaded", function () {

  // -------------------------------
  // SEARCH INPUT - Navigate on Enter
  // -------------------------------
  const searchInput = document.getElementById("stockSearchInput");

  if (searchInput) {
    searchInput.addEventListener("keypress", function (event) {
      if (event.key === "Enter") {
        const ticker = this.value.trim().toUpperCase();
        if (ticker) {
          window.location.href = "/stock/" + ticker;
        }
      }
    });

    // Auto-select on focus
    searchInput.addEventListener("focus", function () {
      this.select();
    });
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
