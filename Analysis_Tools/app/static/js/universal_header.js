// ======================================================================
// UNIVERSAL HEADER INTERACTIVITY (User Menu + Search)
// ======================================================================

document.addEventListener("DOMContentLoaded", function () {
  // -------------------------------
  // USER MENU TOGGLE
  // -------------------------------
  const userBtn = document.querySelector(".user-btn");
  const userDropdown = document.getElementById("userDropdown");

  if (userBtn && userDropdown) {
    userBtn.addEventListener("click", function (event) {
      event.stopPropagation(); // Prevent click bubbling
      const isVisible = userDropdown.style.display === "block";
      userDropdown.style.display = isVisible ? "none" : "block";
    });

    // Close dropdown when clicking outside
    document.addEventListener("click", function (event) {
      if (!userDropdown.contains(event.target) && !userBtn.contains(event.target)) {
        userDropdown.style.display = "none";
      }
    });
  }

  // -------------------------------
  // SEARCH TOGGLE (if used)
  // -------------------------------
  const searchBtn = document.querySelector(".search-btn");
  const searchDropdown = document.getElementById("searchDropdown");

  if (searchBtn && searchDropdown) {
    searchBtn.addEventListener("click", function (event) {
      event.stopPropagation();
      const isVisible = searchDropdown.style.display === "block";
      searchDropdown.style.display = isVisible ? "none" : "block";
    });

    document.addEventListener("click", function (event) {
      if (!searchDropdown.contains(event.target) && !searchBtn.contains(event.target)) {
        searchDropdown.style.display = "none";
      }
    });
  }
});
