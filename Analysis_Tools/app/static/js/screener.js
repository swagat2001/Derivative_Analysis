/**
 * SCREENER PAGE - JAVASCRIPT
 * Handles tooltip initialization for symbol hover effects
 */

document.addEventListener('DOMContentLoaded', function() {
  // Initialize Bootstrap tooltips for all symbol links
  const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-toggle="tooltip"]'));
  tooltipTriggerList.map(function (tooltipTriggerEl) {
    return new bootstrap.Tooltip(tooltipTriggerEl, {
      html: true,
      placement: 'top',
      trigger: 'hover',
      customClass: 'symbol-tooltip'
    });
  });

  console.log('[Screener] Tooltips initialized for', tooltipTriggerList.length, 'symbols');
});


(function () {
  const OFFSET = 80; // header height

  function scrollToTarget(target) {
    const targetPosition =
      target.getBoundingClientRect().top + window.pageYOffset - OFFSET;

    window.scrollTo({
      top: targetPosition,
      behavior: "smooth"
    });
  }

  // Handle anchor clicks
  document.addEventListener("click", function (e) {
    const link = e.target.closest('a[href^="#"]');
    if (!link) return;

    const id = link.getAttribute("href");
    if (!id || id === "#") return;

    const target = document.querySelector(id);
    if (!target) return;

    e.preventDefault();
    history.pushState(null, "", id);
    scrollToTarget(target);
  });

  // Handle page load with hash
  window.addEventListener("load", function () {
    if (window.location.hash) {
      const target = document.querySelector(window.location.hash);
      if (target) {
        setTimeout(() => scrollToTarget(target), 0);
      }
    }
  });
})();
