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
