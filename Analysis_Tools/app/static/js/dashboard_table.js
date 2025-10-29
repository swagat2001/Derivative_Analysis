$(document).ready(function () {
  $('#derivativesTable').DataTable({
    paging: true,
    searching: true,
    ordering: true,
    info: true,
    pageLength: 20,
    lengthMenu: [10,15, 20, 25, 100],
    scrollX: true,
    order: [[0, 'asc']],
    language: {
      search: "🔍 Search:",
      lengthMenu: "Show _MENU_ rows",
      info: "Showing _START_ to _END_ of _TOTAL_ entries"
    }
  });
});
