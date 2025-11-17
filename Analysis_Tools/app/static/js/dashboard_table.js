$(document).ready(function () {
  var table = $('#derivativesTable').DataTable({
    paging: true,
    pageLength: 20,
    lengthMenu: [[10, 20, 50, 100, -1], [10, 20, 50, 100, "All"]],
    searching: false,  // DISABLED - Using universal header search instead
    ordering: true,
    info: true,
    autoWidth: false,
    scrollX: false,  // NO horizontal scroll
    scrollY: false,
    scrollCollapse: false,
    fixedHeader: false,
    order: [[0, 'asc']],
    language: {
      lengthMenu: "Show _MENU_ rows",
      info: "Showing _START_ to _END_ of _TOTAL_ entries"
    },
    columnDefs: [
      { targets: 0, className: 'cell-stock', orderable: true },
      { targets: [1,2,3,4,5,6,7,8,9,10,14,15,16,17,18,19,20,21,22], className: 'dt-body-center', orderable: true },
      { targets: [11, 12], className: 'highlight-col', orderable: true }
    ],
    drawCallback: function() {
      // CRITICAL: Ensure no horizontal scrollbar after every redraw
      $('.dataTables_scrollBody').css({
        'overflow-x': 'hidden',
        'max-width': '100%'
      });
      $('.dataTables_scroll').css({
        'overflow-x': 'hidden',
        'max-width': '100%'
      });
      $('.dataTables_wrapper').css({
        'overflow-x': 'hidden',
        'max-width': '100%'
      });
      $('table.dataTable').css({
        'width': '100%',
        'max-width': '100%'
      });
    },
    initComplete: function() {
      // CRITICAL: Force no scroll on initialization
      $('.dataTables_scrollBody').css({
        'overflow-x': 'hidden',
        'max-width': '100%'
      });
      $('.dataTables_scroll').css({
        'overflow-x': 'hidden',
        'max-width': '100%'
      });
      $('.dataTables_wrapper').css({
        'overflow-x': 'hidden',
        'max-width': '100%'
      });
      $('table.dataTable').css({
        'width': '100%',
        'max-width': '100%',
        'table-layout': 'fixed'
      });
    }
  });
  
  // Additional safeguard - remove scroll on window resize
  $(window).on('resize', function() {
    $('.dataTables_scrollBody').css('overflow-x', 'hidden');
    $('.dataTables_scroll').css('overflow-x', 'hidden');
    $('.dataTables_wrapper').css('overflow-x', 'hidden');
  });
});
