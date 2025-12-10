$(document).ready(function () {
  var table = $('#derivativesTable').DataTable({
    paging: true,
    pageLength: 20,
    lengthMenu: [[10, 20, 50, 100, -1], [10, 20, 50, 100, "All"]],
    searching: false,
    ordering: true,
    info: true,
    autoWidth: false,
    scrollX: false,
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
      { targets: [1,2,3,4,5,7,9,10,14,15,16,17,19,21,22], className: 'dt-body-center', orderable: true },
      // Don't add money-clickable class via DataTables - we'll add it manually to td only
      { targets: [6, 8, 10, 18, 20, 22], className: 'dt-body-center', orderable: true },
      { targets: [11, 12], className: 'highlight-col', orderable: true }
    ],
    drawCallback: function() {
      // Add money-clickable class ONLY to tbody td cells
      $('#derivativesTable tbody tr').each(function() {
        $(this).find('td:eq(6), td:eq(8), td:eq(10), td:eq(18), td:eq(20), td:eq(22)').addClass('money-clickable');
      });

      attachClickHandlers();

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
      // Add money-clickable class ONLY to tbody td cells
      $('#derivativesTable tbody tr').each(function() {
        $(this).find('td:eq(6), td:eq(8), td:eq(10), td:eq(18), td:eq(20), td:eq(22)').addClass('money-clickable');
      });

      attachClickHandlers();

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

  $(window).on('resize', function() {
    $('.dataTables_scrollBody').css('overflow-x', 'hidden');
    $('.dataTables_scroll').css('overflow-x', 'hidden');
    $('.dataTables_wrapper').css('overflow-x', 'hidden');
  });
});

// Function to attach click handlers ONLY to tbody td cells, NOT th headers
function attachClickHandlers() {
  // Remove old handlers first
  $('tbody td.money-clickable').off('click');

  // Attach ONLY to tbody td cells
  $('tbody td.money-clickable').on('click', function() {
    openChartModal(this);
  });
}
