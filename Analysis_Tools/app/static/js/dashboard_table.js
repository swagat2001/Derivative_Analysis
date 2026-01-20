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
    scrollY: false,  // Disable vertical scroll to prevent scrollbar issues
    scrollCollapse: false,
    fixedHeader: {
      header: true,
      headerOffset: 60
    },
    order: [[0, 'asc']],
    language: {
      lengthMenu: "Show _MENU_ rows",
      info: "Showing _START_ to _END_ of _TOTAL_ entries"
    },
    columnDefs: [
      { targets: 0, className: 'cell-stock', orderable: true },
      { targets: [1,2,3,4,5,7,9,11,12,13,14,15,16,17,19,21], className: 'dt-body-center', orderable: true },
      { targets: [6, 8, 10, 18, 20, 22], className: 'dt-body-center', orderable: true },
      { targets: [11, 12], className: 'highlight-col', orderable: true }
    ],
    drawCallback: function() {
      attachClickHandlers();
    },
    initComplete: function() {
      attachClickHandlers();
    }
  });

  // AJAX filter functionality - UPDATE DATA WITHOUT DESTROYING TABLE
  window.filterDashboard = function(mtype) {
    var selectedDate = $('#dateSelect').val();

    // Show loading indicator
    $('#derivativesTable').addClass('loading');

    // Fetch new data via AJAX
    $.ajax({
      url: '/dashboard/',
      type: 'GET',
      data: {
        date: selectedDate,
        mtype: mtype,
        ajax: 'true'
      },
      success: function(response) {
        // Clear existing rows using DataTables API
        table.clear();

        // Add new rows using DataTables API
        if (response.data && response.data.length > 0) {
          var newRows = [];

          response.data.forEach(function(row) {
            newRows.push([
              '<a href="/stock/' + row.stock + '">' + row.stock + '</a>',
              row.call_delta_pos_strike || '',
              row.call_delta_pos_pct || '',
              row.call_delta_neg_strike || '',
              row.call_delta_neg_pct || '',
              row.call_vega_pos_strike || '',
              row.call_vega_pos_pct || '',
              row.call_vega_neg_strike || '',
              row.call_vega_neg_pct || '',
              (row.call_total_tradval || 0).toFixed(2),
              (row.call_total_money || 0).toFixed(2),
              (row.closing_price || 0).toFixed(2),
              (row.rsi || 0).toFixed(2),
              row.put_delta_pos_strike || '',
              row.put_delta_pos_pct || '',
              row.put_delta_neg_strike || '',
              row.put_delta_neg_pct || '',
              row.put_vega_pos_strike || '',
              row.put_vega_pos_pct || '',
              row.put_vega_neg_strike || '',
              row.put_vega_neg_pct || '',
              (row.put_total_tradval || 0).toFixed(2),
              (row.put_total_money || 0).toFixed(2)
            ]);
          });

          // Add all rows at once
          table.rows.add(newRows);
        }

        // Redraw table
        table.draw(false); // false = stay on current page

        // Now add data attributes to clickable cells AFTER draw
        response.data.forEach(function(row, index) {
          var $row = $('#derivativesTable tbody tr').eq(index);

          // Call Vega+ (column 6)
          $row.find('td:eq(6)').addClass('money-clickable')
            .attr('data-stock', row.stock)
            .attr('data-option-type', 'call')
            .attr('data-metric', 'vega')
            .attr('data-strike', row.call_vega_pos_strike || '')
            .attr('data-date', selectedDate);

          // Call Vega- (column 8)
          $row.find('td:eq(8)').addClass('money-clickable')
            .attr('data-stock', row.stock)
            .attr('data-option-type', 'call')
            .attr('data-metric', 'vega')
            .attr('data-strike', row.call_vega_neg_strike || '')
            .attr('data-date', selectedDate);

          // Call AMoney (column 10)
          $row.find('td:eq(10)').addClass('money-clickable')
            .attr('data-stock', row.stock)
            .attr('data-option-type', 'call')
            .attr('data-metric', 'money')
            .attr('data-strike', 'N/A')
            .attr('data-date', selectedDate);

          // Put Vega+ (column 18)
          $row.find('td:eq(18)').addClass('money-clickable')
            .attr('data-stock', row.stock)
            .attr('data-option-type', 'put')
            .attr('data-metric', 'vega')
            .attr('data-strike', row.put_vega_pos_strike || '')
            .attr('data-date', selectedDate);

          // Put Vega- (column 20)
          $row.find('td:eq(20)').addClass('money-clickable')
            .attr('data-stock', row.stock)
            .attr('data-option-type', 'put')
            .attr('data-metric', 'vega')
            .attr('data-strike', row.put_vega_neg_strike || '')
            .attr('data-date', selectedDate);

          // Put AMoney (column 22)
          $row.find('td:eq(22)').addClass('money-clickable')
            .attr('data-stock', row.stock)
            .attr('data-option-type', 'put')
            .attr('data-metric', 'money')
            .attr('data-strike', 'N/A')
            .attr('data-date', selectedDate);
        });

        // Reattach click handlers
        attachClickHandlers();

        // Update active button state
        $('.filter-btn').removeClass('active');
        $('.filter-btn[data-mtype="' + mtype + '"]').addClass('active');

        // Remove loading indicator
        $('#derivativesTable').removeClass('loading');
      },
      error: function(xhr, status, error) {
        console.error('Error loading dashboard data:', error);
        alert('Error loading data. Please try again.');
        $('#derivativesTable').removeClass('loading');
      }
    });
  };
});

// Function to attach click handlers to clickable cells
function attachClickHandlers() {
  // Remove old handlers first
  $('tbody td.money-clickable').off('click');

  // Attach to tbody td cells with money-clickable class
  $('tbody td.money-clickable').on('click', function() {
    // Validate that we have the required data
    var stock = $(this).attr('data-stock');
    var optionType = $(this).attr('data-option-type');
    var metric = $(this).attr('data-metric');
    var date = $(this).attr('data-date');

    // Only open modal if we have valid data
    if (stock && optionType && metric && date) {
      openChartModal(this);
    } else {
      console.error('Missing data attributes:', {
        stock: stock,
        optionType: optionType,
        metric: metric,
        date: date
      });
    }
  });
}
