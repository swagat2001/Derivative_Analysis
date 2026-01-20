// Simple dashboard table - NO DataTables, clean and lightweight

$(document).ready(function() {
  // Attach click handlers on load
  attachClickHandlers();
});

// AJAX filter functionality - Simple tbody replacement
window.filterDashboard = function(mtype) {
  var selectedDate = $('#dateSelect').val();

  // Show loading state
  $('#tableBody').html('<tr><td colspan="23" style="text-align: center; padding: 40px; color: #6b7280;">Loading...</td></tr>');

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
      var tbody = $('#tableBody');
      tbody.empty();

      if (response.data && response.data.length > 0) {
        // Build rows using same structure as template
        response.data.forEach(function(row) {
          var rowHtml =
            '<tr>' +
              '<td class="cell-stock"><a href="/stock/' + row.stock + '" class="symbol-link">' + row.stock + '</a></td>' +
              '<td>' + (row.call_delta_pos_strike || '') + '</td>' +
              '<td>' + (row.call_delta_pos_pct || '') + '</td>' +
              '<td>' + (row.call_delta_neg_strike || '') + '</td>' +
              '<td>' + (row.call_delta_neg_pct || '') + '</td>' +
              '<td>' + (row.call_vega_pos_strike || '') + '</td>' +
              '<td class="money-clickable" data-stock="' + row.stock + '" data-option-type="call" data-metric="vega" data-strike="' + (row.call_vega_pos_strike || '') + '" data-date="' + selectedDate + '">' + (row.call_vega_pos_pct || '') + '</td>' +
              '<td>' + (row.call_vega_neg_strike || '') + '</td>' +
              '<td class="money-clickable" data-stock="' + row.stock + '" data-option-type="call" data-metric="vega" data-strike="' + (row.call_vega_neg_strike || '') + '" data-date="' + selectedDate + '">' + (row.call_vega_neg_pct || '') + '</td>' +
              '<td>' + (row.call_total_tradval || 0).toFixed(2) + '</td>' +
              '<td class="money-clickable" data-stock="' + row.stock + '" data-option-type="call" data-metric="money" data-strike="N/A" data-date="' + selectedDate + '">' + (row.call_total_money || 0).toFixed(2) + '</td>' +
              '<td class="highlight-col">' + (row.closing_price || 0).toFixed(2) + '</td>' +
              '<td class="highlight-col">' + (row.rsi || 0).toFixed(2) + '</td>' +
              '<td>' + (row.put_delta_pos_strike || '') + '</td>' +
              '<td>' + (row.put_delta_pos_pct || '') + '</td>' +
              '<td>' + (row.put_delta_neg_strike || '') + '</td>' +
              '<td>' + (row.put_delta_neg_pct || '') + '</td>' +
              '<td>' + (row.put_vega_pos_strike || '') + '</td>' +
              '<td class="money-clickable" data-stock="' + row.stock + '" data-option-type="put" data-metric="vega" data-strike="' + (row.put_vega_pos_strike || '') + '" data-date="' + selectedDate + '">' + (row.put_vega_pos_pct || '') + '</td>' +
              '<td>' + (row.put_vega_neg_strike || '') + '</td>' +
              '<td class="money-clickable" data-stock="' + row.stock + '" data-option-type="put" data-metric="vega" data-strike="' + (row.put_vega_neg_strike || '') + '" data-date="' + selectedDate + '">' + (row.put_vega_neg_pct || '') + '</td>' +
              '<td>' + (row.put_total_tradval || 0).toFixed(2) + '</td>' +
              '<td class="money-clickable" data-stock="' + row.stock + '" data-option-type="put" data-metric="money" data-strike="N/A" data-date="' + selectedDate + '">' + (row.put_total_money || 0).toFixed(2) + '</td>' +
            '</tr>';

          tbody.append(rowHtml);
        });
      } else {
        tbody.html('<tr><td colspan="23" style="text-align: center; padding: 40px; color: #6b7280;">No data available</td></tr>');
      }

      // Reattach click handlers
      attachClickHandlers();

      // Update active button state
      $('.filter-btn').removeClass('active');
      $('.filter-btn[data-mtype="' + mtype + '"]').addClass('active');
    },
    error: function(xhr, status, error) {
      console.error('Error loading dashboard data:', error);
      $('#tableBody').html('<tr><td colspan="23" style="text-align: center; padding: 40px; color: #dc2626;">Error loading data. Please try again.</td></tr>');
    }
  });
};

// Attach click handlers to clickable cells
function attachClickHandlers() {
  // Remove old handlers
  $('.money-clickable').off('click');

  // Attach new handlers
  $('.money-clickable').on('click', function() {
    var stock = $(this).attr('data-stock');
    var optionType = $(this).attr('data-option-type');
    var metric = $(this).attr('data-metric');
    var date = $(this).attr('data-date');

    // Validate data
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
