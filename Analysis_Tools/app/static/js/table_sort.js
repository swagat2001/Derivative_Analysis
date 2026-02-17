/**
 * Generic Table Sorting Script
 *
 * Usage:
 * 1. Add class "sortable-table" to your <table> element.
 * 2. Ensure your <thead> contains <th> elements.
 * 3. The script will automatically make all columns sortable.
 * 4. To exclude a column, add class "no-sort" to the <th>.
 * 5. To force a specific data type, add data-type="number|string|date" to the <th>.
 */

document.addEventListener('DOMContentLoaded', function () {
    initTableSorting();
});

// Expose checks for dynamic content
window.initTableSorting = function () {
    const tables = document.querySelectorAll('table.sortable-table');
    tables.forEach(table => {
        if (table.getAttribute('data-sort-initialized')) return;

        const headers = table.querySelectorAll('thead th');
        headers.forEach((th, index) => {
            if (th.classList.contains('no-sort')) return;

            // Add sortable class for styling
            th.classList.add('sortable');

            // Add click handler
            th.addEventListener('click', () => {
                sortTable(table, index);
            });
        });

        table.setAttribute('data-sort-initialized', 'true');
    });
};

function sortTable(table, colIndex) {
    const tbody = table.querySelector('tbody');
    if (!tbody) return;

    // Get all rows
    const rows = Array.from(tbody.querySelectorAll('tr'));
    if (rows.length === 0) return;

    // Determine sort direction
    const th = table.querySelectorAll('thead th')[colIndex];
    const isAsc = !th.classList.contains('asc');

    // Reset other headers
    table.querySelectorAll('thead th').forEach(h => {
        h.classList.remove('asc', 'desc');
    });

    // Set current header
    th.classList.add(isAsc ? 'asc' : 'desc');

    // Determine data type
    let type = th.getAttribute('data-type') || 'string';

    // Auto-detect type from first non-empty cell if not specified
    if (!th.getAttribute('data-type')) {
        const sampleCell = rows.find(r => {
            const cell = r.children[colIndex];
            return cell && cell.textContent.trim() !== '' && cell.textContent.trim() !== '-';
        });

        if (sampleCell) {
            const text = sampleCell.children[colIndex].textContent.trim();
            if (isNumeric(text)) type = 'number';
            else if (isDate(text)) type = 'date';
        }
    }

    // Sort rows
    rows.sort((rowA, rowB) => {
        const cellA = rowA.children[colIndex]?.textContent.trim() || '';
        const cellB = rowB.children[colIndex]?.textContent.trim() || '';

        if (cellA === cellB) return 0;

        let valA = cellA;
        let valB = cellB;

        if (type === 'number') {
            valA = parseNumber(cellA);
            valB = parseNumber(cellB);
            return isAsc ? valA - valB : valB - valA;
        } else if (type === 'date') {
            valA = new Date(cellA);
            valB = new Date(cellB);
            return isAsc ? valA - valB : valB - valA;
        } else {
            return isAsc ? valA.localeCompare(valB) : valB.localeCompare(valA);
        }
    });

    // Reorder rows in DOM
    rows.forEach(row => tbody.appendChild(row));
}

// Helpers
function isNumeric(str) {
    if (!str) return false;
    // Remove currency symbols, commas, percentages
    const clean = str.replace(/[₹,%\s]/g, '');
    return !isNaN(parseFloat(clean)) && isFinite(clean);
}

function parseNumber(str) {
    if (!str || str === '-') return -Infinity;
    const clean = str.replace(/[₹,%\s]/g, '');
    const num = parseFloat(clean);
    return isNaN(num) ? -Infinity : num;
}

function isDate(str) {
    const date = new Date(str);
    return !isNaN(date.getTime());
}
