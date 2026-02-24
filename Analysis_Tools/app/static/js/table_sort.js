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
        const headers = table.querySelectorAll('thead th');

        headers.forEach((th, index) => {
            if (th.classList.contains('no-sort')) return;

            // Avoid adding multiple event listeners if already initialized
            if (th.classList.contains('sortable')) return;

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
        let typeFound = false;

        for (let i = 0; i < rows.length; i++) {
            const cell = rows[i].children[colIndex];
            if (cell && cell.textContent.trim() !== '' && cell.textContent.trim() !== '-') {
                const text = cell.textContent.trim();
                if (isNumeric(text)) {
                    type = 'number';
                    typeFound = true;
                    break;
                } else if (isDate(text)) {
                    type = 'date';
                    typeFound = true;
                    break;
                }
            }
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

    // If it clearly matches a date format (YYYY-MM-DD, DD-MMM-YYYY, DD/MM/YYYY), let the date parser handle it completely.
    if (/^\d{2,4}[-/\s][a-zA-Z0-9]{2,3}[-/\s]\d{2,4}(?:\s\d{2}:\d{2}:\d{2})?$/.test(str.trim())) {
        return false;
    }

    const cleanStr = str.replace(/,/g, '');
    // Match standard numbers, or accounting format like (150)
    const match = cleanStr.match(/-?\d+(?:\.\d+)?/);
    if (!match) return false;

    return true;
}

function parseNumber(str) {
    if (!str || str.trim() === '-' || str.trim() === 'N/A') return -Infinity;

    // Support accounting negative format: (15.5) -> -15.5
    let isNegative = str.includes('(') && str.includes(')');

    // Remove typical noise chars
    const cleanStr = str.replace(/[,₹%\s]/g, '');

    const match = cleanStr.match(/-?\d+(?:\.\d+)?/);
    if (match) {
        let val = parseFloat(match[0]);
        if (isNegative && val > 0) val = -val;
        return val;
    }
    return -Infinity;
}

function isDate(str) {
    const date = new Date(str);
    return !isNaN(date.getTime());
}
