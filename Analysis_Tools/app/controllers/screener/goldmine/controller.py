"""
Goldmine Screener Controller
Organizes screeners into categorized tabs
"""

from flask import Blueprint, render_template

# Create blueprint
goldmine_bp = Blueprint(
    'goldmine',
    __name__,
    url_prefix='/scanner/goldmine'
)


@goldmine_bp.route('/')
def goldmine_page():
    """
    Render the Goldmine screener page with categorized tabs.

    Categories:
    - Derivative Screeners
    - Technical Screeners
    - Fundamental Screeners
    - Intraday Screeners
    - Price & Volume Screeners
    """
    return render_template('screener/goldmine.html')
