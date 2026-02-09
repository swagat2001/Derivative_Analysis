"""
Integration tests for API endpoints.
Tests Flask routes and data flow.
"""

import pytest
import json
from datetime import datetime


@pytest.mark.integration
class TestAPIEndpoints:
    """Test suite for API endpoints."""

    def test_home_page_loads(self, flask_test_client):
        """Test that home page loads successfully."""
        response = flask_test_client.get('/')

        assert response.status_code == 200, "Home page should load"

    def test_login_page_loads(self, flask_test_client):
        """Test that login page loads."""
        response = flask_test_client.get('/login')

        assert response.status_code == 200, "Login page should load"

    def test_dashboard_requires_auth(self, flask_test_client):
        """Test that dashboard redirects to login when not authenticated."""
        response = flask_test_client.get('/dashboard')

        # Should redirect to login
        assert response.status_code == 302, "Should redirect when not authenticated"
        assert '/login' in response.location, "Should redirect to login page"

    def test_login_with_valid_credentials(self, flask_test_client):
        """Test login with valid credentials."""
        response = flask_test_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123'  # pragma: allowlist secret
        }, follow_redirects=False)

        # Should redirect on successful login
        assert response.status_code == 302, "Should redirect after login"

    def test_login_with_invalid_credentials(self, flask_test_client):
        """Test login with invalid credentials."""
        response = flask_test_client.post('/login', data={
            'username': 'invalid',
            'password': 'wrong'  # pragma: allowlist secret
        }, follow_redirects=True)

        # Should show error message
        assert response.status_code == 200, "Should return to login page"
        assert b'Invalid' in response.data or b'error' in response.data.lower()

    def test_live_indices_api(self, flask_test_client):
        """Test live indices API endpoint."""
        # Login first
        flask_test_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123'  # pragma: allowlist secret
        })

        response = flask_test_client.get('/api/live-indices')

        assert response.status_code == 200, "API should return 200"

        # Check if response is JSON
        try:
            data = json.loads(response.data)
            assert isinstance(data, (dict, list)), "Should return JSON data"
        except json.JSONDecodeError:
            pytest.fail("Response should be valid JSON")

    def test_historical_chart_data_api(self, flask_test_client):
        """Test historical chart data API."""
        # Login first
        flask_test_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123'  # pragma: allowlist secret
        })

        response = flask_test_client.get('/api/historical-chart-data/RELIANCE')

        assert response.status_code == 200, "API should return 200"

        try:
            data = json.loads(response.data)
            assert isinstance(data, dict), "Should return JSON object"

            # Check for expected keys
            if data:
                assert 'dates' in data or 'error' in data, "Should have dates or error"
        except json.JSONDecodeError:
            pytest.fail("Response should be valid JSON")

    def test_heatmap_api(self, flask_test_client):
        """Test heatmap API endpoint."""
        # Login first
        flask_test_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123'  # pragma: allowlist secret
        })

        response = flask_test_client.get('/api/heatmap')

        assert response.status_code == 200, "API should return 200"

        try:
            data = json.loads(response.data)
            assert isinstance(data, (dict, list)), "Should return JSON data"
        except json.JSONDecodeError:
            pytest.fail("Response should be valid JSON")

    def test_fii_dii_api(self, flask_test_client):
        """Test FII/DII activity API."""
        # Login first
        flask_test_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123'  # pragma: allowlist secret
        })

        response = flask_test_client.get('/api/fii-dii')

        assert response.status_code == 200, "API should return 200"

    def test_dashboard_data_api(self, flask_test_client):
        """Test dashboard data API (server-side DataTables)."""
        # Login first
        flask_test_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123'  # pragma: allowlist secret
        })

        response = flask_test_client.get('/api/dashboard_data?symbol=NIFTY')

        assert response.status_code == 200, "API should return 200"

        try:
            data = json.loads(response.data)
            assert isinstance(data, dict), "Should return JSON object"

            # DataTables format
            if 'data' in data:
                assert isinstance(data['data'], list), "Should have data array"
        except json.JSONDecodeError:
            pytest.fail("Response should be valid JSON")

    def test_screener_landing_page(self, flask_test_client):
        """Test screener landing page loads."""
        # Login first
        flask_test_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123'  # pragma: allowlist secret
        })

        response = flask_test_client.get('/screener/')

        assert response.status_code == 200, "Screener page should load"

    def test_insights_page(self, flask_test_client):
        """Test insights page loads."""
        # Login first
        flask_test_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123'  # pragma: allowlist secret
        })

        response = flask_test_client.get('/insights')

        assert response.status_code == 200, "Insights page should load"

    def test_invalid_symbol_handling(self, flask_test_client):
        """Test API handling of invalid symbol."""
        # Login first
        flask_test_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123'  # pragma: allowlist secret
        })

        response = flask_test_client.get('/api/historical-chart-data/INVALID_SYMBOL_123')

        # Should handle gracefully (200 with error or 404)
        assert response.status_code in [200, 404], "Should handle invalid symbol"

    def test_logout_functionality(self, flask_test_client):
        """Test logout functionality."""
        # Login first
        flask_test_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123'  # pragma: allowlist secret
        })

        # Logout
        response = flask_test_client.get('/logout', follow_redirects=False)

        assert response.status_code == 302, "Should redirect after logout"
        assert '/login' in response.location, "Should redirect to login"

    def test_static_files_accessible(self, flask_test_client):
        """Test that static files are accessible."""
        response = flask_test_client.get('/static/css/global.css')

        # Should be accessible without auth
        assert response.status_code in [200, 304], "Static files should be accessible"

    def test_api_error_handling(self, flask_test_client):
        """Test API error handling for malformed requests."""
        # Login first
        flask_test_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123'  # pragma: allowlist secret
        })

        # Try to access API with invalid parameters
        response = flask_test_client.get('/api/dashboard_data?symbol=')

        # Should handle gracefully
        assert response.status_code in [200, 400], "Should handle empty symbol"
