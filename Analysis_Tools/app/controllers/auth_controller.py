from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from ..models.auth_model import validate_user, get_user_display_name


auth_bp = Blueprint('auth', __name__)


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()

        if validate_user(username, password):
            session['user'] = username
            return redirect(url_for('dashboard.dashboard'))

        flash('Invalid username or password', 'error')

    return render_template('login/login.html')


@auth_bp.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('auth.login'))


