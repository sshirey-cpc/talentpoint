"""
TalentPoint — Shared authentication and authorization.

Google OAuth 2.0 with role-based access from the user_role table.
Replaces all hardcoded ADMIN_EMAILS and TITLE_ROLES patterns.
"""

import os
import functools
from flask import session, redirect, url_for, jsonify, request
from authlib.integrations.flask_client import OAuth

import config


oauth = OAuth()


def init_app(app):
    """Initialize OAuth with the Flask app."""
    oauth.init_app(app)
    oauth.register(
        name='google',
        client_id=os.environ.get('GOOGLE_CLIENT_ID'),
        client_secret=os.environ.get('GOOGLE_CLIENT_SECRET'),
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={'scope': 'openid email profile'},
    )

    # Register auth routes
    app.add_url_rule('/login', 'login', login)
    app.add_url_rule('/auth/callback', 'auth_callback', auth_callback)
    app.add_url_rule('/logout', 'logout', logout)
    app.add_url_rule('/api/auth/status', 'auth_status', auth_status)


def login():
    """Initiate Google OAuth flow."""
    redirect_uri = _get_redirect_uri()
    return oauth.google.authorize_redirect(redirect_uri)


def auth_callback():
    """Handle Google OAuth callback."""
    token = oauth.google.authorize_access_token()
    user_info = token.get('userinfo', {})
    email = user_info.get('email', '').lower().strip()

    # Domain check
    allowed_domain = config.domain()
    if allowed_domain and not email.endswith(f'@{allowed_domain}'):
        return f"Access restricted to @{allowed_domain} accounts.", 403

    # Store user info in session
    session['user'] = {
        'email': email,
        'name': user_info.get('name', email.split('@')[0]),
        'picture': user_info.get('picture', ''),
    }

    # Look up role
    user_role = config.get_user_role(email)
    if user_role:
        session['user']['role'] = user_role['role']
        session['user']['title'] = user_role.get('title', '')
    else:
        session['user']['role'] = 'viewer'
        session['user']['title'] = ''

    return redirect('/')


def logout():
    """Clear session and redirect to login."""
    session.clear()
    return redirect('/')


def auth_status():
    """Return current auth status and user permissions."""
    if not is_authenticated():
        return jsonify({'authenticated': False})

    user = get_current_user()
    return jsonify({
        'authenticated': True,
        'email': user['email'],
        'name': user['name'],
        'picture': user.get('picture', ''),
        'role': user.get('role', 'viewer'),
        'title': user.get('title', ''),
        'permissions': get_permissions(user.get('role', 'viewer')),
    })


# --- Session Helpers ---

def is_authenticated():
    """Check if the current request has a valid session."""
    if os.environ.get('DEV_MODE') == 'true':
        if 'user' not in session:
            dev_email = os.environ.get('DEV_USER_EMAIL', 'dev@example.com')
            # Look up real user info from config
            user_role = config.get_user_role(dev_email)
            if user_role:
                session['user'] = {
                    'email': dev_email,
                    'name': user_role.get('name', dev_email.split('@')[0]),
                    'role': user_role.get('role', 'super_admin'),
                    'title': user_role.get('title', ''),
                    'picture': '',
                }
            else:
                session['user'] = {
                    'email': dev_email,
                    'name': dev_email.split('@')[0].replace('.', ' ').title(),
                    'role': 'super_admin',
                    'title': '',
                    'picture': '',
                }
        return True
    return 'user' in session


def get_current_user():
    """Return the current user dict from the session."""
    return session.get('user', {})


def get_current_email():
    """Return the current user's email."""
    return get_current_user().get('email', '')


def get_current_role():
    """Return the current user's role string."""
    return get_current_user().get('role', 'viewer')


# --- Role Permissions ---

# Permission sets by role — configurable per deployment
ROLE_PERMISSIONS = {
    'super_admin': {
        'can_view': True,
        'can_edit': True,
        'can_delete': True,
        'can_approve_all': True,
        'can_manage_users': True,
        'can_view_compensation': True,
        'can_view_all_schools': True,
        'can_create_positions': True,
        'can_edit_notes': True,
        'can_edit_dates': True,
        'can_archive': True,
    },
    'admin': {
        'can_view': True,
        'can_edit': True,
        'can_delete': False,
        'can_approve_all': False,
        'can_manage_users': False,
        'can_view_compensation': True,
        'can_view_all_schools': True,
        'can_create_positions': True,
        'can_edit_notes': True,
        'can_edit_dates': True,
        'can_archive': True,
    },
    'hr': {
        'can_view': True,
        'can_edit': True,
        'can_delete': False,
        'can_approve_all': False,
        'can_manage_users': False,
        'can_view_compensation': False,
        'can_view_all_schools': True,
        'can_create_positions': True,
        'can_edit_notes': True,
        'can_edit_dates': True,
        'can_archive': True,
    },
    'finance': {
        'can_view': True,
        'can_edit': False,
        'can_delete': False,
        'can_approve_all': False,
        'can_manage_users': False,
        'can_view_compensation': True,
        'can_view_all_schools': True,
        'can_create_positions': False,
        'can_edit_notes': False,
        'can_edit_dates': False,
        'can_archive': False,
    },
    'ceo': {
        'can_view': True,
        'can_edit': False,
        'can_delete': False,
        'can_approve_all': True,
        'can_manage_users': False,
        'can_view_compensation': True,
        'can_view_all_schools': True,
        'can_create_positions': False,
        'can_edit_notes': False,
        'can_edit_dates': False,
        'can_archive': False,
    },
    'principal': {
        'can_view': True,
        'can_edit': False,
        'can_delete': False,
        'can_approve_all': False,
        'can_manage_users': False,
        'can_view_compensation': False,
        'can_view_all_schools': False,  # Only their school
        'can_create_positions': False,
        'can_edit_notes': False,
        'can_edit_dates': False,
        'can_archive': False,
    },
    'viewer': {
        'can_view': True,
        'can_edit': False,
        'can_delete': False,
        'can_approve_all': False,
        'can_manage_users': False,
        'can_view_compensation': False,
        'can_view_all_schools': False,
        'can_create_positions': False,
        'can_edit_notes': False,
        'can_edit_dates': False,
        'can_archive': False,
    },
}


def get_permissions(role):
    """Return the permission dict for a role."""
    return ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS['viewer'])


def has_permission(permission):
    """Check if the current user has a specific permission."""
    role = get_current_role()
    perms = get_permissions(role)
    return perms.get(permission, False)


# --- Decorators ---

def require_auth(f):
    """Decorator: require authentication."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not is_authenticated():
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Not authenticated'}), 401
            return redirect('/login')
        return f(*args, **kwargs)
    return wrapper


def require_role(*roles):
    """Decorator: require one of the specified roles.

    Usage:
        @require_role('admin', 'hr', 'super_admin')
        def my_endpoint():
            ...
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if not is_authenticated():
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Not authenticated'}), 401
                return redirect('/login')
            if get_current_role() not in roles:
                return jsonify({'error': 'Insufficient permissions'}), 403
            return f(*args, **kwargs)
        return wrapper
    return decorator


def require_permission(permission):
    """Decorator: require a specific permission.

    Usage:
        @require_permission('can_edit')
        def my_endpoint():
            ...
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if not is_authenticated():
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Not authenticated'}), 401
                return redirect('/login')
            if not has_permission(permission):
                return jsonify({'error': 'Insufficient permissions'}), 403
            return f(*args, **kwargs)
        return wrapper
    return decorator


# --- Internal ---

def _get_redirect_uri():
    """Build the OAuth redirect URI, handling Cloud Run URL formats."""
    if os.environ.get('REDIRECT_URI'):
        return os.environ['REDIRECT_URI']
    return url_for('auth_callback', _external=True)
