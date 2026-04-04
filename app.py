"""
TalentPoint — Main Flask application.

This is the entry point. It initializes the app, loads tenant config,
registers blueprints, and serves the shared UI shell.
"""

import os
import secrets
from flask import Flask, render_template, jsonify
from flask_cors import CORS

import config
import auth


def create_app():
    app = Flask(__name__,
                template_folder='templates',
                static_folder='static')

    # Security
    app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))
    app.config['SESSION_COOKIE_SECURE'] = os.environ.get('DEV_MODE') != 'true'
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

    CORS(app)

    # Load tenant configuration from BigQuery
    if os.environ.get('SKIP_CONFIG_LOAD') != 'true':
        config.load()

    # Initialize auth
    auth.init_app(app)

    # --- Shared Routes ---

    @app.route('/')
    def index():
        if not auth.is_authenticated():
            return render_template('login.html',
                                   org_name=config.org_name(),
                                   logo_url=config.logo_url(),
                                   primary_color=config.primary_color(),
                                   secondary_color=config.secondary_color())
        user = auth.get_current_user()
        permissions = auth.get_permissions(user.get('role', 'viewer'))
        return render_template('dashboard.html',
                               user=user,
                               permissions=permissions,
                               org_name=config.org_name(),
                               logo_url=config.logo_url(),
                               primary_color=config.primary_color(),
                               secondary_color=config.secondary_color(),
                               schools=config.school_names(),
                               current_year=config.current_year(),
                               planning_year=config.planning_year())

    @app.route('/api/config/schools')
    @auth.require_auth
    def api_schools():
        return jsonify(config.schools())

    @app.route('/api/config/categories')
    @auth.require_auth
    def api_categories():
        return jsonify(config.categories())

    @app.route('/api/config/school-years')
    @auth.require_auth
    def api_school_years():
        return jsonify(config.school_years())

    @app.route('/health')
    def health():
        return jsonify({'status': 'ok', 'product': 'TalentPoint', 'tenant': config.get_tenant_id()})

    # --- Register Module Blueprints ---
    # Each module is optional — only register if the blueprint file exists.
    # This allows enabling/disabling modules per tenant.

    _try_register_blueprint(app, 'blueprints.staffing', url_prefix='/staffing')
    _try_register_blueprint(app, 'blueprints.requests', url_prefix='/requests')
    _try_register_blueprint(app, 'blueprints.pipeline', url_prefix='/pipeline')
    _try_register_blueprint(app, 'blueprints.referral', url_prefix='/referral')
    _try_register_blueprint(app, 'blueprints.onboarding', url_prefix='/onboarding')
    _try_register_blueprint(app, 'blueprints.compensation', url_prefix='/compensation')
    _try_register_blueprint(app, 'blueprints.staff_list', url_prefix='/staff-list')

    return app


def _try_register_blueprint(app, module_path, url_prefix):
    """Try to import and register a blueprint. Skip silently if not found."""
    try:
        module = __import__(module_path, fromlist=['bp'])
        if hasattr(module, 'bp'):
            app.register_blueprint(module.bp, url_prefix=url_prefix)
    except ImportError:
        pass  # Module not yet implemented — that's fine


# Entry point for gunicorn and local dev
app = create_app()

if __name__ == '__main__':
    os.environ.setdefault('DEV_MODE', 'true')
    app.run(host='0.0.0.0', port=8080, debug=True)
