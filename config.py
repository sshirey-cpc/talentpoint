"""
TalentPoint — Tenant configuration system.

Reads all configuration from BigQuery tables instead of hardcoded constants.
Every deployment serves exactly one tenant (single-tenant, config-driven).
The tenant config is loaded once at startup and cached in memory.
"""

import os
import db


# In-memory config cache (loaded once at startup)
_tenant = None
_schools = None
_categories = None
_school_years = None
_user_roles = None


def load():
    """Load all tenant configuration from BigQuery. Call once at app startup."""
    global _tenant, _schools, _categories, _school_years, _user_roles

    tenant_id = get_tenant_id()

    _tenant = db.query_one(
        f"SELECT * FROM `{db.table_ref('tenant')}` WHERE tenant_id = @tid",
        [db.bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]
    )
    if not _tenant:
        raise RuntimeError(f"Tenant '{tenant_id}' not found in config. Run setup_tenant.py first.")

    _schools = db.query(
        f"SELECT * FROM `{db.table_ref('school')}` WHERE tenant_id = @tid AND active = TRUE ORDER BY sort_order",
        [db.bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]
    )

    _categories = db.query(
        f"SELECT * FROM `{db.table_ref('job_category')}` WHERE tenant_id = @tid AND active = TRUE ORDER BY sort_order",
        [db.bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]
    )

    _school_years = db.query(
        f"SELECT * FROM `{db.table_ref('school_year')}` WHERE tenant_id = @tid ORDER BY year_id",
        [db.bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]
    )

    _user_roles = db.query(
        f"SELECT * FROM `{db.table_ref('user_role')}` WHERE tenant_id = @tid AND active = TRUE",
        [db.bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]
    )


def get_tenant_id():
    return os.environ.get('TENANT_ID', 'default')


# --- Tenant ---

def tenant():
    """Return the full tenant config dict."""
    if _tenant is None:
        raise RuntimeError("Config not loaded. Call config.load() at startup.")
    return _tenant


def org_name():
    return tenant()['name']


def domain():
    return tenant()['domain']


def logo_url():
    return tenant().get('logo_url', '')


def primary_color():
    return tenant().get('primary_color', '#2563eb')


def secondary_color():
    return tenant().get('secondary_color', '#1e293b')


def timezone():
    return tenant().get('timezone', 'America/Chicago')


# --- HRIS Config ---

def hris_source_table():
    """Fully-qualified BQ table for HRIS data (e.g., project.dataset.staff_master_list)."""
    return tenant().get('hris_source_table', '')


def hris_field(field_name):
    """Get the HRIS column name mapping for a logical field.

    Example: hris_field('email') might return 'Email_Address'
    """
    key = f"hris_{field_name}_field"
    return tenant().get(key, field_name)


# --- Schools ---

def schools():
    """Return list of school dicts, sorted by sort_order."""
    return _schools or []


def school_names():
    """Return list of school short names."""
    return [s['name'] for s in schools()]


def site_schools():
    """Return only site schools (not network/central office)."""
    return [s for s in schools() if s.get('is_site_school', True)]


def school_by_id(school_id):
    """Look up a school by ID."""
    for s in schools():
        if s['school_id'] == school_id:
            return s
    return None


def school_by_name(name):
    """Look up a school by short name."""
    for s in schools():
        if s['name'] == name:
            return s
    return None


def hris_location_map():
    """Return dict mapping HRIS location names to school short names."""
    return {s['hris_location_name']: s['name'] for s in schools() if s.get('hris_location_name')}


# --- Job Categories ---

def categories():
    """Return list of category dicts."""
    return _categories or []


def category_names():
    """Return list of category names."""
    return [c['name'] for c in categories()]


# --- School Years ---

def school_years():
    """Return list of school year dicts."""
    return _school_years or []


def current_year():
    """Return the current school year dict."""
    for sy in school_years():
        if sy.get('is_current'):
            return sy
    return school_years()[-1] if school_years() else None


def planning_year():
    """Return the school year being planned (next year)."""
    for sy in school_years():
        if sy.get('is_planning'):
            return sy
    return None


def year_labels():
    """Return list of year ID strings (e.g., ['25-26', '26-27'])."""
    return [sy['year_id'] for sy in school_years()]


# --- User Roles ---

def user_roles():
    """Return list of user role dicts."""
    return _user_roles or []


def get_user_role(email):
    """Look up a user's role by email. Returns the role dict or None."""
    email_lower = email.lower().strip()
    for ur in user_roles():
        if ur['email'].lower().strip() == email_lower:
            return ur
    return None


def is_admin(email):
    """Check if a user has admin-level access."""
    role = get_user_role(email)
    if not role:
        return False
    return role['role'] in ('super_admin', 'admin')


def has_role(email, *roles):
    """Check if a user has any of the specified roles."""
    user = get_user_role(email)
    if not user:
        return False
    return user['role'] in roles


def reload():
    """Force reload all config from BigQuery."""
    load()
