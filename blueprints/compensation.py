"""
TalentPoint — Compensation Blueprint.

Two tools in one:
1. Salary Scale Calculator (public) — staff look up their salary by role + experience
2. Salary Projection Dashboard (C-Team only) — scenario modeling for compensation strategy

All salary data comes from config tables, not hardcoded.
Projection pulls live staff data through the HRIS adapter.
"""

import json
from flask import Blueprint, jsonify, request, render_template
from google.cloud import bigquery

import db
import config
import auth
import hris_adapter

bp = Blueprint('compensation', __name__)


# ============================================================
# Views
# ============================================================

@bp.route('/')
def compensation_home():
    user = auth.get_current_user() if auth.is_authenticated() else None
    permissions = auth.get_permissions(user.get('role', 'viewer')) if user else {}
    roles = _get_salary_roles()
    longevity = _get_longevity_tiers()
    return render_template('compensation.html',
                           user=user,
                           permissions=permissions,
                           org_name=config.org_name(),
                           logo_url=config.logo_url(),
                           primary_color=config.primary_color(),
                           secondary_color=config.secondary_color(),
                           salary_roles=roles,
                           longevity_tiers=longevity,
                           schools=config.schools())


# ============================================================
# Salary Scale APIs (public or minimal auth)
# ============================================================

@bp.route('/api/roles')
def get_roles():
    """Get all salary roles with their categories."""
    return jsonify(_get_salary_roles())


@bp.route('/api/schedule/<salary_key>')
def get_schedule(salary_key):
    """Get the full salary schedule for a role."""
    tenant_id = config.get_tenant_id()
    steps = db.query(f"""
        SELECT step, annual_amount, hourly_amount
        FROM `{db.table_ref('salary_schedule')}`
        WHERE tenant_id = @tid AND salary_key = @key
        ORDER BY step
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("key", "STRING", salary_key),
    ])
    return jsonify({'salary_key': salary_key, 'steps': steps})


@bp.route('/api/salary-at-step')
def salary_at_step():
    """Get salary for a specific role at a specific step, including longevity bonus."""
    salary_key = request.args.get('role', '')
    step = int(request.args.get('step', 0))
    org_years = int(request.args.get('org_years', 0))

    tenant_id = config.get_tenant_id()

    # Get base salary
    row = db.query_one(f"""
        SELECT annual_amount, hourly_amount
        FROM `{db.table_ref('salary_schedule')}`
        WHERE tenant_id = @tid AND salary_key = @key AND step = @step
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("key", "STRING", salary_key),
        bigquery.ScalarQueryParameter("step", "INT64", step),
    ])

    if not row:
        # Interpolate if exact step not in table
        row = _interpolate_salary(salary_key, step, tenant_id)

    base = row.get('annual_amount', 0) if row else 0
    hourly = row.get('hourly_amount') if row else None

    # Longevity bonus
    role = _get_role_by_key(salary_key)
    bonus = 0
    if role and role.get('longevity_eligible'):
        bonus = _get_longevity_bonus(org_years, tenant_id)

    return jsonify({
        'salary_key': salary_key,
        'step': step,
        'base_salary': base,
        'hourly_rate': hourly,
        'longevity_bonus': bonus,
        'total_compensation': base + bonus,
        'longevity_eligible': bool(role and role.get('longevity_eligible')),
    })


@bp.route('/api/longevity-tiers')
def get_longevity():
    return jsonify(_get_longevity_tiers())


@bp.route('/api/compare')
def compare_roles():
    """Compare up to 3 roles at the same experience level."""
    keys = request.args.get('roles', '').split(',')
    step = int(request.args.get('step', 0))
    tenant_id = config.get_tenant_id()

    results = []
    for key in keys[:3]:
        key = key.strip()
        if not key:
            continue
        row = db.query_one(f"""
            SELECT annual_amount FROM `{db.table_ref('salary_schedule')}`
            WHERE tenant_id = @tid AND salary_key = @key AND step = @step
        """, [
            bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
            bigquery.ScalarQueryParameter("key", "STRING", key),
            bigquery.ScalarQueryParameter("step", "INT64", step),
        ])

        # Max salary (step 30 or highest available)
        max_row = db.query_one(f"""
            SELECT MAX(annual_amount) as max_salary FROM `{db.table_ref('salary_schedule')}`
            WHERE tenant_id = @tid AND salary_key = @key
        """, [
            bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
            bigquery.ScalarQueryParameter("key", "STRING", key),
        ])

        role = _get_role_by_key(key)
        results.append({
            'salary_key': key,
            'role_name': role['name'] if role else key,
            'category': role['category'] if role else '',
            'salary_at_step': row['annual_amount'] if row else 0,
            'max_salary': max_row['max_salary'] if max_row else 0,
            'longevity_eligible': bool(role and role.get('longevity_eligible')),
        })

    return jsonify({'step': step, 'comparisons': results})


# ============================================================
# Salary Projection APIs (C-Team only)
# ============================================================

@bp.route('/api/projection/staff')
@auth.require_permission('can_view_compensation')
def get_projection_staff():
    """Get staff roster categorized for salary projection."""
    tenant_id = config.get_tenant_id()
    categories = _get_salary_categories()

    # Get all active staff from HRIS
    all_staff = hris_adapter.get_all_staff(active_only=True)

    # Categorize each employee by title pattern matching
    categorized = []
    for staff in all_staff:
        title = (staff.get('job_title') or '').lower()
        matched_cat = None
        for cat in categories:
            patterns = json.loads(cat.get('title_patterns', '[]'))
            for pattern in patterns:
                pattern_clean = pattern.strip('%').lower()
                if pattern_clean in title:
                    matched_cat = cat
                    break
            if matched_cat:
                break

        if matched_cat:
            categorized.append({
                'email': staff['email'],
                'name': staff['full_name'],
                'school': staff.get('school', ''),
                'job_title': staff.get('job_title', ''),
                'category': matched_cat['name'],
                'yoe': staff.get('_raw', {}).get(config.hris_field('hire_date'), ''),
            })

    return jsonify({
        'staff': categorized,
        'categories': [{'name': c['name'], 'default_base': c['default_base']} for c in categories],
        'total': len(categorized),
    })


@bp.route('/api/projection/scenario', methods=['POST'])
@auth.require_permission('can_view_compensation')
def calculate_scenario():
    """Calculate a salary projection scenario.

    Body:
        categories: dict of category_name -> { base: float, annual_increase: float, use_schedule: bool }
        step_cap: int (max step to pay at)
    """
    data = request.json
    tenant_id = config.get_tenant_id()
    step_cap = data.get('step_cap', 30)
    category_params = data.get('categories', {})

    # Get staff
    staff_response = get_projection_staff()
    staff_data = staff_response.get_json()
    staff = staff_data.get('staff', [])

    results = []
    totals = {'current': 0, 'projected': 0, 'by_category': {}}

    for person in staff:
        cat_name = person['category']
        params = category_params.get(cat_name, {})
        base = params.get('base', 0)
        increase = params.get('annual_increase', 0.02)
        use_schedule = params.get('use_schedule', True)

        # Estimate YOE from hire date (simplified)
        yoe = _estimate_yoe(person.get('yoe', ''))
        capped_yoe = min(yoe, step_cap)

        if use_schedule:
            # Look up from schedule
            projected = _lookup_schedule_salary(cat_name, capped_yoe, tenant_id) or base
        else:
            # Calculate from formula: base * (1 + increase)^yoe
            projected = base * ((1 + increase) ** capped_yoe)

        current = _lookup_schedule_salary(cat_name, yoe, tenant_id) or base

        results.append({
            'name': person['name'],
            'school': person['school'],
            'job_title': person['job_title'],
            'category': cat_name,
            'yoe': yoe,
            'current_salary': round(current),
            'projected_salary': round(projected),
            'difference': round(projected - current),
        })

        totals['current'] += current
        totals['projected'] += projected
        if cat_name not in totals['by_category']:
            totals['by_category'][cat_name] = {'count': 0, 'current': 0, 'projected': 0}
        totals['by_category'][cat_name]['count'] += 1
        totals['by_category'][cat_name]['current'] += current
        totals['by_category'][cat_name]['projected'] += projected

    totals['current'] = round(totals['current'])
    totals['projected'] = round(totals['projected'])
    totals['difference'] = totals['projected'] - totals['current']
    for cat in totals['by_category'].values():
        cat['current'] = round(cat['current'])
        cat['projected'] = round(cat['projected'])
        cat['avg_raise'] = round((cat['projected'] - cat['current']) / cat['count']) if cat['count'] else 0

    return jsonify({'employees': results, 'totals': totals, 'step_cap': step_cap})


# ============================================================
# Internal Helpers
# ============================================================

def _get_salary_roles():
    return db.query(
        f"SELECT * FROM `{db.table_ref('salary_role')}` WHERE tenant_id = @tid AND active = TRUE ORDER BY sort_order",
        [bigquery.ScalarQueryParameter("tid", "STRING", config.get_tenant_id())]
    )


def _get_role_by_key(salary_key):
    for r in _get_salary_roles():
        if r['salary_key'] == salary_key:
            return r
    return None


def _get_longevity_tiers():
    return db.query(
        f"SELECT * FROM `{db.table_ref('longevity_tier')}` WHERE tenant_id = @tid ORDER BY min_years",
        [bigquery.ScalarQueryParameter("tid", "STRING", config.get_tenant_id())]
    )


def _get_longevity_bonus(org_years, tenant_id):
    tiers = db.query(
        f"SELECT * FROM `{db.table_ref('longevity_tier')}` WHERE tenant_id = @tid ORDER BY min_years",
        [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]
    )
    for t in tiers:
        if t['min_years'] <= org_years <= t['max_years']:
            return t['bonus_amount']
    return 0


def _get_salary_categories():
    return db.query(
        f"SELECT * FROM `{db.table_ref('salary_category')}` WHERE tenant_id = @tid ORDER BY sort_order",
        [bigquery.ScalarQueryParameter("tid", "STRING", config.get_tenant_id())]
    )


def _interpolate_salary(salary_key, step, tenant_id):
    """If exact step not in table, interpolate between nearest available steps."""
    lower = db.query_one(f"""
        SELECT step, annual_amount FROM `{db.table_ref('salary_schedule')}`
        WHERE tenant_id = @tid AND salary_key = @key AND step <= @step
        ORDER BY step DESC LIMIT 1
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("key", "STRING", salary_key),
        bigquery.ScalarQueryParameter("step", "INT64", step),
    ])
    upper = db.query_one(f"""
        SELECT step, annual_amount FROM `{db.table_ref('salary_schedule')}`
        WHERE tenant_id = @tid AND salary_key = @key AND step >= @step
        ORDER BY step ASC LIMIT 1
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("key", "STRING", salary_key),
        bigquery.ScalarQueryParameter("step", "INT64", step),
    ])

    if lower and upper and lower['step'] != upper['step']:
        ratio = (step - lower['step']) / (upper['step'] - lower['step'])
        interpolated = lower['annual_amount'] + ratio * (upper['annual_amount'] - lower['annual_amount'])
        return {'annual_amount': round(interpolated), 'hourly_amount': None}
    elif lower:
        return lower
    elif upper:
        return upper
    return None


def _lookup_schedule_salary(category_name, yoe, tenant_id):
    """Look up salary from schedule by category name and YOE."""
    cats = _get_salary_categories()
    for cat in cats:
        if cat['name'] == category_name:
            # Map category to salary_key (simplified: use first matching role)
            roles = _get_salary_roles()
            for role in roles:
                if role['category'] and category_name.lower() in role['name'].lower():
                    row = db.query_one(f"""
                        SELECT annual_amount FROM `{db.table_ref('salary_schedule')}`
                        WHERE tenant_id = @tid AND salary_key = @key AND step = @step
                    """, [
                        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
                        bigquery.ScalarQueryParameter("key", "STRING", role['salary_key']),
                        bigquery.ScalarQueryParameter("step", "INT64", yoe),
                    ])
                    if row:
                        return row['annual_amount']
    return None


def _estimate_yoe(hire_date_str):
    """Estimate years of experience from hire date string."""
    if not hire_date_str:
        return 0
    try:
        from datetime import datetime
        hire = datetime.strptime(str(hire_date_str)[:10], '%Y-%m-%d')
        now = datetime.now()
        return max(0, (now - hire).days // 365)
    except (ValueError, TypeError):
        return 0
