"""
TalentPoint — Staff List Blueprint.

Customizable staff directory with:
- Config-driven columns (per tenant)
- Per-column filter dropdowns
- Column chooser with reorder
- Saved views (personal and shared)
- Multi-format export (CSV, JSON)
- Gmail compose integration
- All data through HRIS adapter
"""

import json
from flask import Blueprint, jsonify, request, render_template
from google.cloud import bigquery

import db
import config
import auth
import hris_adapter

bp = Blueprint('staff_list', __name__)


# ============================================================
# Views
# ============================================================

@bp.route('/')
@auth.require_auth
def staff_list_home():
    user = auth.get_current_user()
    permissions = auth.get_permissions(user.get('role', 'viewer'))
    columns = _get_columns()
    views = _get_views(user['email'])
    return render_template('staff_list.html',
                           user=user,
                           permissions=permissions,
                           org_name=config.org_name(),
                           logo_url=config.logo_url(),
                           primary_color=config.primary_color(),
                           secondary_color=config.secondary_color(),
                           columns=columns,
                           saved_views=views,
                           schools=config.schools())


# ============================================================
# Data APIs
# ============================================================

@bp.route('/api/data')
@auth.require_auth
def get_staff_data():
    """Get all staff data with all available fields."""
    columns = _get_columns()
    all_staff = hris_adapter.get_all_staff(active_only=request.args.get('include_inactive') != 'true')

    # Build response with column-mapped fields
    rows = []
    for staff in all_staff:
        row = {'_email': staff['email']}
        raw = staff.get('_raw', {})
        for col in columns:
            field = col['hris_field']
            # Try standardized field first, then raw
            if field in ('first_name', 'last_name'):
                row[col['column_id']] = staff.get(field, '')
            else:
                row[col['column_id']] = raw.get(field, '')
        rows.append(row)

    return jsonify({
        'rows': rows,
        'total': len(rows),
        'columns': [{'id': c['column_id'], 'name': c['display_name'], 'category': c['category'],
                      'data_type': c['data_type'], 'filterable': c['filterable'],
                      'default_visible': c['default_visible'], 'sort_order': c['sort_order']}
                     for c in columns],
    })


@bp.route('/api/filters')
@auth.require_auth
def get_filter_values():
    """Get distinct values for all filterable columns."""
    columns = _get_columns()
    filterable = [c for c in columns if c['filterable']]

    all_staff = hris_adapter.get_all_staff(active_only=True)

    filters = {}
    for col in filterable:
        field = col['hris_field']
        values = set()
        for staff in all_staff:
            raw = staff.get('_raw', {})
            val = raw.get(field, '')
            if val:
                values.add(str(val))
        filters[col['column_id']] = sorted(values)

    return jsonify(filters)


# ============================================================
# Saved Views
# ============================================================

@bp.route('/api/views')
@auth.require_auth
def get_views():
    """Get saved views for the current user."""
    user_email = auth.get_current_email()
    views = _get_views(user_email)
    return jsonify(views)


@bp.route('/api/views', methods=['POST'])
@auth.require_auth
def save_view():
    """Save a new view."""
    data = request.json
    user_email = auth.get_current_email()
    view_id = db.generate_id()
    now = db.now_iso()

    db.insert_row('staff_list_view', {
        'view_id': view_id,
        'tenant_id': config.get_tenant_id(),
        'name': data.get('name', 'Untitled View'),
        'created_by': user_email,
        'is_shared': data.get('is_shared', False),
        'visible_columns': json.dumps(data.get('visible_columns', [])),
        'filters': json.dumps(data.get('filters', {})),
        'sort_column': data.get('sort_column', ''),
        'sort_direction': data.get('sort_direction', 'asc'),
        'created_at': now,
        'updated_at': now,
    })

    return jsonify({'view_id': view_id}), 201


@bp.route('/api/views/<view_id>', methods=['PUT'])
@auth.require_auth
def update_view(view_id):
    """Update an existing view."""
    data = request.json
    now = db.now_iso()

    updates = {}
    if 'name' in data:
        updates['name'] = data['name']
    if 'visible_columns' in data:
        updates['visible_columns'] = json.dumps(data['visible_columns'])
    if 'filters' in data:
        updates['filters'] = json.dumps(data['filters'])
    if 'sort_column' in data:
        updates['sort_column'] = data['sort_column']
    if 'sort_direction' in data:
        updates['sort_direction'] = data['sort_direction']
    if 'is_shared' in data:
        updates['is_shared'] = data['is_shared']

    if updates:
        updates['updated_at'] = now
        db.update_row('staff_list_view', 'view_id', view_id, updates)

    return jsonify({'message': 'View updated'})


@bp.route('/api/views/<view_id>', methods=['DELETE'])
@auth.require_auth
def delete_view(view_id):
    """Delete a saved view."""
    db.delete_row('staff_list_view', 'view_id', view_id)
    return jsonify({'message': 'View deleted'})


# ============================================================
# Column Config
# ============================================================

@bp.route('/api/columns')
@auth.require_auth
def get_column_config():
    """Get all available columns with their config."""
    columns = _get_columns()
    return jsonify(columns)


# ============================================================
# Internal Helpers
# ============================================================

def _get_columns():
    """Get column definitions for this tenant."""
    return db.query(
        f"SELECT * FROM `{db.table_ref('staff_list_column')}` WHERE tenant_id = @tid AND active = TRUE ORDER BY sort_order",
        [bigquery.ScalarQueryParameter("tid", "STRING", config.get_tenant_id())]
    )


def _get_views(user_email):
    """Get saved views: user's own + shared views."""
    tenant_id = config.get_tenant_id()
    return db.query(f"""
        SELECT * FROM `{db.table_ref('staff_list_view')}`
        WHERE tenant_id = @tid AND (created_by = @email OR is_shared = TRUE)
        ORDER BY name
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("email", "STRING", user_email),
    ])
