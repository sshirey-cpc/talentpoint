"""
TalentPoint — Staffing Blueprint.

Position management, staffing matrix, HRIS reconciliation, and assignment tracking.
This is the core module — everything else connects through positions.
"""

from flask import Blueprint, jsonify, request, render_template
from google.cloud import bigquery

import db
import config
import auth
import hris_adapter

bp = Blueprint('staffing', __name__)


# ============================================================
# Views
# ============================================================

@bp.route('/')
@auth.require_auth
def staffing_home():
    user = auth.get_current_user()
    permissions = auth.get_permissions(user.get('role', 'viewer'))
    return render_template('staffing.html',
                           user=user,
                           permissions=permissions,
                           org_name=config.org_name(),
                           logo_url=config.logo_url(),
                           primary_color=config.primary_color(),
                           secondary_color=config.secondary_color(),
                           schools=config.schools(),
                           categories=config.categories(),
                           current_year=config.current_year(),
                           planning_year=config.planning_year(),
                           school_years=config.school_years())


# ============================================================
# Position CRUD
# ============================================================

@bp.route('/api/positions')
@auth.require_auth
def get_positions():
    """Get all positions with optional filters and HRIS mismatch flags."""
    tenant_id = config.get_tenant_id()
    year = request.args.get('year')
    school = request.args.get('school')
    category = request.args.get('category')
    status = request.args.get('status')
    search = request.args.get('search', '').strip()

    t_pos = db.table_ref('position')
    t_assign = db.table_ref('position_assignment')

    # Base query: positions LEFT JOIN assignments for the requested year
    sql = f"""
        SELECT
            p.*,
            a.assignment_id,
            a.employee_name,
            a.employee_email,
            a.employee_number,
            a.assignment_status,
            a.itr_response,
            a.candidate_name as assignment_candidate
        FROM `{t_pos}` p
        LEFT JOIN `{t_assign}` a
            ON p.position_id = a.position_id
            AND a.school_year = @year
        WHERE p.tenant_id = @tenant_id
    """
    params = [
        bigquery.ScalarQueryParameter("tenant_id", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("year", "STRING", year or config.current_year()['year_id']),
    ]

    # Year lifecycle filter
    if year:
        sql += " AND p.start_year <= @year AND (p.end_year >= @year OR p.end_year IS NULL)"

    # Optional filters
    if school:
        sql += " AND p.school_id = @school"
        params.append(bigquery.ScalarQueryParameter("school", "STRING", school))
    if category:
        sql += " AND p.category_id = @category"
        params.append(bigquery.ScalarQueryParameter("category", "STRING", category))
    if status:
        sql += " AND p.status = @status"
        params.append(bigquery.ScalarQueryParameter("status", "STRING", status))
    if search:
        sql += " AND (LOWER(p.job_title) LIKE @search OR LOWER(a.employee_name) LIKE @search OR LOWER(a.employee_email) LIKE @search)"
        params.append(bigquery.ScalarQueryParameter("search", "STRING", f"%{search.lower()}%"))

    sql += " ORDER BY p.school_id, p.category_id, p.job_title"

    positions = db.query(sql, params)

    # Add HRIS mismatch flags
    positions = _add_mismatch_flags(positions)

    # Map school_id and category_id to names for display
    for p in positions:
        s = config.school_by_id(p.get('school_id', ''))
        p['school_name'] = s['name'] if s else p.get('school_id', '')
        cat = next((c for c in config.categories() if c['category_id'] == p.get('category_id')), None)
        p['category_name'] = cat['name'] if cat else p.get('category_id', '')

    return jsonify(positions)


@bp.route('/api/positions/<position_id>')
@auth.require_auth
def get_position(position_id):
    """Get a single position with all assignments."""
    tenant_id = config.get_tenant_id()
    t_pos = db.table_ref('position')
    t_assign = db.table_ref('position_assignment')

    position = db.query_one(
        f"SELECT * FROM `{t_pos}` WHERE position_id = @pid AND tenant_id = @tid",
        [
            bigquery.ScalarQueryParameter("pid", "STRING", position_id),
            bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        ]
    )
    if not position:
        return jsonify({'error': 'Position not found'}), 404

    assignments = db.query(
        f"SELECT * FROM `{t_assign}` WHERE position_id = @pid ORDER BY school_year",
        [bigquery.ScalarQueryParameter("pid", "STRING", position_id)]
    )
    position['assignments'] = assignments

    return jsonify(position)


@bp.route('/api/positions', methods=['POST'])
@auth.require_permission('can_create_positions')
def create_position():
    """Create a new position."""
    data = request.json
    tenant_id = config.get_tenant_id()
    user_email = auth.get_current_email()
    position_id = db.generate_id()
    now = db.now_iso()

    row = {
        'position_id': position_id,
        'tenant_id': tenant_id,
        'school_id': data.get('school_id', ''),
        'category_id': data.get('category_id', ''),
        'job_title': data.get('job_title', ''),
        'subject': data.get('subject', ''),
        'grade_level': data.get('grade_level', ''),
        'funding_source': data.get('funding_source', ''),
        'fte': data.get('fte', 1.0),
        'position_code': data.get('position_code', ''),
        'staffing_matrix': data.get('staffing_matrix', ''),
        'status': data.get('status', 'active'),
        'start_year': data.get('start_year', config.current_year()['year_id']),
        'end_year': data.get('end_year'),
        'request_id': data.get('request_id'),
        'notes': data.get('notes', ''),
        'created_by': user_email,
        'created_at': now,
        'updated_by': user_email,
        'updated_at': now,
    }
    db.insert_row('position', row)

    # Audit
    _log_history(position_id, 'position', position_id, 'CREATE', '', '', f"Position created: {row['job_title']}", user_email)

    return jsonify({'position_id': position_id, 'message': 'Position created'}), 201


@bp.route('/api/positions/<position_id>', methods=['PUT'])
@auth.require_permission('can_edit')
def update_position(position_id):
    """Update a position. Logs field-level changes to history."""
    data = request.json
    tenant_id = config.get_tenant_id()
    user_email = auth.get_current_email()
    now = db.now_iso()

    # Get current values
    current = db.query_one(
        f"SELECT * FROM `{db.table_ref('position')}` WHERE position_id = @pid AND tenant_id = @tid",
        [
            bigquery.ScalarQueryParameter("pid", "STRING", position_id),
            bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        ]
    )
    if not current:
        return jsonify({'error': 'Position not found'}), 404

    editable = ['school_id', 'category_id', 'job_title', 'subject', 'grade_level',
                'funding_source', 'fte', 'position_code', 'staffing_matrix', 'status',
                'start_year', 'end_year', 'notes']

    updates = {}
    changes = 0
    for field in editable:
        if field in data and str(data[field]) != str(current.get(field, '')):
            updates[field] = data[field]
            _log_history(position_id, 'position', position_id, 'UPDATE', field,
                         str(current.get(field, '')), str(data[field]), user_email)
            changes += 1

    if updates:
        updates['updated_by'] = user_email
        updates['updated_at'] = now
        db.update_row('position', 'position_id', position_id, updates)

    return jsonify({'changes': changes})


@bp.route('/api/positions/<position_id>', methods=['DELETE'])
@auth.require_permission('can_delete')
def delete_position(position_id):
    """Delete a position and log it."""
    tenant_id = config.get_tenant_id()
    user_email = auth.get_current_email()

    _log_history(position_id, 'position', position_id, 'DELETE', '', '', 'Position deleted', user_email)
    db.delete_row('position', 'position_id', position_id)

    return jsonify({'message': 'Position deleted'})


# ============================================================
# Assignments
# ============================================================

@bp.route('/api/positions/<position_id>/assignments', methods=['POST'])
@auth.require_permission('can_edit')
def create_assignment(position_id):
    """Create or update an assignment for a position + school year."""
    data = request.json
    user_email = auth.get_current_email()
    assignment_id = db.generate_id()
    now = db.now_iso()

    row = {
        'assignment_id': assignment_id,
        'position_id': position_id,
        'tenant_id': config.get_tenant_id(),
        'school_year': data.get('school_year', config.current_year()['year_id']),
        'employee_name': data.get('employee_name', ''),
        'employee_email': data.get('employee_email', ''),
        'employee_number': data.get('employee_number', ''),
        'assignment_status': data.get('assignment_status', 'active'),
        'itr_response': data.get('itr_response', ''),
        'candidate_name': data.get('candidate_name', ''),
        'start_date': data.get('start_date'),
        'end_date': data.get('end_date'),
        'notes': data.get('notes', ''),
        'created_by': user_email,
        'created_at': now,
        'updated_by': user_email,
        'updated_at': now,
    }
    db.insert_row('position_assignment', row)

    _log_history(position_id, 'position_assignment', assignment_id, 'CREATE', '', '',
                 f"Assignment created: {row['employee_name']} ({row['school_year']})", user_email)

    return jsonify({'assignment_id': assignment_id}), 201


@bp.route('/api/assignments/<assignment_id>', methods=['PUT'])
@auth.require_permission('can_edit')
def update_assignment(assignment_id):
    """Update an assignment."""
    data = request.json
    user_email = auth.get_current_email()
    now = db.now_iso()

    current = db.query_one(
        f"SELECT * FROM `{db.table_ref('position_assignment')}` WHERE assignment_id = @aid",
        [bigquery.ScalarQueryParameter("aid", "STRING", assignment_id)]
    )
    if not current:
        return jsonify({'error': 'Assignment not found'}), 404

    editable = ['employee_name', 'employee_email', 'employee_number', 'assignment_status',
                'itr_response', 'candidate_name', 'start_date', 'end_date', 'notes']

    updates = {}
    for field in editable:
        if field in data and str(data[field]) != str(current.get(field, '')):
            updates[field] = data[field]
            _log_history(current['position_id'], 'position_assignment', assignment_id,
                         'UPDATE', field, str(current.get(field, '')), str(data[field]), user_email)

    if updates:
        updates['updated_by'] = user_email
        updates['updated_at'] = now
        db.update_row('position_assignment', 'assignment_id', assignment_id, updates)

    return jsonify({'changes': len(updates)})


# ============================================================
# Stats & Aggregates
# ============================================================

@bp.route('/api/positions/stats')
@auth.require_auth
def get_stats():
    """Aggregate position statistics for a school year."""
    tenant_id = config.get_tenant_id()
    year = request.args.get('year', config.current_year()['year_id'])

    t_pos = db.table_ref('position')
    t_assign = db.table_ref('position_assignment')

    sql = f"""
        SELECT
            COUNT(DISTINCT p.position_id) as total_positions,
            COUNTIF(a.assignment_status = 'active') as filled,
            COUNTIF(a.assignment_status = 'open' OR a.assignment_id IS NULL) as open_positions,
            COUNTIF(a.assignment_status = 'returning') as returning,
            COUNTIF(a.assignment_status = 'leaving') as leaving,
            COUNTIF(a.assignment_status = 'new_hire') as new_hires,
            COUNTIF(a.itr_response = 'Unsure') as at_risk
        FROM `{t_pos}` p
        LEFT JOIN `{t_assign}` a
            ON p.position_id = a.position_id AND a.school_year = @year
        WHERE p.tenant_id = @tid
            AND p.status = 'active'
            AND p.start_year <= @year
            AND (p.end_year >= @year OR p.end_year IS NULL)
    """
    params = [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("year", "STRING", year),
    ]
    stats = db.query_one(sql, params)
    return jsonify(stats or {})


@bp.route('/api/positions/by-school')
@auth.require_auth
def get_by_school():
    """Position counts grouped by school and category."""
    tenant_id = config.get_tenant_id()
    year = request.args.get('year', config.current_year()['year_id'])

    t_pos = db.table_ref('position')
    t_assign = db.table_ref('position_assignment')

    sql = f"""
        SELECT
            p.school_id,
            p.category_id,
            COUNT(DISTINCT p.position_id) as total,
            COUNTIF(a.assignment_status IN ('active', 'returning', 'new_hire')) as filled,
            COUNTIF(a.assignment_status = 'open' OR a.assignment_id IS NULL) as open_count
        FROM `{t_pos}` p
        LEFT JOIN `{t_assign}` a
            ON p.position_id = a.position_id AND a.school_year = @year
        WHERE p.tenant_id = @tid
            AND p.status = 'active'
            AND p.start_year <= @year
            AND (p.end_year >= @year OR p.end_year IS NULL)
        GROUP BY p.school_id, p.category_id
        ORDER BY p.school_id, p.category_id
    """
    params = [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("year", "STRING", year),
    ]
    rows = db.query(sql, params)

    # Enrich with names
    for r in rows:
        s = config.school_by_id(r['school_id'])
        r['school_name'] = s['name'] if s else r['school_id']
        cat = next((c for c in config.categories() if c['category_id'] == r['category_id']), None)
        r['category_name'] = cat['name'] if cat else r['category_id']

    return jsonify(rows)


# ============================================================
# Staffing Matrix
# ============================================================

@bp.route('/api/matrix')
@auth.require_auth
def get_matrix():
    """Get staffing matrix: targets vs actual by school and category."""
    tenant_id = config.get_tenant_id()
    year = request.args.get('year', config.planning_year()['year_id'] if config.planning_year() else config.current_year()['year_id'])

    t_target = db.table_ref('staffing_target')
    t_pos = db.table_ref('position')
    t_assign = db.table_ref('position_assignment')

    # Get targets
    targets = db.query(
        f"SELECT * FROM `{t_target}` WHERE tenant_id = @tid AND school_year = @year",
        [
            bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
            bigquery.ScalarQueryParameter("year", "STRING", year),
        ]
    )

    # Get actuals
    actuals = db.query(f"""
        SELECT p.school_id, p.category_id, COUNT(*) as actual_count
        FROM `{t_pos}` p
        LEFT JOIN `{t_assign}` a ON p.position_id = a.position_id AND a.school_year = @year
        WHERE p.tenant_id = @tid AND p.status = 'active'
            AND p.start_year <= @year AND (p.end_year >= @year OR p.end_year IS NULL)
        GROUP BY p.school_id, p.category_id
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("year", "STRING", year),
    ])

    # Build matrix
    actual_map = {(r['school_id'], r['category_id']): r['actual_count'] for r in actuals}
    matrix = []
    for school in config.schools():
        row = {'school_id': school['school_id'], 'school_name': school['name'], 'categories': {}}
        for cat in config.categories():
            target = next((t for t in targets if t['school_id'] == school['school_id'] and t['category_id'] == cat['category_id']), None)
            actual = actual_map.get((school['school_id'], cat['category_id']), 0)
            row['categories'][cat['name']] = {
                'target': target['target_count'] if target else 0,
                'actual': actual,
                'delta': actual - (target['target_count'] if target else 0),
            }
        matrix.append(row)

    return jsonify({'year': year, 'matrix': matrix})


@bp.route('/api/matrix/target', methods=['PUT'])
@auth.require_permission('can_edit')
def set_target():
    """Set or update a staffing target."""
    data = request.json
    tenant_id = config.get_tenant_id()
    user_email = auth.get_current_email()
    now = db.now_iso()

    t_target = db.table_ref('staffing_target')

    # Check if target exists
    existing = db.query_one(f"""
        SELECT target_id FROM `{t_target}`
        WHERE tenant_id = @tid AND school_id = @sid AND category_id = @cid AND school_year = @year
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("sid", "STRING", data['school_id']),
        bigquery.ScalarQueryParameter("cid", "STRING", data['category_id']),
        bigquery.ScalarQueryParameter("year", "STRING", data['school_year']),
    ])

    if existing:
        db.update_row('staffing_target', 'target_id', existing['target_id'], {
            'target_count': data['target_count'],
            'notes': data.get('notes', ''),
            'updated_by': user_email,
            'updated_at': now,
        })
    else:
        db.insert_row('staffing_target', {
            'target_id': db.generate_id(),
            'tenant_id': tenant_id,
            'school_id': data['school_id'],
            'category_id': data['category_id'],
            'school_year': data['school_year'],
            'job_title': data.get('job_title', ''),
            'target_count': data['target_count'],
            'notes': data.get('notes', ''),
            'created_by': user_email,
            'created_at': now,
            'updated_by': user_email,
            'updated_at': now,
        })

    return jsonify({'message': 'Target updated'})


# ============================================================
# HRIS Reconciliation
# ============================================================

@bp.route('/api/reconciliation')
@auth.require_auth
def get_reconciliation():
    """Compare positions against HRIS data and flag mismatches."""
    tenant_id = config.get_tenant_id()
    year = request.args.get('year', config.current_year()['year_id'])

    t_pos = db.table_ref('position')
    t_assign = db.table_ref('position_assignment')

    # Get all assigned positions for this year
    positions = db.query(f"""
        SELECT p.position_id, p.school_id, p.job_title, p.subject,
               a.employee_name, a.employee_email, a.assignment_status
        FROM `{t_pos}` p
        JOIN `{t_assign}` a ON p.position_id = a.position_id AND a.school_year = @year
        WHERE p.tenant_id = @tid AND p.status = 'active'
            AND a.employee_email IS NOT NULL AND a.employee_email != ''
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("year", "STRING", year),
    ])

    mismatches = []
    location_map = config.hris_location_map()
    reverse_location_map = {v: k for k, v in location_map.items()}

    for pos in positions:
        staff = hris_adapter.get_staff_by_email(pos['employee_email'])
        flags = []

        if not staff:
            flags.append({'type': 'not_in_hris', 'label': 'Not in HR', 'color': 'red'})
        else:
            # Status check
            if staff['status'] not in ('Active', 'Leave of absence'):
                flags.append({'type': 'status', 'label': 'Status Mismatch',
                              'detail': f"HR: {staff['status']}", 'color': 'orange'})
            # Title check
            if staff['job_title'] and pos['job_title']:
                if staff['job_title'].lower().strip() != pos['job_title'].lower().strip():
                    flags.append({'type': 'title', 'label': 'Title Mismatch',
                                  'detail': f"HR: {staff['job_title']}", 'color': 'blue'})
            # Location check
            school = config.school_by_id(pos['school_id'])
            if school and staff['school']:
                if staff['school'] != school['name']:
                    flags.append({'type': 'location', 'label': 'Location Mismatch',
                                  'detail': f"HR: {staff['school']}", 'color': 'purple'})
            # Subject check
            if pos.get('subject') and staff.get('_raw', {}).get(config.hris_field('subject'), ''):
                hris_subject = staff['_raw'].get(config.hris_field('subject'), '')
                if hris_subject and pos['subject'].lower().strip() != hris_subject.lower().strip():
                    flags.append({'type': 'subject', 'label': 'Subject Mismatch',
                                  'detail': f"HR: {hris_subject}", 'color': 'yellow'})

        if flags:
            mismatches.append({
                'position_id': pos['position_id'],
                'employee_name': pos['employee_name'],
                'employee_email': pos['employee_email'],
                'job_title': pos['job_title'],
                'school_id': pos['school_id'],
                'flags': flags,
            })

    return jsonify({'year': year, 'mismatches': mismatches, 'total_checked': len(positions)})


@bp.route('/api/unassigned-staff')
@auth.require_auth
def get_unassigned_staff():
    """Find HRIS employees not assigned to any position."""
    tenant_id = config.get_tenant_id()
    year = request.args.get('year', config.current_year()['year_id'])

    # Get all assigned emails
    assigned = db.query(f"""
        SELECT DISTINCT LOWER(TRIM(employee_email)) as email
        FROM `{db.table_ref('position_assignment')}`
        WHERE tenant_id = @tid AND school_year = @year
            AND employee_email IS NOT NULL AND employee_email != ''
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("year", "STRING", year),
    ])
    assigned_emails = {r['email'] for r in assigned}

    # Get all active HRIS staff
    all_staff = hris_adapter.get_all_staff(active_only=True)

    unassigned = [s for s in all_staff if s['email'] and s['email'] not in assigned_emails]

    return jsonify({'year': year, 'unassigned': unassigned, 'count': len(unassigned)})


# ============================================================
# Filter Options
# ============================================================

@bp.route('/api/positions/filter-options')
@auth.require_auth
def get_filter_options():
    """Return distinct values for all filter dropdowns."""
    return jsonify({
        'schools': [{'id': s['school_id'], 'name': s['name']} for s in config.schools()],
        'categories': [{'id': c['category_id'], 'name': c['name']} for c in config.categories()],
        'school_years': [{'id': y['year_id'], 'label': y['label']} for y in config.school_years()],
        'statuses': ['active', 'frozen', 'eliminated'],
        'assignment_statuses': ['active', 'returning', 'leaving', 'new_hire', 'open'],
        'itr_responses': ['Yes', 'No', 'Unsure', 'No response yet'],
    })


@bp.route('/api/job-options')
@auth.require_auth
def get_job_options():
    """Get job titles, subjects, grade levels from HRIS for dropdowns."""
    titles = hris_adapter.get_distinct_titles()
    return jsonify({
        'job_titles': titles,
        'categories': config.category_names(),
    })


# ============================================================
# History
# ============================================================

@bp.route('/api/positions/<position_id>/history')
@auth.require_auth
def get_history(position_id):
    """Get audit trail for a position."""
    rows = db.query(
        f"""SELECT * FROM `{db.table_ref('position_history')}`
            WHERE position_id = @pid ORDER BY changed_at DESC""",
        [bigquery.ScalarQueryParameter("pid", "STRING", position_id)]
    )
    return jsonify(rows)


# ============================================================
# Internal Helpers
# ============================================================

def _log_history(position_id, table_name, record_id, action, field, old_val, new_val, user_email):
    """Write an audit trail record."""
    db.insert_row('position_history', {
        'history_id': db.generate_id(),
        'position_id': position_id,
        'tenant_id': config.get_tenant_id(),
        'table_name': table_name,
        'record_id': record_id,
        'action': action,
        'field_changed': field,
        'old_value': old_val,
        'new_value': new_val,
        'changed_by': user_email,
        'changed_at': db.now_iso(),
    })


def _add_mismatch_flags(positions):
    """Add HRIS mismatch flags to position dicts (lightweight version).

    For the full reconciliation report, use /api/reconciliation.
    This is a faster inline check for the position list view.
    """
    for pos in positions:
        email = (pos.get('employee_email') or '').strip()
        pos['mismatch_flags'] = []
        if not email:
            continue

        staff = hris_adapter.get_staff_by_email(email)
        if not staff:
            pos['mismatch_flags'].append('not_in_hris')
        else:
            if staff['status'] not in ('Active', 'Leave of absence'):
                pos['mismatch_flags'].append('status')
            if staff['job_title'] and pos.get('job_title'):
                if staff['job_title'].lower().strip() != pos['job_title'].lower().strip():
                    pos['mismatch_flags'].append('title')
            school = config.school_by_id(pos.get('school_id', ''))
            if school and staff.get('school') and staff['school'] != school['name']:
                pos['mismatch_flags'].append('location')

    return positions
