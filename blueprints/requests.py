"""
TalentPoint — Position Requests Blueprint.

Handles position request submission, multi-stage approval workflow,
cascading position creation, and email notifications.
"""

from flask import Blueprint, jsonify, request, render_template
from google.cloud import bigquery

import db
import config
import auth
import notifications
import hris_adapter

bp = Blueprint('requests', __name__)


# Request types and which ones require CEO/Finance approval
REQUEST_TYPES = [
    'New Position', 'Open Position', 'Additional Comp (Stipend)',
    'Status Change', 'Title/Role Change', 'Salary Adjustment',
    'Temp Hire', 'Before/After School', 'Supervisor Change',
]

# Which request types create or update positions on approval
POSITION_CREATE_TYPES = ['New Position', 'Temp Hire', 'Before/After School']
POSITION_UPDATE_TYPES = ['Open Position', 'Status Change', 'Title/Role Change']


# ============================================================
# Views
# ============================================================

@bp.route('/')
def requests_home():
    """Serve the requests page — public form + admin dashboard."""
    if auth.is_authenticated():
        user = auth.get_current_user()
        permissions = auth.get_permissions(user.get('role', 'viewer'))
    else:
        user = None
        permissions = {}

    return render_template('requests.html',
                           user=user,
                           permissions=permissions,
                           org_name=config.org_name(),
                           logo_url=config.logo_url(),
                           primary_color=config.primary_color(),
                           secondary_color=config.secondary_color(),
                           schools=config.schools(),
                           categories=config.categories(),
                           request_types=REQUEST_TYPES,
                           school_years=config.school_years())


# ============================================================
# Public APIs (no auth required for submission)
# ============================================================

@bp.route('/api/submit', methods=['POST'])
def submit_request():
    """Submit a new position request. No auth required (like the original PCR form)."""
    data = request.json
    tenant_id = config.get_tenant_id()
    request_id = db.generate_id()
    now = db.now_iso()

    request_type = data.get('request_type', '')

    # Find the approval chain for this request type
    chain = db.query_one(
        f"SELECT * FROM `{db.table_ref('approval_chain')}` WHERE tenant_id = @tid AND request_type = @rtype AND active = TRUE",
        [
            bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
            bigquery.ScalarQueryParameter("rtype", "STRING", request_type),
        ]
    )

    row = {
        'request_id': request_id,
        'tenant_id': tenant_id,
        'request_type': request_type,
        'status': 'pending',
        'position_id': data.get('position_id', ''),
        'school_id': data.get('school_id', ''),
        'category_id': data.get('category_id', ''),
        'job_title': data.get('job_title', ''),
        'subject': data.get('subject', ''),
        'grade_level': data.get('grade_level', ''),
        'school_year': data.get('school_year', ''),
        'funding_source': data.get('funding_source', ''),
        'fte': data.get('fte', 1.0),
        'hours_status': data.get('hours_status', ''),
        'reports_to': data.get('reports_to', ''),
        'justification': data.get('justification', ''),
        'change_description': data.get('change_description', ''),
        'hire_type': data.get('hire_type', ''),
        'employee_email': data.get('employee_email', ''),
        'employee_name': data.get('employee_name', ''),
        'candidate_email': data.get('candidate_email', ''),
        'candidate_position_id': data.get('candidate_position_id', ''),
        'linked_position_id': data.get('linked_position_id', ''),
        'sped_reviewed': data.get('sped_reviewed', ''),
        'requested_amount': data.get('requested_amount', ''),
        'payment_dates': data.get('payment_dates', ''),
        'chain_id': chain['chain_id'] if chain else '',
        'current_step': 1,
        'final_status': 'Pending',
        'offer_sent': None,
        'offer_signed': None,
        'admin_notes': '',
        'is_archived': False,
        'requested_by': data.get('requestor_email', ''),
        'requested_at': now,
        'resolved_at': None,
        'resolved_by': None,
        'updated_at': now,
        'updated_by': data.get('requestor_email', ''),
    }
    db.insert_row('position_request', row)

    # Send confirmation email
    _send_confirmation(row)
    # Send alert to talent/hr team
    _send_new_request_alert(row)

    return jsonify({'request_id': request_id, 'message': 'Request submitted'}), 201


@bp.route('/api/lookup')
def lookup_requests():
    """Look up requests by email (public, no auth)."""
    email = request.args.get('email', '').strip().lower()
    if not email:
        return jsonify([])

    rows = db.query(
        f"""SELECT request_id, request_type, job_title, school_id, final_status, requested_at
            FROM `{db.table_ref('position_request')}`
            WHERE tenant_id = @tid AND LOWER(requested_by) = @email AND (is_archived = FALSE OR is_archived IS NULL)
            ORDER BY requested_at DESC""",
        [
            bigquery.ScalarQueryParameter("tid", "STRING", config.get_tenant_id()),
            bigquery.ScalarQueryParameter("email", "STRING", email),
        ]
    )
    return jsonify(rows)


@bp.route('/api/employee-lookup')
def employee_lookup():
    """Look up an employee from HRIS for auto-filling the form."""
    email = request.args.get('email', '').strip()
    if not email:
        return jsonify({'error': 'Email required'}), 400

    staff = hris_adapter.get_staff_by_email(email)
    if not staff:
        return jsonify({'error': 'Employee not found'}), 404

    return jsonify({
        'name': staff['full_name'],
        'email': staff['email'],
        'job_title': staff['job_title'],
        'school': staff['school'],
        'location': staff['location'],
        'supervisor': staff['supervisor'],
        'employee_id': staff['employee_id'],
    })


# ============================================================
# Admin APIs (auth required)
# ============================================================

@bp.route('/api/admin/requests')
@auth.require_auth
def get_all_requests():
    """Get all requests for admin dashboard."""
    tenant_id = config.get_tenant_id()
    status_filter = request.args.get('status')
    include_archived = request.args.get('include_archived') == 'true'

    sql = f"SELECT * FROM `{db.table_ref('position_request')}` WHERE tenant_id = @tid"
    params = [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]

    if not include_archived:
        sql += " AND (is_archived = FALSE OR is_archived IS NULL)"
    if status_filter:
        sql += " AND final_status = @status"
        params.append(bigquery.ScalarQueryParameter("status", "STRING", status_filter))

    sql += " ORDER BY requested_at DESC"
    return jsonify(db.query(sql, params))


@bp.route('/api/admin/stats')
@auth.require_auth
def get_request_stats():
    """Dashboard stats for requests."""
    tenant_id = config.get_tenant_id()
    sql = f"""
        SELECT
            COUNT(*) as total,
            COUNTIF(final_status = 'Pending') as pending,
            COUNTIF(final_status = 'Approved') as approved,
            COUNTIF(final_status = 'Denied') as denied
        FROM `{db.table_ref('position_request')}`
        WHERE tenant_id = @tid AND (is_archived = FALSE OR is_archived IS NULL)
    """
    stats = db.query_one(sql, [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)])
    return jsonify(stats or {})


@bp.route('/api/admin/requests/<request_id>', methods=['PATCH'])
@auth.require_auth
def update_request(request_id):
    """Update a request (role-enforced)."""
    data = request.json
    user_email = auth.get_current_email()
    user_role = auth.get_current_role()
    now = db.now_iso()

    current = db.query_one(
        f"SELECT * FROM `{db.table_ref('position_request')}` WHERE request_id = @rid",
        [bigquery.ScalarQueryParameter("rid", "STRING", request_id)]
    )
    if not current:
        return jsonify({'error': 'Request not found'}), 404

    updates = {}
    allowed_fields = _get_editable_fields(user_role)

    for field in allowed_fields:
        if field in data:
            updates[field] = data[field]

    # Track approval status change
    old_status = current.get('final_status', 'Pending')

    if updates:
        updates['updated_by'] = user_email
        updates['updated_at'] = now

        # If final_status changed to Approved/Denied, record resolution
        new_status = updates.get('final_status', old_status)
        if new_status != old_status and new_status in ('Approved', 'Denied'):
            updates['resolved_at'] = now
            updates['resolved_by'] = user_email

        db.update_row('position_request', 'request_id', request_id, updates)

        # Log approval action if status changed
        if updates.get('final_status') and updates['final_status'] != old_status:
            db.insert_row('approval_action', {
                'action_id': db.generate_id(),
                'request_id': request_id,
                'step_id': '',
                'action': 'approve' if updates['final_status'] == 'Approved' else 'deny',
                'comment': data.get('admin_notes', ''),
                'acted_by': user_email,
                'acted_at': now,
            })

            # Send status notification
            current.update(updates)
            _send_status_update(current)

    return jsonify({'message': 'Request updated', 'changes': len(updates)})


@bp.route('/api/admin/requests/<request_id>/create-position', methods=['POST'])
@auth.require_permission('can_create_positions')
def create_position_from_request(request_id):
    """Create or update a position from an approved request."""
    user_email = auth.get_current_email()
    now = db.now_iso()

    req = db.query_one(
        f"SELECT * FROM `{db.table_ref('position_request')}` WHERE request_id = @rid",
        [bigquery.ScalarQueryParameter("rid", "STRING", request_id)]
    )
    if not req:
        return jsonify({'error': 'Request not found'}), 404

    request_type = req.get('request_type', '')

    if request_type in POSITION_CREATE_TYPES:
        # Create new position
        position_id = db.generate_id()
        db.insert_row('position', {
            'position_id': position_id,
            'tenant_id': config.get_tenant_id(),
            'school_id': req.get('school_id', ''),
            'category_id': req.get('category_id', ''),
            'job_title': req.get('job_title', ''),
            'subject': req.get('subject', ''),
            'grade_level': req.get('grade_level', ''),
            'funding_source': req.get('funding_source', ''),
            'fte': req.get('fte', 1.0),
            'status': 'active',
            'start_year': req.get('school_year', config.current_year()['year_id']),
            'end_year': None,
            'request_id': request_id,
            'notes': f"Created from request {request_id}",
            'created_by': user_email,
            'created_at': now,
            'updated_by': user_email,
            'updated_at': now,
        })

        # Link request to position
        db.update_row('position_request', 'request_id', request_id, {'position_id': position_id})

        # Handle cascading: if internal transfer, create open position for vacated seat
        if req.get('candidate_position_id'):
            _create_cascade_request(req, user_email)

        return jsonify({'position_id': position_id, 'action': 'created'}), 201

    elif request_type in POSITION_UPDATE_TYPES:
        # Update existing position
        linked_id = req.get('linked_position_id') or req.get('position_id')
        if not linked_id:
            return jsonify({'error': 'No linked position to update'}), 400

        updates = {'updated_by': user_email, 'updated_at': now}
        if request_type == 'Open Position':
            updates['status'] = 'active'  # Position stays active, assignment changes
        elif request_type == 'Status Change':
            if req.get('hours_status'):
                updates['notes'] = f"Status changed to {req['hours_status']} via request {request_id}"
        elif request_type == 'Title/Role Change':
            if req.get('job_title'):
                updates['job_title'] = req['job_title']
            if req.get('subject'):
                updates['subject'] = req['subject']
            if req.get('grade_level'):
                updates['grade_level'] = req['grade_level']

        db.update_row('position', 'position_id', linked_id, updates)
        return jsonify({'position_id': linked_id, 'action': 'updated'})

    return jsonify({'error': f'No position action for request type: {request_type}'}), 400


@bp.route('/api/admin/requests/<request_id>/archive', methods=['PATCH'])
@auth.require_auth
def archive_request(request_id):
    db.update_row('position_request', 'request_id', request_id, {'is_archived': True})
    return jsonify({'message': 'Archived'})


@bp.route('/api/admin/requests/<request_id>/unarchive', methods=['PATCH'])
@auth.require_auth
def unarchive_request(request_id):
    db.update_row('position_request', 'request_id', request_id, {'is_archived': False})
    return jsonify({'message': 'Unarchived'})


@bp.route('/api/admin/requests/<request_id>', methods=['DELETE'])
@auth.require_permission('can_delete')
def delete_request(request_id):
    db.delete_row('position_request', 'request_id', request_id)
    return jsonify({'message': 'Deleted'})


# ============================================================
# Internal Helpers
# ============================================================

def _get_editable_fields(role):
    """Return which fields a role can edit on a request."""
    base = ['admin_notes', 'updated_at', 'updated_by']
    if role in ('super_admin', 'admin'):
        return base + ['final_status', 'offer_sent', 'offer_signed',
                       'job_title', 'school_id', 'category_id', 'hours_status']
    elif role == 'hr':
        return base + ['final_status', 'offer_sent', 'offer_signed']
    elif role in ('ceo', 'finance'):
        return base + ['final_status']
    return base


def _create_cascade_request(original_req, user_email):
    """When an internal candidate is promoted, auto-create an Open Position request
    for the seat they're vacating."""
    cascade_id = db.generate_id()
    now = db.now_iso()

    db.insert_row('position_request', {
        'request_id': cascade_id,
        'tenant_id': config.get_tenant_id(),
        'request_type': 'Open Position',
        'status': 'pending',
        'position_id': '',
        'school_id': original_req.get('school_id', ''),
        'category_id': original_req.get('category_id', ''),
        'job_title': '',  # Will be filled from the vacated position
        'subject': '',
        'grade_level': '',
        'school_year': original_req.get('school_year', ''),
        'funding_source': '',
        'fte': 1.0,
        'hours_status': '',
        'reports_to': '',
        'justification': f"Auto-generated: Vacancy created by internal transfer (request {original_req['request_id']})",
        'change_description': '',
        'hire_type': 'Post Position',
        'employee_email': original_req.get('candidate_email', ''),
        'employee_name': '',
        'candidate_email': '',
        'candidate_position_id': '',
        'linked_position_id': original_req.get('candidate_position_id', ''),
        'sped_reviewed': '',
        'requested_amount': '',
        'payment_dates': '',
        'chain_id': '',
        'current_step': 1,
        'final_status': 'Pending',
        'offer_sent': None,
        'offer_signed': None,
        'admin_notes': 'Cascaded from internal transfer',
        'is_archived': False,
        'requested_by': user_email,
        'requested_at': now,
        'resolved_at': None,
        'resolved_by': None,
        'updated_at': now,
        'updated_by': user_email,
    })


def _send_confirmation(req):
    """Send confirmation email to the requestor."""
    notifications.send_email(
        to=req['requested_by'],
        subject=f"Position Request Submitted — {req['request_id']}",
        html_body=f"""
            <h2 style="color: {config.secondary_color()};">Request Submitted</h2>
            <p>Your position request has been submitted and is pending review.</p>
            <table style="width:100%; border-collapse:collapse; margin:16px 0;">
                <tr><td style="padding:8px; font-weight:600; color:#666;">Request ID</td>
                    <td style="padding:8px;">{req['request_id']}</td></tr>
                <tr><td style="padding:8px; font-weight:600; color:#666;">Type</td>
                    <td style="padding:8px;">{req['request_type']}</td></tr>
                <tr><td style="padding:8px; font-weight:600; color:#666;">Position</td>
                    <td style="padding:8px;">{req.get('job_title', 'N/A')}</td></tr>
                <tr><td style="padding:8px; font-weight:600; color:#666;">School Year</td>
                    <td style="padding:8px;">{req.get('school_year', 'N/A')}</td></tr>
            </table>
            <p style="color:#666; font-size:14px;">You will receive an update when your request is reviewed.</p>
        """
    )


def _send_new_request_alert(req):
    """Send alert to talent/HR team about a new request."""
    # Get notification recipients from user_role table
    hr_users = [u['email'] for u in config.user_roles() if u['role'] in ('hr', 'admin', 'super_admin')]

    if hr_users:
        notifications.send_email(
            to=hr_users[0],
            cc=hr_users[1:] if len(hr_users) > 1 else None,
            subject=f"New Position Request — {req['request_type']} — {req.get('job_title', 'N/A')}",
            html_body=f"""
                <h2 style="color: {config.secondary_color()};">New Position Request</h2>
                <table style="width:100%; border-collapse:collapse; margin:16px 0;">
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Request ID</td>
                        <td style="padding:8px;">{req['request_id']}</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Type</td>
                        <td style="padding:8px;">{req['request_type']}</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Position</td>
                        <td style="padding:8px;">{req.get('job_title', 'N/A')}</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Requestor</td>
                        <td style="padding:8px;">{req['requested_by']}</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Justification</td>
                        <td style="padding:8px;">{req.get('justification', 'N/A')}</td></tr>
                </table>
                <p><a href="/" style="color: {config.primary_color()};">Review in TalentPoint</a></p>
            """
        )


def _send_status_update(req):
    """Send status update to requestor."""
    status = req.get('final_status', 'Pending')
    status_color = '#22c55e' if status == 'Approved' else '#ef4444' if status == 'Denied' else '#f59e0b'

    notifications.send_email(
        to=req['requested_by'],
        subject=f"Position Request {status} — {req['request_id']}",
        html_body=f"""
            <h2 style="color: {config.secondary_color()};">Request Update</h2>
            <p>Your position request has been updated.</p>
            <div style="display:inline-block; padding:6px 16px; border-radius:20px;
                        background-color:{status_color}; color:white; font-weight:600; margin:8px 0;">
                {status}
            </div>
            <table style="width:100%; border-collapse:collapse; margin:16px 0;">
                <tr><td style="padding:8px; font-weight:600; color:#666;">Request ID</td>
                    <td style="padding:8px;">{req['request_id']}</td></tr>
                <tr><td style="padding:8px; font-weight:600; color:#666;">Type</td>
                    <td style="padding:8px;">{req['request_type']}</td></tr>
                <tr><td style="padding:8px; font-weight:600; color:#666;">Position</td>
                    <td style="padding:8px;">{req.get('job_title', 'N/A')}</td></tr>
            </table>
        """
    )
