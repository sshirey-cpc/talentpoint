"""
TalentPoint — Onboarding Blueprint.

New hire intake form (public, no auth) + admin tracking dashboard.
Custom compliance fields configurable per tenant.
Links to hiring pipeline: when a candidate is hired, an onboarding record can be auto-created.
"""

import json
from flask import Blueprint, jsonify, request, render_template
from google.cloud import bigquery

import db
import config
import auth
import notifications

bp = Blueprint('onboarding', __name__)


# ============================================================
# Views
# ============================================================

@bp.route('/')
def onboarding_home():
    user = auth.get_current_user() if auth.is_authenticated() else None
    permissions = auth.get_permissions(user.get('role', 'viewer')) if user else {}
    custom_fields = _get_custom_fields()
    return render_template('onboarding.html',
                           user=user,
                           permissions=permissions,
                           org_name=config.org_name(),
                           logo_url=config.logo_url(),
                           primary_color=config.primary_color(),
                           secondary_color=config.secondary_color(),
                           schools=config.schools(),
                           custom_fields=custom_fields)


# ============================================================
# Public APIs
# ============================================================

@bp.route('/api/submit', methods=['POST'])
def submit_onboarding():
    """Submit onboarding intake form. No auth required."""
    data = request.json
    tenant_id = config.get_tenant_id()
    submission_id = db.generate_id()
    now = db.now_iso()

    # Extract custom field values into JSON
    custom_fields = _get_custom_fields()
    custom_values = {}
    for field in custom_fields:
        key = field['field_name']
        if key in data:
            custom_values[key] = data[key]

    row = {
        'submission_id': submission_id,
        'tenant_id': tenant_id,
        'submitted_at': now,
        'email': data.get('email', ''),
        'first_name': data.get('first_name', ''),
        'last_name': data.get('last_name', ''),
        'preferred_name': data.get('preferred_name', ''),
        'school_location': data.get('school_location', ''),
        'phone': data.get('phone', ''),
        'physical_address': data.get('physical_address', ''),
        'tshirt_size': data.get('tshirt_size', ''),
        'dietary_needs': data.get('dietary_needs', ''),
        'food_allergies': data.get('food_allergies', ''),
        'ada_accommodation': data.get('ada_accommodation', ''),
        'custom_fields': json.dumps(custom_values),
        'onboarding_status': 'Not Started',
        'start_date': None,
        'position_title': '',
        'badge_printed': 'No',
        'equipment_issued': 'No',
        'orientation_complete': 'No',
        'admin_notes': '',
        'updated_at': now,
        'updated_by': '',
        'is_archived': False,
        'candidate_id': data.get('candidate_id', ''),
    }
    db.insert_row('onboarding_submission', row)

    # Confirmation email
    notifications.send_email(
        to=row['email'],
        subject=f"Welcome to {config.org_name()}!",
        html_body=f"""
            <h2 style="color: {config.secondary_color()};">Welcome, {row['first_name']}!</h2>
            <p>Thank you for completing your onboarding intake form. Our team will be in touch
            with next steps before your start date.</p>
            <p style="color:#666; font-size:14px;">Submission ID: {submission_id}</p>
        """
    )

    # Alert HR
    hr_users = [u['email'] for u in config.user_roles() if u['role'] in ('hr', 'admin', 'super_admin')]
    if hr_users:
        notifications.send_email(
            to=hr_users[0],
            cc=hr_users[1:] if len(hr_users) > 1 else None,
            subject=f"New Onboarding Submission — {row['first_name']} {row['last_name']}",
            html_body=f"""
                <h2 style="color: {config.secondary_color()};">New Hire Onboarding Submitted</h2>
                <table style="width:100%; border-collapse:collapse; margin:16px 0;">
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Name</td>
                        <td style="padding:8px;">{row['first_name']} {row['last_name']}</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Email</td>
                        <td style="padding:8px;">{row['email']}</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">School</td>
                        <td style="padding:8px;">{row['school_location']}</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">ADA Accommodation</td>
                        <td style="padding:8px;">{row['ada_accommodation'] or 'None'}</td></tr>
                </table>
            """
        )

    return jsonify({'submission_id': submission_id}), 201


@bp.route('/api/lookup')
def lookup_submission():
    """Look up submission by email. No auth required."""
    email = request.args.get('email', '').strip().lower()
    if not email:
        return jsonify([])

    rows = db.query(f"""
        SELECT submission_id, first_name, last_name, school_location,
               onboarding_status, submitted_at, start_date, position_title
        FROM `{db.table_ref('onboarding_submission')}`
        WHERE tenant_id = @tid AND LOWER(email) = @email
            AND (is_archived = FALSE OR is_archived IS NULL)
        ORDER BY submitted_at DESC
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", config.get_tenant_id()),
        bigquery.ScalarQueryParameter("email", "STRING", email),
    ])
    return jsonify(rows)


# ============================================================
# Admin APIs
# ============================================================

@bp.route('/api/admin/submissions')
@auth.require_auth
def get_all_submissions():
    tenant_id = config.get_tenant_id()
    include_archived = request.args.get('include_archived') == 'true'

    sql = f"SELECT * FROM `{db.table_ref('onboarding_submission')}` WHERE tenant_id = @tid"
    params = [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]
    if not include_archived:
        sql += " AND (is_archived = FALSE OR is_archived IS NULL)"
    sql += " ORDER BY submitted_at DESC"

    return jsonify(db.query(sql, params))


@bp.route('/api/admin/stats')
@auth.require_auth
def get_onboarding_stats():
    tenant_id = config.get_tenant_id()
    stats = db.query_one(f"""
        SELECT
            COUNT(*) as total,
            COUNTIF(onboarding_status = 'Not Started') as not_started,
            COUNTIF(onboarding_status = 'In Progress') as in_progress,
            COUNTIF(onboarding_status = 'Complete') as complete,
            COUNTIF(ada_accommodation IS NOT NULL AND ada_accommodation != '' AND ada_accommodation != 'None') as needs_accommodation
        FROM `{db.table_ref('onboarding_submission')}`
        WHERE tenant_id = @tid AND (is_archived = FALSE OR is_archived IS NULL)
    """, [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)])
    return jsonify(stats or {})


@bp.route('/api/admin/submissions/<submission_id>', methods=['PATCH'])
@auth.require_auth
def update_submission(submission_id):
    """Update onboarding tracking fields."""
    data = request.json
    user_email = auth.get_current_email()
    now = db.now_iso()

    editable = ['onboarding_status', 'start_date', 'position_title',
                'badge_printed', 'equipment_issued', 'orientation_complete', 'admin_notes']

    updates = {k: v for k, v in data.items() if k in editable}
    if updates:
        updates['updated_by'] = user_email
        updates['updated_at'] = now
        db.update_row('onboarding_submission', 'submission_id', submission_id, updates)

    return jsonify({'message': 'Submission updated'})


@bp.route('/api/admin/submissions/<submission_id>/archive', methods=['PATCH'])
@auth.require_auth
def archive_submission(submission_id):
    db.update_row('onboarding_submission', 'submission_id', submission_id, {'is_archived': True})
    return jsonify({'message': 'Archived'})


@bp.route('/api/admin/submissions/<submission_id>', methods=['DELETE'])
@auth.require_permission('can_delete')
def delete_submission(submission_id):
    db.delete_row('onboarding_submission', 'submission_id', submission_id)
    return jsonify({'message': 'Deleted'})


# ============================================================
# Auto-create from pipeline hire
# ============================================================

@bp.route('/api/auto-create', methods=['POST'])
@auth.require_auth
def auto_create_from_hire():
    """Create an onboarding record when a candidate is hired through the pipeline."""
    data = request.json
    tenant_id = config.get_tenant_id()
    submission_id = db.generate_id()
    now = db.now_iso()

    row = {
        'submission_id': submission_id,
        'tenant_id': tenant_id,
        'submitted_at': now,
        'email': data.get('email', ''),
        'first_name': data.get('first_name', ''),
        'last_name': data.get('last_name', ''),
        'preferred_name': '',
        'school_location': data.get('school', ''),
        'phone': data.get('phone', ''),
        'physical_address': '',
        'tshirt_size': '',
        'dietary_needs': '',
        'food_allergies': '',
        'ada_accommodation': '',
        'custom_fields': '{}',
        'onboarding_status': 'Not Started',
        'start_date': data.get('start_date'),
        'position_title': data.get('position_title', ''),
        'badge_printed': 'No',
        'equipment_issued': 'No',
        'orientation_complete': 'No',
        'admin_notes': f"Auto-created from pipeline hire (candidate {data.get('candidate_id', '')})",
        'updated_at': now,
        'updated_by': auth.get_current_email(),
        'is_archived': False,
        'candidate_id': data.get('candidate_id', ''),
    }
    db.insert_row('onboarding_submission', row)

    return jsonify({'submission_id': submission_id}), 201


# ============================================================
# Custom Fields Config
# ============================================================

@bp.route('/api/custom-fields')
def get_fields():
    """Return tenant-specific custom fields for the intake form."""
    return jsonify(_get_custom_fields())


def _get_custom_fields():
    """Get custom onboarding fields from config."""
    return db.query(
        f"SELECT * FROM `{db.table_ref('onboarding_config')}` WHERE tenant_id = @tid AND active = TRUE ORDER BY sort_order",
        [bigquery.ScalarQueryParameter("tid", "STRING", config.get_tenant_id())]
    )
