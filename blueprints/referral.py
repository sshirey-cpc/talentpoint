"""
TalentPoint — Staff Referral Program Blueprint.

Zero-auth submission for staff, admin dashboard for HR/Talent,
60-day payout tracking, weekly rollup emails, configurable bonus tiers.
"""

from datetime import datetime, timedelta, timezone
from flask import Blueprint, jsonify, request, render_template
from google.cloud import bigquery

import db
import config
import auth
import notifications

bp = Blueprint('referral', __name__)

REFERRAL_STATUSES = [
    'Submitted', 'Under Review', 'Candidate Applied', 'Interviewing',
    'Hired', 'Eligible', 'Paid',
    'Not Hired', 'Withdrawn/Non-responsive', 'Candidate Left', 'Ineligible',
]


# ============================================================
# Views
# ============================================================

@bp.route('/')
def referral_home():
    user = auth.get_current_user() if auth.is_authenticated() else None
    permissions = auth.get_permissions(user.get('role', 'viewer')) if user else {}
    bonus_tiers = _get_bonus_tiers()
    return render_template('referral.html',
                           user=user,
                           permissions=permissions,
                           org_name=config.org_name(),
                           logo_url=config.logo_url(),
                           primary_color=config.primary_color(),
                           secondary_color=config.secondary_color(),
                           schools=config.schools(),
                           bonus_tiers=bonus_tiers,
                           statuses=REFERRAL_STATUSES)


# ============================================================
# Public APIs (no auth — staff submit and check status by email)
# ============================================================

@bp.route('/api/submit', methods=['POST'])
def submit_referral():
    """Submit a new referral. No auth required."""
    data = request.json
    tenant_id = config.get_tenant_id()
    referral_id = db.generate_id()
    now = db.now_iso()

    # Look up bonus amount from config
    position_type = data.get('position_type', 'Other')
    bonus = _get_bonus_amount(position_type)

    row = {
        'referral_id': referral_id,
        'tenant_id': tenant_id,
        'submitted_at': now,
        'referrer_name': data.get('referrer_name', ''),
        'referrer_email': data.get('referrer_email', ''),
        'referrer_school': data.get('referrer_school', ''),
        'candidate_name': data.get('candidate_name', ''),
        'candidate_email': data.get('candidate_email', ''),
        'candidate_phone': data.get('candidate_phone', ''),
        'position': data.get('position', ''),
        'position_type': position_type,
        'bonus_amount': bonus,
        'relationship': data.get('relationship', ''),
        'already_applied': data.get('already_applied', ''),
        'notes': data.get('notes', ''),
        'status': 'Submitted',
        'status_updated_at': now,
        'status_updated_by': '',
        'hire_date': None,
        'retention_date': None,
        'payout_month': None,
        'paid_date': None,
        'admin_notes': '',
        'is_archived': False,
    }
    db.insert_row('referral', row)

    # Send confirmation to referrer
    notifications.send_email(
        to=row['referrer_email'],
        subject=f"Referral Submitted — {referral_id}",
        html_body=f"""
            <h2 style="color: {config.secondary_color()};">Thank You for Your Referral!</h2>
            <p>Your referral has been submitted and is under review.</p>
            <table style="width:100%; border-collapse:collapse; margin:16px 0;">
                <tr><td style="padding:8px; font-weight:600; color:#666;">Referral ID</td>
                    <td style="padding:8px;">{referral_id}</td></tr>
                <tr><td style="padding:8px; font-weight:600; color:#666;">Candidate</td>
                    <td style="padding:8px;">{row['candidate_name']}</td></tr>
                <tr><td style="padding:8px; font-weight:600; color:#666;">Position</td>
                    <td style="padding:8px;">{row['position']}</td></tr>
                <tr><td style="padding:8px; font-weight:600; color:#666;">Potential Bonus</td>
                    <td style="padding:8px;">${bonus}</td></tr>
            </table>
        """
    )

    # Alert HR/Talent
    hr_users = [u['email'] for u in config.user_roles() if u['role'] in ('hr', 'admin', 'super_admin')]
    if hr_users:
        notifications.send_email(
            to=hr_users[0],
            cc=hr_users[1:] if len(hr_users) > 1 else None,
            subject=f"New Staff Referral — {row['candidate_name']} for {row['position']}",
            html_body=f"""
                <h2 style="color: {config.secondary_color()};">New Referral Submitted</h2>
                <table style="width:100%; border-collapse:collapse; margin:16px 0;">
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Referrer</td>
                        <td style="padding:8px;">{row['referrer_name']} ({row['referrer_email']})</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Candidate</td>
                        <td style="padding:8px;">{row['candidate_name']} ({row['candidate_email']})</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Position</td>
                        <td style="padding:8px;">{row['position']} ({row['position_type']})</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Bonus</td>
                        <td style="padding:8px;">${bonus}</td></tr>
                </table>
            """
        )

    return jsonify({'referral_id': referral_id, 'bonus_amount': bonus}), 201


@bp.route('/api/lookup')
def lookup_referrals():
    """Look up referrals by referrer email. No auth required."""
    email = request.args.get('email', '').strip().lower()
    if not email:
        return jsonify([])

    rows = db.query(f"""
        SELECT referral_id, candidate_name, position, position_type, bonus_amount,
               status, submitted_at, hire_date, paid_date
        FROM `{db.table_ref('referral')}`
        WHERE tenant_id = @tid AND LOWER(referrer_email) = @email
            AND (is_archived = FALSE OR is_archived IS NULL)
        ORDER BY submitted_at DESC
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", config.get_tenant_id()),
        bigquery.ScalarQueryParameter("email", "STRING", email),
    ])

    # Summary stats
    total_bonus = sum(r['bonus_amount'] for r in rows if r['status'] == 'Paid')
    pending_bonus = sum(r['bonus_amount'] for r in rows if r['status'] in ('Hired', 'Eligible'))

    return jsonify({
        'referrals': rows,
        'total_paid': total_bonus,
        'pending_bonus': pending_bonus,
        'total_referrals': len(rows),
    })


@bp.route('/api/staff-lookup')
def staff_lookup():
    """Auto-fill referrer name from previous submissions."""
    email = request.args.get('email', '').strip().lower()
    if not email:
        return jsonify({})

    row = db.query_one(f"""
        SELECT referrer_name, referrer_school
        FROM `{db.table_ref('referral')}`
        WHERE tenant_id = @tid AND LOWER(referrer_email) = @email
        ORDER BY submitted_at DESC LIMIT 1
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", config.get_tenant_id()),
        bigquery.ScalarQueryParameter("email", "STRING", email),
    ])
    return jsonify(row or {})


# ============================================================
# Admin APIs
# ============================================================

@bp.route('/api/admin/referrals')
@auth.require_auth
def get_all_referrals():
    tenant_id = config.get_tenant_id()
    include_archived = request.args.get('include_archived') == 'true'

    sql = f"SELECT * FROM `{db.table_ref('referral')}` WHERE tenant_id = @tid"
    params = [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]
    if not include_archived:
        sql += " AND (is_archived = FALSE OR is_archived IS NULL)"
    sql += " ORDER BY submitted_at DESC"

    return jsonify(db.query(sql, params))


@bp.route('/api/admin/stats')
@auth.require_auth
def get_referral_stats():
    tenant_id = config.get_tenant_id()
    stats = db.query_one(f"""
        SELECT
            COUNT(*) as total,
            COUNTIF(status = 'Submitted' OR status = 'Under Review') as pending_review,
            COUNTIF(status IN ('Candidate Applied', 'Interviewing')) as in_progress,
            COUNTIF(status = 'Paid') as bonuses_paid,
            COUNTIF(status IN ('Hired', 'Eligible')) as bonuses_pending,
            SUM(CASE WHEN status = 'Paid' THEN bonus_amount ELSE 0 END) as total_paid_amount,
            SUM(CASE WHEN status IN ('Hired', 'Eligible') THEN bonus_amount ELSE 0 END) as total_pending_amount
        FROM `{db.table_ref('referral')}`
        WHERE tenant_id = @tid AND (is_archived = FALSE OR is_archived IS NULL)
    """, [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)])
    return jsonify(stats or {})


@bp.route('/api/admin/referrals/<referral_id>', methods=['PATCH'])
@auth.require_auth
def update_referral(referral_id):
    """Update a referral — handles status changes, hire dates, and payout tracking."""
    data = request.json
    user_email = auth.get_current_email()
    now = db.now_iso()

    current = db.query_one(
        f"SELECT * FROM `{db.table_ref('referral')}` WHERE referral_id = @rid",
        [bigquery.ScalarQueryParameter("rid", "STRING", referral_id)]
    )
    if not current:
        return jsonify({'error': 'Referral not found'}), 404

    updates = {}
    editable = ['status', 'bonus_amount', 'position', 'position_type',
                'hire_date', 'paid_date', 'admin_notes']

    old_status = current.get('status', '')

    for field in editable:
        if field in data:
            updates[field] = data[field]

    # Auto-calculate retention date and payout month when hire_date is set
    if 'hire_date' in updates and updates['hire_date']:
        retention_days = _get_retention_days(current.get('position_type', 'Other'))
        hire_dt = datetime.strptime(updates['hire_date'], '%Y-%m-%d')
        retention_dt = hire_dt + timedelta(days=retention_days)
        updates['retention_date'] = retention_dt.strftime('%Y-%m-%d')
        # Payout month is the month after the retention date
        payout_dt = retention_dt.replace(day=1) + timedelta(days=32)
        updates['payout_month'] = payout_dt.strftime('%B %Y')

    if updates:
        updates['status_updated_at'] = now
        updates['status_updated_by'] = user_email
        db.update_row('referral', 'referral_id', referral_id, updates)

    # Send notifications on status change
    new_status = updates.get('status', old_status)
    if new_status != old_status:
        current.update(updates)
        _send_status_notification(current)

        # Special notification when eligible
        if new_status == 'Eligible':
            _send_eligible_alert(current)

    return jsonify({'message': 'Referral updated'})


@bp.route('/api/admin/referrals/<referral_id>/archive', methods=['PATCH'])
@auth.require_auth
def archive_referral(referral_id):
    db.update_row('referral', 'referral_id', referral_id, {'is_archived': True})
    return jsonify({'message': 'Archived'})


@bp.route('/api/admin/referrals/<referral_id>', methods=['DELETE'])
@auth.require_permission('can_delete')
def delete_referral(referral_id):
    db.delete_row('referral', 'referral_id', referral_id)
    return jsonify({'message': 'Deleted'})


# ============================================================
# Weekly Rollup
# ============================================================

@bp.route('/api/admin/weekly-rollup', methods=['POST'])
@auth.require_auth
def send_weekly_rollup():
    """Send the weekly referral summary to HR/Talent team."""
    tenant_id = config.get_tenant_id()
    now = datetime.now(timezone.utc)
    week_ago = (now - timedelta(days=7)).isoformat()

    # New this week
    new_this_week = db.query(f"""
        SELECT referrer_name, candidate_name, position FROM `{db.table_ref('referral')}`
        WHERE tenant_id = @tid AND submitted_at >= @since AND (is_archived = FALSE OR is_archived IS NULL)
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("since", "STRING", week_ago),
    ])

    # Needs review
    needs_review = db.query(f"""
        SELECT referral_id, candidate_name, position FROM `{db.table_ref('referral')}`
        WHERE tenant_id = @tid AND status IN ('Submitted', 'Under Review')
            AND (is_archived = FALSE OR is_archived IS NULL)
    """, [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)])

    # Upcoming retention completions (within 14 days)
    two_weeks = (now + timedelta(days=14)).strftime('%Y-%m-%d')
    today_str = now.strftime('%Y-%m-%d')
    upcoming = db.query(f"""
        SELECT referrer_name, candidate_name, retention_date, bonus_amount FROM `{db.table_ref('referral')}`
        WHERE tenant_id = @tid AND status = 'Hired'
            AND retention_date IS NOT NULL AND retention_date <= @cutoff AND retention_date >= @today
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("cutoff", "STRING", two_weeks),
        bigquery.ScalarQueryParameter("today", "STRING", today_str),
    ])

    # Ready for payout
    ready = db.query(f"""
        SELECT referrer_name, candidate_name, bonus_amount, payout_month FROM `{db.table_ref('referral')}`
        WHERE tenant_id = @tid AND status = 'Eligible' AND (is_archived = FALSE OR is_archived IS NULL)
    """, [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)])

    # Build email
    sections = []
    if new_this_week:
        items = ''.join(f"<li>{r['referrer_name']} referred {r['candidate_name']} for {r['position']}</li>" for r in new_this_week)
        sections.append(f"<h3>New This Week ({len(new_this_week)})</h3><ul>{items}</ul>")
    if needs_review:
        items = ''.join(f"<li>{r['candidate_name']} — {r['position']}</li>" for r in needs_review)
        sections.append(f"<h3>Needs Review ({len(needs_review)})</h3><ul>{items}</ul>")
    if upcoming:
        items = ''.join(f"<li>{r['candidate_name']} (referred by {r['referrer_name']}) — completes {r['retention_date']}, ${r['bonus_amount']}</li>" for r in upcoming)
        sections.append(f"<h3>Upcoming Retention Completions ({len(upcoming)})</h3><ul>{items}</ul>")
    if ready:
        items = ''.join(f"<li>{r['candidate_name']} (referred by {r['referrer_name']}) — ${r['bonus_amount']} in {r['payout_month']}</li>" for r in ready)
        sections.append(f"<h3>Ready for Payout ({len(ready)})</h3><ul>{items}</ul>")

    body = f"""
        <h2 style="color: {config.secondary_color()};">Weekly Referral Summary</h2>
        <p>Week of {now.strftime('%B %d, %Y')}</p>
        {''.join(sections) if sections else '<p>No action items this week.</p>'}
    """

    hr_users = [u['email'] for u in config.user_roles() if u['role'] in ('hr', 'admin', 'super_admin')]
    if hr_users:
        notifications.send_email(to=hr_users, subject=f"Weekly Referral Summary — {now.strftime('%B %d')}", html_body=body)

    return jsonify({'message': 'Weekly rollup sent', 'recipients': len(hr_users)})


# ============================================================
# Internal Helpers
# ============================================================

def _get_bonus_tiers():
    """Get all bonus tiers from config."""
    return db.query(
        f"SELECT * FROM `{db.table_ref('referral_config')}` WHERE tenant_id = @tid AND active = TRUE",
        [bigquery.ScalarQueryParameter("tid", "STRING", config.get_tenant_id())]
    )


def _get_bonus_amount(position_type):
    """Look up bonus amount for a position type."""
    tiers = _get_bonus_tiers()
    for t in tiers:
        if t['position_type'] == position_type:
            return t['bonus_amount']
    # Default to lowest tier
    return min((t['bonus_amount'] for t in tiers), default=300)


def _get_retention_days(position_type):
    """Look up retention period for a position type."""
    tiers = _get_bonus_tiers()
    for t in tiers:
        if t['position_type'] == position_type:
            return t.get('retention_days', 60)
    return 60


def _send_status_notification(referral):
    """Notify referrer of status change."""
    status = referral.get('status', '')
    status_color = '#22c55e' if status in ('Hired', 'Eligible', 'Paid') else '#ef4444' if status in ('Not Hired', 'Ineligible') else '#f59e0b'

    notifications.send_email(
        to=referral['referrer_email'],
        subject=f"Referral Update — {referral['candidate_name']}",
        html_body=f"""
            <h2 style="color: {config.secondary_color()};">Referral Status Update</h2>
            <div style="display:inline-block; padding:6px 16px; border-radius:20px;
                        background-color:{status_color}; color:white; font-weight:600; margin:8px 0;">
                {status}
            </div>
            <table style="width:100%; border-collapse:collapse; margin:16px 0;">
                <tr><td style="padding:8px; font-weight:600; color:#666;">Candidate</td>
                    <td style="padding:8px;">{referral['candidate_name']}</td></tr>
                <tr><td style="padding:8px; font-weight:600; color:#666;">Position</td>
                    <td style="padding:8px;">{referral['position']}</td></tr>
                <tr><td style="padding:8px; font-weight:600; color:#666;">Bonus</td>
                    <td style="padding:8px;">${referral['bonus_amount']}</td></tr>
            </table>
        """
    )


def _send_eligible_alert(referral):
    """Notify HR + Payroll when a referral bonus becomes eligible."""
    hr_users = [u['email'] for u in config.user_roles() if u['role'] in ('hr', 'admin', 'super_admin')]
    if hr_users:
        notifications.send_email(
            to=hr_users,
            subject=f"Referral Bonus Eligible — {referral['referrer_name']} (${referral['bonus_amount']})",
            html_body=f"""
                <h2 style="color: {config.secondary_color()};">Referral Bonus Ready for Payout</h2>
                <table style="width:100%; border-collapse:collapse; margin:16px 0;">
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Referrer</td>
                        <td style="padding:8px;">{referral['referrer_name']} ({referral['referrer_email']})</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Candidate</td>
                        <td style="padding:8px;">{referral['candidate_name']}</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Amount</td>
                        <td style="padding:8px;">${referral['bonus_amount']}</td></tr>
                    <tr><td style="padding:8px; font-weight:600; color:#666;">Payout Month</td>
                        <td style="padding:8px;">{referral.get('payout_month', 'TBD')}</td></tr>
                </table>
                <p style="color:#666;">Please process this bonus in the next payroll cycle.</p>
            """
        )
