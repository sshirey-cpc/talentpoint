"""
TalentPoint — Hiring Pipeline Blueprint.

Tracks candidates from sourcing through hire. When a candidate is hired,
creates a position_assignment linking them to the position.
"""

from flask import Blueprint, jsonify, request, render_template
from google.cloud import bigquery

import db
import config
import auth

bp = Blueprint('pipeline', __name__)


# ============================================================
# Views
# ============================================================

@bp.route('/')
@auth.require_auth
def pipeline_home():
    user = auth.get_current_user()
    permissions = auth.get_permissions(user.get('role', 'viewer'))
    return render_template('pipeline.html',
                           user=user,
                           permissions=permissions,
                           org_name=config.org_name(),
                           logo_url=config.logo_url(),
                           primary_color=config.primary_color(),
                           secondary_color=config.secondary_color(),
                           schools=config.schools(),
                           categories=config.categories(),
                           current_year=config.current_year(),
                           planning_year=config.planning_year())


# ============================================================
# Pipeline Stages (config)
# ============================================================

@bp.route('/api/stages')
@auth.require_auth
def get_stages():
    """Get configured pipeline stages."""
    tenant_id = config.get_tenant_id()
    stages = db.query(
        f"SELECT * FROM `{db.table_ref('pipeline_stage')}` WHERE tenant_id = @tid ORDER BY stage_order",
        [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]
    )
    return jsonify(stages)


# ============================================================
# Pipeline Overview
# ============================================================

@bp.route('/api/overview')
@auth.require_auth
def pipeline_overview():
    """Pipeline overview: positions with their candidates grouped by stage."""
    tenant_id = config.get_tenant_id()
    year = request.args.get('year')
    if not year:
        py = config.planning_year()
        year = py['year_id'] if py else config.current_year()['year_id']

    t_pos = db.table_ref('position')
    t_assign = db.table_ref('position_assignment')
    t_cand = db.table_ref('candidate')
    t_stage = db.table_ref('pipeline_stage')

    # Get open positions (no filled assignment for the target year)
    positions = db.query(f"""
        SELECT p.position_id, p.school_id, p.category_id, p.job_title, p.subject
        FROM `{t_pos}` p
        LEFT JOIN `{t_assign}` a
            ON p.position_id = a.position_id AND a.school_year = @year
            AND a.assignment_status IN ('active', 'new_hire')
        WHERE p.tenant_id = @tid AND p.status = 'active'
            AND p.start_year <= @year AND (p.end_year >= @year OR p.end_year IS NULL)
            AND a.assignment_id IS NULL
        ORDER BY p.school_id, p.job_title
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("year", "STRING", year),
    ])

    # Get all active candidates
    candidates = db.query(f"""
        SELECT c.*, s.name as stage_name, s.stage_order, s.color as stage_color, s.stage_type
        FROM `{t_cand}` c
        JOIN `{t_stage}` s ON c.current_stage_id = s.stage_id
        WHERE c.tenant_id = @tid AND c.status = 'active'
        ORDER BY s.stage_order
    """, [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)])

    # Group candidates by position
    cand_by_pos = {}
    for c in candidates:
        pid = c['position_id']
        if pid not in cand_by_pos:
            cand_by_pos[pid] = []
        cand_by_pos[pid].append(c)

    # Enrich positions
    for p in positions:
        s = config.school_by_id(p['school_id'])
        p['school_name'] = s['name'] if s else p['school_id']
        p['candidates'] = cand_by_pos.get(p['position_id'], [])
        p['candidate_count'] = len(p['candidates'])

    # Stage summary
    stages = db.query(
        f"SELECT * FROM `{db.table_ref('pipeline_stage')}` WHERE tenant_id = @tid ORDER BY stage_order",
        [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]
    )
    stage_counts = {}
    for c in candidates:
        stage = c.get('stage_name', 'Unknown')
        stage_counts[stage] = stage_counts.get(stage, 0) + 1

    return jsonify({
        'year': year,
        'open_positions': positions,
        'total_open': len(positions),
        'total_candidates': len(candidates),
        'stages': stages,
        'stage_counts': stage_counts,
    })


@bp.route('/api/stats')
@auth.require_auth
def pipeline_stats():
    """Aggregate pipeline statistics."""
    tenant_id = config.get_tenant_id()
    year = request.args.get('year')
    if not year:
        py = config.planning_year()
        year = py['year_id'] if py else config.current_year()['year_id']

    t_pos = db.table_ref('position')
    t_assign = db.table_ref('position_assignment')
    t_cand = db.table_ref('candidate')

    # Total positions needing hires vs filled
    stats = db.query_one(f"""
        SELECT
            COUNT(DISTINCT p.position_id) as total_positions,
            COUNTIF(a.assignment_status IN ('active', 'new_hire')) as filled,
            COUNTIF(a.assignment_id IS NULL) as open_positions
        FROM `{t_pos}` p
        LEFT JOIN `{t_assign}` a ON p.position_id = a.position_id AND a.school_year = @year
        WHERE p.tenant_id = @tid AND p.status = 'active'
            AND p.start_year <= @year AND (p.end_year >= @year OR p.end_year IS NULL)
    """, [
        bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
        bigquery.ScalarQueryParameter("year", "STRING", year),
    ])

    # Active candidates
    cand_stats = db.query_one(f"""
        SELECT
            COUNT(*) as total_candidates,
            COUNTIF(status = 'active') as active_candidates,
            COUNTIF(status = 'hired') as hired
        FROM `{t_cand}` WHERE tenant_id = @tid
    """, [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)])

    # Hiring targets
    targets = db.query(
        f"SELECT * FROM `{db.table_ref('hiring_target')}` WHERE tenant_id = @tid AND school_year = @year ORDER BY month",
        [
            bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
            bigquery.ScalarQueryParameter("year", "STRING", year),
        ]
    )

    result = {**(stats or {}), **(cand_stats or {}), 'hiring_targets': targets, 'year': year}

    # Calculate hiring progress percentage
    if result.get('total_positions') and result.get('open_positions') is not None:
        total_to_hire = result['open_positions'] + result.get('hired', 0)
        if total_to_hire > 0:
            result['hire_pct'] = round(result.get('hired', 0) / total_to_hire * 100, 1)
        else:
            result['hire_pct'] = 100.0

    return jsonify(result)


# ============================================================
# Candidate CRUD
# ============================================================

@bp.route('/api/candidates', methods=['POST'])
@auth.require_permission('can_edit')
def create_candidate():
    """Add a candidate to a position."""
    data = request.json
    tenant_id = config.get_tenant_id()
    user_email = auth.get_current_email()
    candidate_id = db.generate_id()
    now = db.now_iso()

    # Get first active stage
    first_stage = db.query_one(
        f"""SELECT stage_id FROM `{db.table_ref('pipeline_stage')}`
            WHERE tenant_id = @tid AND is_terminal = FALSE
            ORDER BY stage_order LIMIT 1""",
        [bigquery.ScalarQueryParameter("tid", "STRING", tenant_id)]
    )

    row = {
        'candidate_id': candidate_id,
        'tenant_id': tenant_id,
        'position_id': data['position_id'],
        'first_name': data.get('first_name', ''),
        'last_name': data.get('last_name', ''),
        'email': data.get('email', ''),
        'phone': data.get('phone', ''),
        'source': data.get('source', 'direct'),
        'current_stage_id': data.get('stage_id', first_stage['stage_id'] if first_stage else ''),
        'status': 'active',
        'notes': data.get('notes', ''),
        'created_by': user_email,
        'created_at': now,
        'updated_by': user_email,
        'updated_at': now,
    }
    db.insert_row('candidate', row)

    # Log initial stage
    db.insert_row('candidate_stage_history', {
        'history_id': db.generate_id(),
        'candidate_id': candidate_id,
        'from_stage_id': '',
        'to_stage_id': row['current_stage_id'],
        'notes': 'Candidate added to pipeline',
        'moved_by': user_email,
        'moved_at': now,
    })

    return jsonify({'candidate_id': candidate_id}), 201


@bp.route('/api/candidates/<candidate_id>')
@auth.require_auth
def get_candidate(candidate_id):
    """Get candidate details with stage history."""
    cand = db.query_one(
        f"SELECT * FROM `{db.table_ref('candidate')}` WHERE candidate_id = @cid",
        [bigquery.ScalarQueryParameter("cid", "STRING", candidate_id)]
    )
    if not cand:
        return jsonify({'error': 'Candidate not found'}), 404

    history = db.query(
        f"""SELECT h.*, s1.name as from_stage_name, s2.name as to_stage_name
            FROM `{db.table_ref('candidate_stage_history')}` h
            LEFT JOIN `{db.table_ref('pipeline_stage')}` s1 ON h.from_stage_id = s1.stage_id
            LEFT JOIN `{db.table_ref('pipeline_stage')}` s2 ON h.to_stage_id = s2.stage_id
            WHERE h.candidate_id = @cid ORDER BY h.moved_at""",
        [bigquery.ScalarQueryParameter("cid", "STRING", candidate_id)]
    )
    cand['stage_history'] = history
    return jsonify(cand)


@bp.route('/api/candidates/<candidate_id>/stage', methods=['PUT'])
@auth.require_permission('can_edit')
def advance_stage(candidate_id):
    """Move a candidate to a new pipeline stage."""
    data = request.json
    user_email = auth.get_current_email()
    now = db.now_iso()
    new_stage_id = data['stage_id']

    cand = db.query_one(
        f"SELECT * FROM `{db.table_ref('candidate')}` WHERE candidate_id = @cid",
        [bigquery.ScalarQueryParameter("cid", "STRING", candidate_id)]
    )
    if not cand:
        return jsonify({'error': 'Candidate not found'}), 404

    old_stage_id = cand['current_stage_id']

    # Check if new stage is terminal
    stage = db.query_one(
        f"SELECT * FROM `{db.table_ref('pipeline_stage')}` WHERE stage_id = @sid",
        [bigquery.ScalarQueryParameter("sid", "STRING", new_stage_id)]
    )

    updates = {
        'current_stage_id': new_stage_id,
        'updated_by': user_email,
        'updated_at': now,
    }
    if stage and stage.get('is_terminal'):
        updates['status'] = stage['stage_type']  # hired, rejected, withdrawn

    db.update_row('candidate', 'candidate_id', candidate_id, updates)

    # Log stage change
    db.insert_row('candidate_stage_history', {
        'history_id': db.generate_id(),
        'candidate_id': candidate_id,
        'from_stage_id': old_stage_id,
        'to_stage_id': new_stage_id,
        'notes': data.get('notes', ''),
        'moved_by': user_email,
        'moved_at': now,
    })

    return jsonify({'message': 'Stage updated', 'new_stage': stage['name'] if stage else new_stage_id})


@bp.route('/api/candidates/<candidate_id>/hire', methods=['POST'])
@auth.require_permission('can_create_positions')
def hire_candidate(candidate_id):
    """Mark candidate as hired and create a position assignment."""
    data = request.json
    user_email = auth.get_current_email()
    now = db.now_iso()

    cand = db.query_one(
        f"SELECT * FROM `{db.table_ref('candidate')}` WHERE candidate_id = @cid",
        [bigquery.ScalarQueryParameter("cid", "STRING", candidate_id)]
    )
    if not cand:
        return jsonify({'error': 'Candidate not found'}), 404

    # Find the "hired" stage
    hired_stage = db.query_one(
        f"SELECT stage_id FROM `{db.table_ref('pipeline_stage')}` WHERE tenant_id = @tid AND stage_type = 'hired'",
        [bigquery.ScalarQueryParameter("tid", "STRING", config.get_tenant_id())]
    )

    # Update candidate status
    old_stage = cand['current_stage_id']
    db.update_row('candidate', 'candidate_id', candidate_id, {
        'status': 'hired',
        'current_stage_id': hired_stage['stage_id'] if hired_stage else cand['current_stage_id'],
        'updated_by': user_email,
        'updated_at': now,
    })

    # Log stage change
    db.insert_row('candidate_stage_history', {
        'history_id': db.generate_id(),
        'candidate_id': candidate_id,
        'from_stage_id': old_stage,
        'to_stage_id': hired_stage['stage_id'] if hired_stage else '',
        'notes': 'Candidate hired',
        'moved_by': user_email,
        'moved_at': now,
    })

    # Create position assignment
    school_year = data.get('school_year')
    if not school_year:
        py = config.planning_year()
        school_year = py['year_id'] if py else config.current_year()['year_id']

    assignment_id = db.generate_id()
    db.insert_row('position_assignment', {
        'assignment_id': assignment_id,
        'position_id': cand['position_id'],
        'tenant_id': config.get_tenant_id(),
        'school_year': school_year,
        'employee_name': f"{cand['first_name']} {cand['last_name']}".strip(),
        'employee_email': cand.get('email', ''),
        'employee_number': '',
        'assignment_status': 'new_hire',
        'itr_response': 'New Hire',
        'candidate_name': '',
        'start_date': data.get('start_date'),
        'end_date': None,
        'notes': f"Hired from pipeline (candidate {candidate_id})",
        'created_by': user_email,
        'created_at': now,
        'updated_by': user_email,
        'updated_at': now,
    })

    # Withdraw other active candidates for this position
    db.execute(f"""
        UPDATE `{db.table_ref('candidate')}`
        SET status = 'withdrawn', updated_by = @user, updated_at = @now
        WHERE position_id = @pid AND candidate_id != @cid AND status = 'active'
    """, [
        bigquery.ScalarQueryParameter("user", "STRING", user_email),
        bigquery.ScalarQueryParameter("now", "STRING", now),
        bigquery.ScalarQueryParameter("pid", "STRING", cand['position_id']),
        bigquery.ScalarQueryParameter("cid", "STRING", candidate_id),
    ])

    return jsonify({
        'message': 'Candidate hired',
        'assignment_id': assignment_id,
        'position_id': cand['position_id'],
    })


# ============================================================
# Hiring Targets
# ============================================================

@bp.route('/api/hiring-targets')
@auth.require_auth
def get_hiring_targets():
    """Get monthly hiring targets for progress tracking."""
    tenant_id = config.get_tenant_id()
    year = request.args.get('year')
    if not year:
        py = config.planning_year()
        year = py['year_id'] if py else config.current_year()['year_id']

    targets = db.query(
        f"SELECT * FROM `{db.table_ref('hiring_target')}` WHERE tenant_id = @tid AND school_year = @year ORDER BY month",
        [
            bigquery.ScalarQueryParameter("tid", "STRING", tenant_id),
            bigquery.ScalarQueryParameter("year", "STRING", year),
        ]
    )
    return jsonify(targets)
