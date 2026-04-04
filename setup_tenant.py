"""
TalentPoint — Tenant setup script.

Creates all BigQuery tables and seeds initial configuration for a new tenant.
Run once per client deployment.

Usage:
    python setup_tenant.py --tenant-id firstline --name "FirstLine Schools" --domain firstlineschools.org
"""

import argparse
import os
from google.cloud import bigquery


def get_client(project_id):
    return bigquery.Client(project=project_id)


def create_tables(client, dataset_id):
    """Create all TalentPoint tables in the dataset."""
    dataset_ref = f"{client.project}.{dataset_id}"

    # Ensure dataset exists
    try:
        client.get_dataset(dataset_ref)
        print(f"  Dataset {dataset_ref} exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        print(f"  Created dataset {dataset_ref}.")

    tables = {
        'tenant': [
            bigquery.SchemaField("tenant_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("short_name", "STRING"),
            bigquery.SchemaField("domain", "STRING"),
            bigquery.SchemaField("logo_url", "STRING"),
            bigquery.SchemaField("primary_color", "STRING"),
            bigquery.SchemaField("secondary_color", "STRING"),
            bigquery.SchemaField("timezone", "STRING"),
            bigquery.SchemaField("hris_adapter_type", "STRING"),
            bigquery.SchemaField("hris_source_table", "STRING"),
            bigquery.SchemaField("hris_email_field", "STRING"),
            bigquery.SchemaField("hris_first_name_field", "STRING"),
            bigquery.SchemaField("hris_last_name_field", "STRING"),
            bigquery.SchemaField("hris_title_field", "STRING"),
            bigquery.SchemaField("hris_location_field", "STRING"),
            bigquery.SchemaField("hris_department_field", "STRING"),
            bigquery.SchemaField("hris_status_field", "STRING"),
            bigquery.SchemaField("hris_employee_id_field", "STRING"),
            bigquery.SchemaField("hris_supervisor_field", "STRING"),
            bigquery.SchemaField("hris_hire_date_field", "STRING"),
            bigquery.SchemaField("hris_function_field", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ],
        'school': [
            bigquery.SchemaField("school_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("tenant_id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("full_name", "STRING"),
            bigquery.SchemaField("hris_location_name", "STRING"),
            bigquery.SchemaField("is_site_school", "BOOL"),
            bigquery.SchemaField("sort_order", "INT64"),
            bigquery.SchemaField("active", "BOOL"),
        ],
        'job_category': [
            bigquery.SchemaField("category_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("tenant_id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("sort_order", "INT64"),
            bigquery.SchemaField("active", "BOOL"),
        ],
        'school_year': [
            bigquery.SchemaField("year_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("tenant_id", "STRING"),
            bigquery.SchemaField("label", "STRING"),
            bigquery.SchemaField("start_date", "DATE"),
            bigquery.SchemaField("end_date", "DATE"),
            bigquery.SchemaField("is_current", "BOOL"),
            bigquery.SchemaField("is_planning", "BOOL"),
        ],
        'user_role': [
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("tenant_id", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("role", "STRING"),
            bigquery.SchemaField("school_id", "STRING"),
            bigquery.SchemaField("active", "BOOL"),
            bigquery.SchemaField("last_login", "TIMESTAMP"),
        ],
    }

    # --- Position Management ---
    tables['position'] = [
        bigquery.SchemaField("position_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("school_id", "STRING"),
        bigquery.SchemaField("category_id", "STRING"),
        bigquery.SchemaField("job_title", "STRING"),
        bigquery.SchemaField("subject", "STRING"),
        bigquery.SchemaField("grade_level", "STRING"),
        bigquery.SchemaField("funding_source", "STRING"),
        bigquery.SchemaField("fte", "FLOAT64"),
        bigquery.SchemaField("position_code", "STRING"),
        bigquery.SchemaField("staffing_matrix", "STRING"),
        bigquery.SchemaField("status", "STRING"),          # active, frozen, eliminated
        bigquery.SchemaField("start_year", "STRING"),
        bigquery.SchemaField("end_year", "STRING"),
        bigquery.SchemaField("request_id", "STRING"),      # FK to position_request
        bigquery.SchemaField("notes", "STRING"),
        bigquery.SchemaField("created_by", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_by", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    tables['position_assignment'] = [
        bigquery.SchemaField("assignment_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("position_id", "STRING"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("school_year", "STRING"),
        bigquery.SchemaField("employee_name", "STRING"),
        bigquery.SchemaField("employee_email", "STRING"),
        bigquery.SchemaField("employee_number", "STRING"),
        bigquery.SchemaField("assignment_status", "STRING"),  # active, returning, leaving, new_hire, open
        bigquery.SchemaField("itr_response", "STRING"),
        bigquery.SchemaField("candidate_name", "STRING"),     # pre-hire tracking
        bigquery.SchemaField("start_date", "DATE"),
        bigquery.SchemaField("end_date", "DATE"),
        bigquery.SchemaField("notes", "STRING"),
        bigquery.SchemaField("created_by", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_by", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    tables['position_history'] = [
        bigquery.SchemaField("history_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("position_id", "STRING"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("table_name", "STRING"),      # position, position_assignment, etc.
        bigquery.SchemaField("record_id", "STRING"),
        bigquery.SchemaField("action", "STRING"),           # CREATE, UPDATE, DELETE
        bigquery.SchemaField("field_changed", "STRING"),
        bigquery.SchemaField("old_value", "STRING"),
        bigquery.SchemaField("new_value", "STRING"),
        bigquery.SchemaField("changed_by", "STRING"),
        bigquery.SchemaField("changed_at", "TIMESTAMP"),
    ]

    tables['staffing_target'] = [
        bigquery.SchemaField("target_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("school_id", "STRING"),
        bigquery.SchemaField("category_id", "STRING"),
        bigquery.SchemaField("school_year", "STRING"),
        bigquery.SchemaField("job_title", "STRING"),
        bigquery.SchemaField("target_count", "INT64"),
        bigquery.SchemaField("notes", "STRING"),
        bigquery.SchemaField("created_by", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_by", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    # --- Position Requests & Approvals ---
    tables['approval_chain'] = [
        bigquery.SchemaField("chain_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("request_type", "STRING"),
        bigquery.SchemaField("active", "BOOL"),
    ]

    tables['approval_chain_step'] = [
        bigquery.SchemaField("step_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chain_id", "STRING"),
        bigquery.SchemaField("step_order", "INT64"),
        bigquery.SchemaField("approver_role", "STRING"),
        bigquery.SchemaField("approver_user_id", "STRING"),
        bigquery.SchemaField("label", "STRING"),
    ]

    tables['position_request'] = [
        bigquery.SchemaField("request_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("request_type", "STRING"),
        bigquery.SchemaField("status", "STRING"),           # draft, pending, approved, denied, withdrawn
        bigquery.SchemaField("position_id", "STRING"),      # FK for change/eliminate
        bigquery.SchemaField("school_id", "STRING"),
        bigquery.SchemaField("category_id", "STRING"),
        bigquery.SchemaField("job_title", "STRING"),
        bigquery.SchemaField("subject", "STRING"),
        bigquery.SchemaField("grade_level", "STRING"),
        bigquery.SchemaField("school_year", "STRING"),
        bigquery.SchemaField("funding_source", "STRING"),
        bigquery.SchemaField("fte", "FLOAT64"),
        bigquery.SchemaField("hours_status", "STRING"),
        bigquery.SchemaField("reports_to", "STRING"),
        bigquery.SchemaField("justification", "STRING"),
        bigquery.SchemaField("change_description", "STRING"),
        bigquery.SchemaField("hire_type", "STRING"),        # post_position, promote_transfer
        bigquery.SchemaField("employee_email", "STRING"),   # departing employee
        bigquery.SchemaField("employee_name", "STRING"),
        bigquery.SchemaField("candidate_email", "STRING"),  # internal candidate
        bigquery.SchemaField("candidate_position_id", "STRING"),
        bigquery.SchemaField("linked_position_id", "STRING"),
        bigquery.SchemaField("sped_reviewed", "STRING"),
        bigquery.SchemaField("requested_amount", "STRING"),
        bigquery.SchemaField("payment_dates", "STRING"),
        bigquery.SchemaField("chain_id", "STRING"),
        bigquery.SchemaField("current_step", "INT64"),
        bigquery.SchemaField("final_status", "STRING"),     # pending, approved, denied
        bigquery.SchemaField("offer_sent", "DATE"),
        bigquery.SchemaField("offer_signed", "DATE"),
        bigquery.SchemaField("admin_notes", "STRING"),
        bigquery.SchemaField("is_archived", "BOOL"),
        bigquery.SchemaField("requested_by", "STRING"),
        bigquery.SchemaField("requested_at", "TIMESTAMP"),
        bigquery.SchemaField("resolved_at", "TIMESTAMP"),
        bigquery.SchemaField("resolved_by", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_by", "STRING"),
    ]

    tables['approval_action'] = [
        bigquery.SchemaField("action_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("request_id", "STRING"),
        bigquery.SchemaField("step_id", "STRING"),
        bigquery.SchemaField("action", "STRING"),           # approve, deny, comment, return
        bigquery.SchemaField("comment", "STRING"),
        bigquery.SchemaField("acted_by", "STRING"),
        bigquery.SchemaField("acted_at", "TIMESTAMP"),
    ]

    # --- Hiring Pipeline ---
    tables['pipeline_stage'] = [
        bigquery.SchemaField("stage_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("stage_order", "INT64"),
        bigquery.SchemaField("is_terminal", "BOOL"),
        bigquery.SchemaField("stage_type", "STRING"),       # active, hired, rejected, withdrawn
        bigquery.SchemaField("color", "STRING"),
    ]

    tables['candidate'] = [
        bigquery.SchemaField("candidate_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("position_id", "STRING"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("source", "STRING"),           # referral, job_board, direct, ats
        bigquery.SchemaField("current_stage_id", "STRING"),
        bigquery.SchemaField("status", "STRING"),           # active, hired, withdrawn, rejected
        bigquery.SchemaField("notes", "STRING"),
        bigquery.SchemaField("created_by", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_by", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    tables['candidate_stage_history'] = [
        bigquery.SchemaField("history_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("candidate_id", "STRING"),
        bigquery.SchemaField("from_stage_id", "STRING"),
        bigquery.SchemaField("to_stage_id", "STRING"),
        bigquery.SchemaField("notes", "STRING"),
        bigquery.SchemaField("moved_by", "STRING"),
        bigquery.SchemaField("moved_at", "TIMESTAMP"),
    ]

    tables['hiring_target'] = [
        bigquery.SchemaField("target_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("school_year", "STRING"),
        bigquery.SchemaField("month", "INT64"),
        bigquery.SchemaField("target_pct", "FLOAT64"),
        bigquery.SchemaField("notes", "STRING"),
    ]

    # --- Staff Referral Program ---
    tables['referral_config'] = [
        bigquery.SchemaField("config_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("position_type", "STRING"),    # e.g., "Lead Teacher", "Other"
        bigquery.SchemaField("bonus_amount", "INT64"),
        bigquery.SchemaField("retention_days", "INT64"),     # days before payout eligible (e.g., 60)
        bigquery.SchemaField("active", "BOOL"),
    ]

    tables['referral'] = [
        bigquery.SchemaField("referral_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("submitted_at", "TIMESTAMP"),
        bigquery.SchemaField("referrer_name", "STRING"),
        bigquery.SchemaField("referrer_email", "STRING"),
        bigquery.SchemaField("referrer_school", "STRING"),
        bigquery.SchemaField("candidate_name", "STRING"),
        bigquery.SchemaField("candidate_email", "STRING"),
        bigquery.SchemaField("candidate_phone", "STRING"),
        bigquery.SchemaField("position", "STRING"),
        bigquery.SchemaField("position_type", "STRING"),
        bigquery.SchemaField("bonus_amount", "INT64"),
        bigquery.SchemaField("relationship", "STRING"),
        bigquery.SchemaField("already_applied", "STRING"),
        bigquery.SchemaField("notes", "STRING"),
        bigquery.SchemaField("status", "STRING"),           # Submitted, Under Review, Interviewing, Hired, Eligible, Paid, etc.
        bigquery.SchemaField("status_updated_at", "TIMESTAMP"),
        bigquery.SchemaField("status_updated_by", "STRING"),
        bigquery.SchemaField("hire_date", "DATE"),
        bigquery.SchemaField("retention_date", "DATE"),     # hire_date + retention_days
        bigquery.SchemaField("payout_month", "STRING"),
        bigquery.SchemaField("paid_date", "DATE"),
        bigquery.SchemaField("admin_notes", "STRING"),
        bigquery.SchemaField("is_archived", "BOOL"),
    ]

    # --- Onboarding ---
    tables['onboarding_config'] = [
        bigquery.SchemaField("field_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("field_name", "STRING"),
        bigquery.SchemaField("field_label", "STRING"),
        bigquery.SchemaField("field_type", "STRING"),       # text, select, radio, checkbox
        bigquery.SchemaField("options", "STRING"),           # JSON array for select/radio options
        bigquery.SchemaField("section", "STRING"),           # about_you, preferences, compliance
        bigquery.SchemaField("required", "BOOL"),
        bigquery.SchemaField("sort_order", "INT64"),
        bigquery.SchemaField("active", "BOOL"),
    ]

    tables['onboarding_submission'] = [
        bigquery.SchemaField("submission_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("submitted_at", "TIMESTAMP"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("preferred_name", "STRING"),
        bigquery.SchemaField("school_location", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("physical_address", "STRING"),
        bigquery.SchemaField("tshirt_size", "STRING"),
        bigquery.SchemaField("dietary_needs", "STRING"),
        bigquery.SchemaField("food_allergies", "STRING"),
        bigquery.SchemaField("ada_accommodation", "STRING"),
        bigquery.SchemaField("custom_fields", "STRING"),     # JSON for tenant-specific fields
        bigquery.SchemaField("onboarding_status", "STRING"), # Not Started, In Progress, Complete
        bigquery.SchemaField("start_date", "DATE"),
        bigquery.SchemaField("position_title", "STRING"),
        bigquery.SchemaField("badge_printed", "STRING"),
        bigquery.SchemaField("equipment_issued", "STRING"),
        bigquery.SchemaField("orientation_complete", "STRING"),
        bigquery.SchemaField("admin_notes", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_by", "STRING"),
        bigquery.SchemaField("is_archived", "BOOL"),
        bigquery.SchemaField("candidate_id", "STRING"),     # FK to pipeline candidate (if hired via pipeline)
    ]

    # --- Compensation ---
    tables['salary_role'] = [
        bigquery.SchemaField("role_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("name", "STRING"),             # display name: "Teacher"
        bigquery.SchemaField("category", "STRING"),         # grouping: "Teaching", "Classroom Support", "Leadership"
        bigquery.SchemaField("salary_key", "STRING"),       # key into salary_schedule: "teacher"
        bigquery.SchemaField("is_hourly", "BOOL"),
        bigquery.SchemaField("longevity_eligible", "BOOL"), # gets tenure bonus
        bigquery.SchemaField("sort_order", "INT64"),
        bigquery.SchemaField("active", "BOOL"),
    ]

    tables['salary_schedule'] = [
        bigquery.SchemaField("schedule_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("salary_key", "STRING"),       # matches salary_role.salary_key
        bigquery.SchemaField("step", "INT64"),              # years of experience (0-30)
        bigquery.SchemaField("annual_amount", "FLOAT64"),
        bigquery.SchemaField("hourly_amount", "FLOAT64"),
    ]

    tables['longevity_tier'] = [
        bigquery.SchemaField("tier_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("min_years", "INT64"),         # years at org
        bigquery.SchemaField("max_years", "INT64"),
        bigquery.SchemaField("bonus_amount", "FLOAT64"),
    ]

    tables['salary_category'] = [
        bigquery.SchemaField("category_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("name", "STRING"),             # "Paraprofessional", "Teacher", etc.
        bigquery.SchemaField("title_patterns", "STRING"),   # JSON array of LIKE patterns for HRIS matching
        bigquery.SchemaField("default_base", "FLOAT64"),    # starting salary for projection
        bigquery.SchemaField("sort_order", "INT64"),
    ]

    for table_name, schema in tables.items():
        table_ref = f"{dataset_ref}.{table_name}"
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"  Table {table_name} ready.")


def seed_firstline(client, dataset_id, tenant_id):
    """Seed FirstLine Schools configuration as the reference tenant."""
    dataset_ref = f"{client.project}.{dataset_id}"
    now = "CURRENT_TIMESTAMP()"

    # Tenant
    client.query(f"""
        INSERT INTO `{dataset_ref}.tenant` VALUES (
            '{tenant_id}', 'FirstLine Schools', 'firstline', 'firstlineschools.org',
            'https://firstlineschools.org/wp-content/uploads/2020/08/FLS-Logo_v2019_Color.png',
            '#e47727', '#002f60', 'America/Chicago',
            'bigquery',
            'talent-demo-482004.talent_grow_observations.staff_master_list_with_function',
            'Email_Address', 'first_name', 'last_name', 'Job_Title',
            'Location_Name', 'Dept', 'Employment_Status', 'Employee_ID',
            'Supervisor_Name__Unsecured_', 'Last_Hire_Date', 'Job_Function',
            {now}
        )
    """).result()
    print("  Seeded tenant: FirstLine Schools")

    # Schools
    schools = [
        ("ashe", "Arthur Ashe", "Arthur Ashe Charter School", "Arthur Ashe Charter School", True, 1),
        ("green", "Samuel J Green", "Samuel J Green Charter School", "Samuel J Green Charter School", True, 2),
        ("hughes", "Langston Hughes", "Langston Hughes Academy", "Langston Hughes Academy", True, 3),
        ("wheatley", "Phillis Wheatley", "Phillis Wheatley Community School", "Phillis Wheatley Community School", True, 4),
        ("network", "Network", "FirstLine Network", "FirstLine Network", False, 5),
    ]
    for sid, name, full_name, hris_name, is_site, sort in schools:
        client.query(f"""
            INSERT INTO `{dataset_ref}.school` VALUES (
                '{sid}', '{tenant_id}', '{name}', '{full_name}', '{hris_name}', {is_site}, {sort}, TRUE
            )
        """).result()
    print(f"  Seeded {len(schools)} schools")

    # Job Categories
    categories = [
        ("leadership", "Leadership", 1),
        ("teacher", "Teacher", 2),
        ("support", "Support", 3),
        ("operations", "Operations", 4),
        ("network", "Network", 5),
    ]
    for cid, name, sort in categories:
        client.query(f"""
            INSERT INTO `{dataset_ref}.job_category` VALUES (
                '{cid}', '{tenant_id}', '{name}', {sort}, TRUE
            )
        """).result()
    print(f"  Seeded {len(categories)} categories")

    # School Years
    years = [
        ("25-26", "2025-26", "2025-07-01", "2026-06-30", True, False),
        ("26-27", "2026-27", "2026-07-01", "2027-06-30", False, True),
        ("27-28", "2027-28", "2027-07-01", "2028-06-30", False, False),
    ]
    for yid, label, start, end, is_current, is_planning in years:
        client.query(f"""
            INSERT INTO `{dataset_ref}.school_year` VALUES (
                '{yid}', '{tenant_id}', '{label}', '{start}', '{end}', {is_current}, {is_planning}
            )
        """).result()
    print(f"  Seeded {len(years)} school years")

    # Pipeline Stages (default)
    stages = [
        ("sourcing", "Sourcing", 1, False, "active", "#3b82f6"),
        ("screening", "Screening", 2, False, "active", "#8b5cf6"),
        ("interview", "Interview", 3, False, "active", "#f59e0b"),
        ("finalist", "Finalist", 4, False, "active", "#ec4899"),
        ("offer", "Offer Extended", 5, False, "active", "#06b6d4"),
        ("hired", "Hired", 6, True, "hired", "#22c55e"),
        ("withdrawn", "Withdrawn", 7, True, "withdrawn", "#6b7280"),
        ("rejected", "Not Selected", 8, True, "rejected", "#ef4444"),
    ]
    for sid, name, order, terminal, stype, color in stages:
        client.query(f"""
            INSERT INTO `{dataset_ref}.pipeline_stage` VALUES (
                '{sid}', '{tenant_id}', '{name}', {order}, {terminal}, '{stype}', '{color}'
            )
        """).result()
    print(f"  Seeded {len(stages)} pipeline stages")

    # Approval Chains
    chains = [
        ("new_position", "New Position Approval", "New Position"),
        ("open_position", "Open Position Notification", "Open Position"),
        ("status_change", "Status Change Approval", "Status Change"),
        ("title_change", "Title/Role Change Approval", "Title/Role Change"),
        ("stipend", "Stipend Approval", "Additional Comp (Stipend)"),
        ("temp_hire", "Temp Hire Approval", "Temp Hire"),
    ]
    for cid, name, rtype in chains:
        client.query(f"""
            INSERT INTO `{dataset_ref}.approval_chain` VALUES (
                '{cid}', '{tenant_id}', '{name}', '{rtype}', TRUE
            )
        """).result()

    # Approval steps — New Position requires CEO + Finance + Talent + HR
    new_pos_steps = [
        ("np_ceo", "new_position", 1, "ceo", "", "CEO Approval"),
        ("np_fin", "new_position", 2, "finance", "", "Finance Approval"),
        ("np_tal", "new_position", 3, "hr", "", "Talent Review"),
        ("np_hr", "new_position", 4, "hr", "", "HR Processing"),
    ]
    # Other types just need Talent + HR
    other_steps = [
        ("op_tal", "open_position", 1, "hr", "", "Talent Review"),
        ("op_hr", "open_position", 2, "hr", "", "HR Processing"),
        ("sc_tal", "status_change", 1, "hr", "", "Talent Review"),
        ("sc_hr", "status_change", 2, "hr", "", "HR Processing"),
        ("tc_tal", "title_change", 1, "hr", "", "Talent Review"),
        ("tc_hr", "title_change", 2, "hr", "", "HR Processing"),
        ("st_tal", "stipend", 1, "hr", "", "Talent Review"),
        ("st_hr", "stipend", 2, "hr", "", "HR Processing"),
        ("th_tal", "temp_hire", 1, "hr", "", "Talent Review"),
        ("th_hr", "temp_hire", 2, "hr", "", "HR Processing"),
    ]
    all_steps = new_pos_steps + other_steps
    for sid, cid, order, role, uid, label in all_steps:
        client.query(f"""
            INSERT INTO `{dataset_ref}.approval_chain_step` VALUES (
                '{sid}', '{cid}', {order}, '{role}', '{uid}', '{label}'
            )
        """).result()
    print(f"  Seeded {len(chains)} approval chains with {len(all_steps)} steps")

    # Hiring Targets (default monthly targets for planning year)
    targets = [
        (1, 0.07), (2, 0.16), (3, 0.33), (4, 0.50),
        (5, 0.72), (6, 0.88), (7, 0.95), (8, 1.00),
    ]
    for month, pct in targets:
        tid_val = f"ht_{month}"
        client.query(f"""
            INSERT INTO `{dataset_ref}.hiring_target` VALUES (
                '{tid_val}', '{tenant_id}', '26-27', {month}, {pct}, NULL
            )
        """).result()
    print(f"  Seeded {len(targets)} monthly hiring targets")

    # Referral Bonus Tiers
    bonus_tiers = [
        ("lead_teacher", "Lead Teacher", 500, 60),
        ("other", "Other", 300, 60),
    ]
    for bid, ptype, amount, days in bonus_tiers:
        client.query(f"""
            INSERT INTO `{dataset_ref}.referral_config` VALUES (
                '{bid}', '{tenant_id}', '{ptype}', {amount}, {days}, TRUE
            )
        """).result()
    print(f"  Seeded {len(bonus_tiers)} referral bonus tiers")

    # Onboarding custom fields (Louisiana-specific compliance questions)
    onboard_fields = [
        ("reading_cert", "reading_certification", "Science of Reading Certification (K-3)", "select", '["Yes","No","In process","N/A"]', "compliance", True, 1),
        ("numeracy_cert", "numeracy_coursework", "Numeracy/Act 108 Coursework (4-8)", "select", '["Yes","No","In process","N/A"]', "compliance", True, 2),
    ]
    for fid, fname, flabel, ftype, opts, section, req, sort in onboard_fields:
        client.query(f"""
            INSERT INTO `{dataset_ref}.onboarding_config` VALUES (
                '{fid}', '{tenant_id}', '{fname}', '{flabel}', '{ftype}', '{opts}', '{section}', {req}, {sort}, TRUE
            )
        """).result()
    print(f"  Seeded {len(onboard_fields)} onboarding custom fields")

    # Salary Roles
    salary_roles = [
        ("para", "Paraprofessional", "Classroom Support", "paraprofessional", False, True, 1),
        ("ta", "Teacher Assistant", "Classroom Support", "teacher_assistant", False, True, 2),
        ("teacher", "Teacher", "Teaching", "teacher", False, True, 3),
        ("asst_dean", "Assistant Dean / Associate Teacher", "Teaching", "asst_dean_associate_teacher", False, True, 4),
        ("lead_dean", "Lead Dean", "Student Services", "lead_dean", False, False, 5),
        ("nurse", "School Nurse", "Student Services", "nurse", False, False, 6),
        ("slp", "Speech Language Pathologist", "Student Services", "slp", False, False, 7),
        ("ops_mgr", "School Operations Manager", "Leadership", "school_ops_manager", False, False, 8),
        ("ap", "Assistant Principal / SPED / RTI Coordinator", "Leadership", "asst_principal", False, False, 9),
        ("principal", "Principal", "Leadership", "principal", False, False, 10),
        ("director", "School Director", "Leadership", "school_director", False, False, 11),
    ]
    for rid, name, cat, key, hourly, longevity, sort in salary_roles:
        client.query(f"""
            INSERT INTO `{dataset_ref}.salary_role` VALUES (
                '{rid}', '{tenant_id}', '{name}', '{cat}', '{key}', {hourly}, {longevity}, {sort}, TRUE
            )
        """).result()
    print(f"  Seeded {len(salary_roles)} salary roles")

    # Salary Schedule (abbreviated — key steps for Teacher as example)
    teacher_steps = [
        (0, 48000), (1, 48500), (2, 49000), (3, 49600), (4, 50200),
        (5, 51000), (6, 51800), (7, 52700), (8, 53600), (9, 54600),
        (10, 55700), (11, 56800), (12, 58000), (13, 59200), (14, 60500),
        (15, 61900), (16, 63300), (17, 64800), (18, 66300), (19, 67900),
        (20, 69600), (21, 71300), (22, 73100), (23, 75000), (24, 77000),
        (25, 79000), (26, 81100), (27, 83300), (28, 85500), (29, 87800),
        (30, 90200),
    ]
    for step, amount in teacher_steps:
        sid = f"t_{step}"
        client.query(f"""
            INSERT INTO `{dataset_ref}.salary_schedule` VALUES (
                '{sid}', '{tenant_id}', 'teacher', {step}, {amount}, NULL
            )
        """).result()
    print(f"  Seeded {len(teacher_steps)} teacher salary steps")

    # Para schedule (abbreviated)
    para_steps = [
        (0, 28850), (5, 31000), (10, 33500), (15, 36000), (20, 39000), (25, 42000), (30, 45000),
    ]
    for step, amount in para_steps:
        sid = f"p_{step}"
        client.query(f"""
            INSERT INTO `{dataset_ref}.salary_schedule` VALUES (
                '{sid}', '{tenant_id}', 'paraprofessional', {step}, {amount}, NULL
            )
        """).result()
    print(f"  Seeded {len(para_steps)} paraprofessional salary steps")

    # Longevity Tiers
    longevity = [
        ("l0", 0, 0, 0), ("l1", 1, 2, 500), ("l2", 3, 5, 750),
        ("l3", 6, 9, 1000), ("l4", 10, 30, 1250),
    ]
    for lid, min_y, max_y, amount in longevity:
        client.query(f"""
            INSERT INTO `{dataset_ref}.longevity_tier` VALUES (
                '{lid}', '{tenant_id}', {min_y}, {max_y}, {amount}
            )
        """).result()
    print(f"  Seeded {len(longevity)} longevity tiers")

    # Salary Categories (for projection dashboard — maps HRIS titles to groups)
    sal_cats = [
        ("sc_para", "Paraprofessional", '["%paraprofessional%"]', 28850, 1),
        ("sc_asst", "Assistant Teacher", '["%asst teacher%", "%assistant teacher%"]', 31900, 2),
        ("sc_teacher", "Teacher", '["%teacher%"]', 48000, 3),
    ]
    for cid, name, patterns, base, sort in sal_cats:
        client.query(f"""
            INSERT INTO `{dataset_ref}.salary_category` VALUES (
                '{cid}', '{tenant_id}', '{name}', '{patterns}', {base}, {sort}
            )
        """).result()
    print(f"  Seeded {len(sal_cats)} salary categories")

    print("\n  FirstLine Schools tenant setup complete!")
    print(f"  Add admin users with: INSERT INTO `{dataset_ref}.user_role` ...")


def main():
    parser = argparse.ArgumentParser(description="TalentPoint tenant setup")
    parser.add_argument("--project-id", default="confluence-point-consulting", help="GCP project ID")
    parser.add_argument("--dataset", default="talentpoint", help="BigQuery dataset name")
    parser.add_argument("--tenant-id", default="firstline", help="Tenant ID")
    parser.add_argument("--seed-firstline", action="store_true", help="Seed FirstLine Schools config")
    args = parser.parse_args()

    print(f"\nTalentPoint Setup")
    print(f"  Project: {args.project_id}")
    print(f"  Dataset: {args.dataset}")
    print(f"  Tenant:  {args.tenant_id}")
    print()

    client = get_client(args.project_id)

    print("Creating tables...")
    create_tables(client, args.dataset)

    if args.seed_firstline:
        print("\nSeeding FirstLine Schools data...")
        seed_firstline(client, args.dataset, args.tenant_id)

    print("\nDone!")


if __name__ == "__main__":
    main()
