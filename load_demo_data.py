"""Load realistic demo data into TalentPoint for pitch demos."""

import uuid
import random
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

client = bigquery.Client(project='confluence-point-consulting')
DS = 'confluence-point-consulting.talentpoint'
TID = 'firstline'
NOW = datetime.now(timezone.utc).isoformat()


def run_query(sql, params):
    """Execute a parameterized query."""
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(sql, job_config=job_config).result()

schools = [
    ('ashe', 'Northside Elementary'),
    ('green', 'Riverside Academy'),
    ('hughes', 'Westlake Middle'),
    ('wheatley', 'Eastbrook High'),
]

titles_by_cat = {
    'teacher': [
        '3rd Grade Teacher', '4th Grade Teacher', '5th Grade Teacher',
        'Math Teacher', 'ELA Teacher', 'Science Teacher',
        'Social Studies Teacher', 'SPED Teacher', 'Art Teacher',
        'Music Teacher', 'PE Teacher', 'ESL Teacher',
    ],
    'leadership': ['Principal', 'Assistant Principal', 'Dean of Students', 'Instructional Coach'],
    'support': ['Paraprofessional', 'Counselor', 'Social Worker', 'Speech Pathologist', 'School Nurse'],
    'operations': ['Office Manager', 'Receptionist', 'Custodian', 'IT Specialist'],
}

first_names = [
    'Maria', 'James', 'Sarah', 'Michael', 'Jessica', 'David', 'Ashley',
    'Robert', 'Jennifer', 'William', 'Amanda', 'Daniel', 'Stephanie',
    'Christopher', 'Nicole', 'Anthony', 'Brittany', 'Kevin', 'Lauren',
    'Marcus', 'Tanya', 'Carlos', 'Keisha', 'Andre', 'Crystal', 'DeShawn',
    'Latoya', 'Jamal', 'Monique', 'Terrence', 'Patricia', 'Raymond',
    'Vanessa', 'Gregory', 'Angela', 'Brian', 'Donna', 'Timothy', 'Lisa',
]

last_names = [
    'Johnson', 'Williams', 'Brown', 'Davis', 'Wilson', 'Anderson', 'Thomas',
    'Jackson', 'White', 'Harris', 'Martin', 'Thompson', 'Garcia', 'Robinson',
    'Clark', 'Lewis', 'Lee', 'Walker', 'Hall', 'Allen', 'Young', 'King',
    'Wright', 'Scott', 'Green', 'Baker', 'Adams', 'Nelson', 'Hill', 'Mitchell',
]

subjects = ['Math', 'ELA', 'Science', 'Social Studies', 'SPED', 'Art', 'Music', 'PE', 'ESL']
grades = ['K', '1st', '2nd', '3rd', '4th', '5th', '6th', '7th', '8th']


def gen_id():
    return uuid.uuid4().hex[:8].upper()


def gen_person():
    fn = random.choice(first_names)
    ln = random.choice(last_names)
    return fn, ln, f"{fn.lower()}.{ln.lower()}@example.org"


def insert_position(pid, school_id, cat_id, title, subject='', grade=''):
    sql = f"""
        INSERT INTO `{DS}.position`
        (position_id, tenant_id, school_id, category_id, job_title, subject, grade_level,
         funding_source, fte, position_code, staffing_matrix, status, start_year, end_year,
         request_id, notes, created_by, created_at, updated_by, updated_at)
        VALUES (@pid, @tid, @sid, @cid, @title, @subj, @grade,
                '', 1.0, '', '', 'active', '25-26', NULL,
                '', '', 'demo', @now, 'demo', @now)
    """
    run_query(sql, [
        bigquery.ScalarQueryParameter("pid", "STRING", pid),
        bigquery.ScalarQueryParameter("tid", "STRING", TID),
        bigquery.ScalarQueryParameter("sid", "STRING", school_id),
        bigquery.ScalarQueryParameter("cid", "STRING", cat_id),
        bigquery.ScalarQueryParameter("title", "STRING", title),
        bigquery.ScalarQueryParameter("subj", "STRING", subject),
        bigquery.ScalarQueryParameter("grade", "STRING", grade),
        bigquery.ScalarQueryParameter("now", "STRING", NOW),
    ])


def insert_assignment(pid, name, email, emp_num, status, itr_resp):
    aid = gen_id()
    sql = f"""
        INSERT INTO `{DS}.position_assignment`
        (assignment_id, position_id, tenant_id, school_year, employee_name, employee_email,
         employee_number, assignment_status, itr_response, candidate_name,
         start_date, end_date, notes, created_by, created_at, updated_by, updated_at)
        VALUES (@aid, @pid, @tid, '25-26', @name, @email,
                @enum, @status, @itr, '',
                NULL, NULL, '', 'demo', @now, 'demo', @now)
    """
    run_query(sql, [
        bigquery.ScalarQueryParameter("aid", "STRING", aid),
        bigquery.ScalarQueryParameter("pid", "STRING", pid),
        bigquery.ScalarQueryParameter("tid", "STRING", TID),
        bigquery.ScalarQueryParameter("name", "STRING", name),
        bigquery.ScalarQueryParameter("email", "STRING", email),
        bigquery.ScalarQueryParameter("enum", "STRING", emp_num),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("itr", "STRING", itr_resp),
        bigquery.ScalarQueryParameter("now", "STRING", NOW),
    ])


def insert_referral(referrer_name, referrer_email, cand_name, position, status, bonus):
    rid = gen_id()
    days_ago = random.randint(1, 60)
    submitted = (datetime.utcnow() - timedelta(days=days_ago)).isoformat()
    sql = f"""
        INSERT INTO `{DS}.referral`
        (referral_id, tenant_id, submitted_at, referrer_name, referrer_email, referrer_school,
         candidate_name, candidate_email, candidate_phone, position, position_type,
         bonus_amount, relationship, already_applied, notes, status,
         status_updated_at, status_updated_by, hire_date, retention_date, payout_month,
         paid_date, admin_notes, is_archived)
        VALUES (@rid, @tid, @sub, @rname, @remail, 'Northside Elementary',
                @cname, @cemail, '555-0100', @pos, 'Other',
                @bonus, 'Colleague', 'Yes', '', @status,
                @sub, '', NULL, NULL, NULL,
                NULL, '', FALSE)
    """
    _, _, cemail = gen_person()
    run_query(sql, [
        bigquery.ScalarQueryParameter("rid", "STRING", rid),
        bigquery.ScalarQueryParameter("tid", "STRING", TID),
        bigquery.ScalarQueryParameter("sub", "STRING", submitted),
        bigquery.ScalarQueryParameter("rname", "STRING", referrer_name),
        bigquery.ScalarQueryParameter("remail", "STRING", referrer_email),
        bigquery.ScalarQueryParameter("cname", "STRING", cand_name),
        bigquery.ScalarQueryParameter("cemail", "STRING", cemail),
        bigquery.ScalarQueryParameter("pos", "STRING", position),
        bigquery.ScalarQueryParameter("bonus", "INT64", bonus),
        bigquery.ScalarQueryParameter("status", "STRING", status),
    ])


def insert_request(req_type, title, school_id, status, days_ago):
    rid = gen_id()
    submitted = (datetime.utcnow() - timedelta(days=days_ago)).isoformat()
    fn, ln, email = gen_person()
    sql = f"""
        INSERT INTO `{DS}.position_request`
        (request_id, tenant_id, request_type, status, position_id, school_id, category_id,
         job_title, subject, grade_level, school_year, funding_source, fte, hours_status,
         reports_to, justification, change_description, hire_type, employee_email, employee_name,
         candidate_email, candidate_position_id, linked_position_id, sped_reviewed,
         requested_amount, payment_dates, chain_id, current_step, final_status,
         offer_sent, offer_signed, admin_notes, is_archived,
         requested_by, requested_at, resolved_at, resolved_by, updated_at, updated_by)
        VALUES (@rid, @tid, @rtype, 'pending', '', @sid, 'teacher',
                @title, '', '', '26-27', '', 1.0, 'Full-Time (40 hrs)',
                '', @just, '', 'Post Position', '', '',
                '', '', '', 'No',
                '', '', '', 1, @fstatus,
                NULL, NULL, '', FALSE,
                @email, @sub, NULL, '', @sub, @email)
    """
    run_query(sql, [
        bigquery.ScalarQueryParameter("rid", "STRING", rid),
        bigquery.ScalarQueryParameter("tid", "STRING", TID),
        bigquery.ScalarQueryParameter("rtype", "STRING", req_type),
        bigquery.ScalarQueryParameter("sid", "STRING", school_id),
        bigquery.ScalarQueryParameter("title", "STRING", title),
        bigquery.ScalarQueryParameter("just", "STRING", f"Needed for {title} position at school"),
        bigquery.ScalarQueryParameter("fstatus", "STRING", status),
        bigquery.ScalarQueryParameter("email", "STRING", email),
        bigquery.ScalarQueryParameter("sub", "STRING", submitted),
    ])


# ============================================================
# Generate positions + assignments
# ============================================================
print("Loading positions...")
total_pos = 0
total_assign = 0

for school_id, school_name in schools:
    # Teachers: 10 per school
    for i in range(10):
        pid = gen_id()
        title = titles_by_cat['teacher'][i % len(titles_by_cat['teacher'])]
        subj = random.choice(subjects) if 'Grade' not in title else ''
        grade = random.choice(grades[:6]) if 'Grade' in title else ''
        insert_position(pid, school_id, 'teacher', title, subj, grade)
        total_pos += 1

        # 80% filled, 10% leaving, 10% open
        roll = random.random()
        if roll < 0.10:
            insert_assignment(pid, '', '', '', 'open', '')
        elif roll < 0.20:
            fn, ln, email = gen_person()
            insert_assignment(pid, f"{fn} {ln}", email, str(random.randint(10000,99999)), 'leaving', 'No')
        else:
            fn, ln, email = gen_person()
            itr_val = random.choices(['Yes', 'Unsure', 'No response yet'], weights=[70, 20, 10])[0]
            insert_assignment(pid, f"{fn} {ln}", email, str(random.randint(10000,99999)), 'active', itr_val)
        total_assign += 1

    # Leadership: 3
    for title in random.sample(titles_by_cat['leadership'], 3):
        pid = gen_id()
        insert_position(pid, school_id, 'leadership', title)
        fn, ln, email = gen_person()
        insert_assignment(pid, f"{fn} {ln}", email, str(random.randint(10000,99999)), 'active', 'Yes')
        total_pos += 1
        total_assign += 1

    # Support: 4
    for title in random.sample(titles_by_cat['support'], 4):
        pid = gen_id()
        insert_position(pid, school_id, 'support', title)
        roll = random.random()
        if roll < 0.15:
            insert_assignment(pid, '', '', '', 'open', '')
        else:
            fn, ln, email = gen_person()
            insert_assignment(pid, f"{fn} {ln}", email, str(random.randint(10000,99999)), 'active', 'Yes')
        total_pos += 1
        total_assign += 1

    # Operations: 2
    for title in random.sample(titles_by_cat['operations'], 2):
        pid = gen_id()
        insert_position(pid, school_id, 'operations', title)
        fn, ln, email = gen_person()
        insert_assignment(pid, f"{fn} {ln}", email, str(random.randint(10000,99999)), 'active', 'Yes')
        total_pos += 1
        total_assign += 1

    print(f"  {school_name}: done")

# Network positions
for title in ['Chief People Officer', 'HR Director', 'Talent Manager', 'Payroll Manager']:
    pid = gen_id()
    cat = 'leadership' if 'Chief' in title or 'Director' in title else 'operations'
    insert_position(pid, 'network', cat, title)
    fn, ln, email = gen_person()
    insert_assignment(pid, f"{fn} {ln}", email, str(random.randint(10000,99999)), 'active', 'Yes')
    total_pos += 1
    total_assign += 1

print(f"  Network Office: done")
print(f"\nPositions: {total_pos}, Assignments: {total_assign}")

# ============================================================
# Referrals
# ============================================================
print("\nLoading referrals...")
ref_statuses = ['Submitted', 'Under Review', 'Interviewing', 'Hired', 'Eligible', 'Paid', 'Not Hired']
for i in range(12):
    fn, ln, email = gen_person()
    cfn, cln, _ = gen_person()
    insert_referral(f"{fn} {ln}", email, f"{cfn} {cln}",
                    random.choice(titles_by_cat['teacher']),
                    random.choice(ref_statuses), random.choice([300, 500]))
print(f"  12 referrals loaded")

# ============================================================
# Position Requests
# ============================================================
print("\nLoading position requests...")
req_data = [
    ('New Position', '6th Grade Math Teacher', 'ashe', 'Pending', 3),
    ('New Position', 'Reading Interventionist', 'green', 'Pending', 7),
    ('Open Position', 'SPED Teacher', 'hughes', 'Approved', 14),
    ('Open Position', 'Art Teacher', 'wheatley', 'Pending', 2),
    ('Title/Role Change', 'Instructional Coach', 'ashe', 'Approved', 21),
    ('Status Change', 'Paraprofessional', 'green', 'Denied', 30),
    ('New Position', 'School Counselor', 'hughes', 'Pending', 5),
    ('Temp Hire', 'Long-term Sub', 'wheatley', 'Approved', 10),
]
for rtype, title, sid, status, days in req_data:
    insert_request(rtype, title, sid, status, days)
print(f"  {len(req_data)} requests loaded")

# ============================================================
# Staffing Targets
# ============================================================
print("\nLoading staffing targets...")
targets = {
    'ashe': {'teacher': 12, 'leadership': 3, 'support': 5, 'operations': 3},
    'green': {'teacher': 11, 'leadership': 3, 'support': 4, 'operations': 2},
    'hughes': {'teacher': 12, 'leadership': 4, 'support': 5, 'operations': 3},
    'wheatley': {'teacher': 10, 'leadership': 3, 'support': 4, 'operations': 2},
}
for sid, cats in targets.items():
    for cid, count in cats.items():
        stid = gen_id()
        run_query(f"""
            INSERT INTO `{DS}.staffing_target`
            (target_id, tenant_id, school_id, category_id, school_year, job_title,
             target_count, notes, created_by, created_at, updated_by, updated_at)
            VALUES (@stid, @tid, @sid, @cid, '26-27', '',
                    @count, '', 'demo', @now, 'demo', @now)
        """, [
            bigquery.ScalarQueryParameter("stid", "STRING", stid),
            bigquery.ScalarQueryParameter("tid", "STRING", TID),
            bigquery.ScalarQueryParameter("sid", "STRING", sid),
            bigquery.ScalarQueryParameter("cid", "STRING", cid),
            bigquery.ScalarQueryParameter("count", "INT64", count),
            bigquery.ScalarQueryParameter("now", "STRING", NOW),
        ])
print(f"  {sum(sum(c.values()) for c in targets.values())} targets across 4 schools")

print("\nDemo data loaded!")
