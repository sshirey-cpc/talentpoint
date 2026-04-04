"""
TalentPoint — BigQuery HRIS adapter.

Reads staff data from a BigQuery table with configurable column mappings.
This is the default adapter — it works with any HRIS that exports to BigQuery
(via scheduled ETL, CSV upload to BQ, or direct connector).

Column mappings are stored in the tenant config table:
  hris_email_field       -> e.g., "Email_Address"
  hris_name_field        -> e.g., "first_name || ' ' || last_name"
  hris_first_name_field  -> e.g., "first_name"
  hris_last_name_field   -> e.g., "last_name"
  hris_title_field       -> e.g., "Job_Title"
  hris_location_field    -> e.g., "Location_Name"
  hris_department_field  -> e.g., "Dept"
  hris_status_field      -> e.g., "Employment_Status"
  hris_employee_id_field -> e.g., "Employee_ID"
  hris_supervisor_field  -> e.g., "Supervisor_Name__Unsecured_"
  hris_hire_date_field   -> e.g., "Last_Hire_Date"
  hris_function_field    -> e.g., "Job_Function"
"""

import config
import db
from hris_adapter import HRISAdapter


class BigQueryHRISAdapter(HRISAdapter):
    """HRIS adapter that reads from a BigQuery table."""

    def __init__(self):
        self.source_table = config.hris_source_table()
        if not self.source_table:
            raise ValueError("hris_source_table not configured in tenant config")
        self._location_map = config.hris_location_map()

    def _field(self, logical_name):
        """Get the actual BQ column name for a logical field."""
        return config.hris_field(logical_name)

    def _active_statuses(self):
        """Status values that mean 'active employee'."""
        return ('Active', 'Leave of absence')

    def _normalize_location(self, hris_location):
        """Map an HRIS location name to the tenant's school short name."""
        if not hris_location:
            return None
        return self._location_map.get(hris_location, hris_location)

    def _standardize_row(self, row):
        """Convert a raw BQ row to the standard TalentPoint staff dict."""
        return {
            'email': (row.get(self._field('email')) or '').strip().lower(),
            'first_name': row.get(self._field('first_name'), ''),
            'last_name': row.get(self._field('last_name'), ''),
            'full_name': f"{row.get(self._field('first_name'), '')} {row.get(self._field('last_name'), '')}".strip(),
            'employee_id': row.get(self._field('employee_id'), ''),
            'job_title': row.get(self._field('title'), ''),
            'job_function': row.get(self._field('function'), ''),
            'department': row.get(self._field('department'), ''),
            'location': row.get(self._field('location'), ''),
            'school': self._normalize_location(row.get(self._field('location'), '')),
            'supervisor': row.get(self._field('supervisor'), ''),
            'status': row.get(self._field('status'), ''),
            'hire_date': row.get(self._field('hire_date'), None),
            # Pass through all raw fields too, for modules that need them
            '_raw': row,
        }

    def get_all_staff(self, active_only=True):
        status_field = self._field('status')
        sql = f"SELECT * FROM `{self.source_table}`"
        params = []
        if active_only:
            sql += f" WHERE {status_field} IN UNNEST(@statuses)"
            params.append(db.bigquery.ArrayQueryParameter("statuses", "STRING", list(self._active_statuses())))
        sql += f" ORDER BY {self._field('last_name')}, {self._field('first_name')}"

        rows = db.query(sql, params or None)
        return [self._standardize_row(r) for r in rows]

    def get_staff_by_email(self, email):
        email_field = self._field('email')
        sql = f"SELECT * FROM `{self.source_table}` WHERE LOWER(TRIM({email_field})) = @email"
        params = [db.bigquery.ScalarQueryParameter("email", "STRING", email.lower().strip())]
        row = db.query_one(sql, params)
        if row:
            return self._standardize_row(row)
        return None

    def get_staff_by_school(self, school_name, active_only=True):
        # Find the HRIS location name for this school
        school = config.school_by_name(school_name)
        hris_location = school['hris_location_name'] if school else school_name

        location_field = self._field('location')
        status_field = self._field('status')

        sql = f"SELECT * FROM `{self.source_table}` WHERE {location_field} = @location"
        params = [db.bigquery.ScalarQueryParameter("location", "STRING", hris_location)]

        if active_only:
            sql += f" AND {status_field} IN UNNEST(@statuses)"
            params.append(db.bigquery.ArrayQueryParameter("statuses", "STRING", list(self._active_statuses())))

        sql += f" ORDER BY {self._field('last_name')}, {self._field('first_name')}"

        rows = db.query(sql, params)
        return [self._standardize_row(r) for r in rows]

    def get_distinct_titles(self):
        title_field = self._field('title')
        status_field = self._field('status')
        sql = f"""
            SELECT DISTINCT {title_field} as title
            FROM `{self.source_table}`
            WHERE {status_field} IN UNNEST(@statuses)
            AND {title_field} IS NOT NULL AND {title_field} != ''
            ORDER BY title
        """
        params = [db.bigquery.ArrayQueryParameter("statuses", "STRING", list(self._active_statuses()))]
        return [r['title'] for r in db.query(sql, params)]

    def get_distinct_locations(self):
        location_field = self._field('location')
        sql = f"""
            SELECT DISTINCT {location_field} as location
            FROM `{self.source_table}`
            WHERE {location_field} IS NOT NULL AND {location_field} != ''
            ORDER BY location
        """
        return [r['location'] for r in db.query(sql)]

    def get_distinct_departments(self):
        dept_field = self._field('department')
        sql = f"""
            SELECT DISTINCT {dept_field} as department
            FROM `{self.source_table}`
            WHERE {dept_field} IS NOT NULL AND {dept_field} != ''
            ORDER BY department
        """
        return [r['department'] for r in db.query(sql)]

    def get_staff_count_by_school(self, active_only=True):
        location_field = self._field('location')
        status_field = self._field('status')

        sql = f"""
            SELECT {location_field} as location, COUNT(*) as count
            FROM `{self.source_table}`
        """
        params = []
        if active_only:
            sql += f" WHERE {status_field} IN UNNEST(@statuses)"
            params.append(db.bigquery.ArrayQueryParameter("statuses", "STRING", list(self._active_statuses())))
        sql += f" GROUP BY {location_field} ORDER BY {location_field}"

        rows = db.query(sql, params or None)
        result = {}
        for r in rows:
            school_name = self._normalize_location(r['location'])
            if school_name:
                result[school_name] = result.get(school_name, 0) + r['count']
        return result
