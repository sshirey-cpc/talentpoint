"""
TalentPoint — HRIS adapter layer.

Abstracts HRIS data behind a standard interface so TalentPoint works
with any client's HR system. The adapter reads the HRIS source configuration
from the tenant config (table name, column mappings).

Current adapters:
  - BigQuery table (default) — reads from a BQ table loaded via ETL
  - CSV upload (future) — for clients without a BQ pipeline

To add a new adapter (e.g., Workday API, ADP API), implement the
HRISAdapter interface and register it in get_adapter().
"""

from abc import ABC, abstractmethod

import config


class HRISAdapter(ABC):
    """Interface for HRIS data access."""

    @abstractmethod
    def get_all_staff(self, active_only=True):
        """Return all staff as list of standardized dicts."""
        pass

    @abstractmethod
    def get_staff_by_email(self, email):
        """Return a single staff record by email, or None."""
        pass

    @abstractmethod
    def get_staff_by_school(self, school_name, active_only=True):
        """Return staff filtered by school/location."""
        pass

    @abstractmethod
    def get_distinct_titles(self):
        """Return sorted list of distinct job titles."""
        pass

    @abstractmethod
    def get_distinct_locations(self):
        """Return sorted list of distinct location names."""
        pass

    @abstractmethod
    def get_distinct_departments(self):
        """Return sorted list of distinct departments."""
        pass

    @abstractmethod
    def get_staff_count_by_school(self, active_only=True):
        """Return dict of school_name -> active staff count."""
        pass


def get_adapter():
    """Return the configured HRIS adapter for the current tenant."""
    adapter_type = config.tenant().get('hris_adapter_type', 'bigquery')

    if adapter_type == 'bigquery':
        from adapters.bigquery_hris import BigQueryHRISAdapter
        return BigQueryHRISAdapter()
    elif adapter_type == 'csv':
        from adapters.csv_hris import CSVHRISAdapter
        return CSVHRISAdapter()
    else:
        raise ValueError(f"Unknown HRIS adapter type: {adapter_type}")


# Convenience functions — use these throughout the app

def get_all_staff(active_only=True):
    return get_adapter().get_all_staff(active_only)


def get_staff_by_email(email):
    return get_adapter().get_staff_by_email(email)


def get_staff_by_school(school_name, active_only=True):
    return get_adapter().get_staff_by_school(school_name, active_only)


def get_distinct_titles():
    return get_adapter().get_distinct_titles()


def get_distinct_locations():
    return get_adapter().get_distinct_locations()


def get_distinct_departments():
    return get_adapter().get_distinct_departments()


def get_staff_count_by_school(active_only=True):
    return get_adapter().get_staff_count_by_school(active_only)
