"""
fhirflat is a library for transforming FHIR resources in NDJSON or native Python
dictionaries to a flat structure that can be written to a Parquet file.
"""

from .resources import (
    Condition,
    Encounter,
    Immunization,
    Location,
    MedicationAdministration,
    MedicationStatement,
    Observation,
    Organization,
    Patient,
    Procedure,
    ResearchSubject,
    Specimen,
)
from .ingest import convert_data_to_flat

# Update this when bumping version in pyproject.toml!
__version__ = "0.1.0"
__all__ = ["convert_data_to_flat"]
