import pandas as pd
from pandas.testing import assert_frame_equal
import os
from fhirflat.resources.medicationadministration import MedicationAdministration
import datetime
import pytest

MEDS_DICT_INPUT = {
    "resourceType": "MedicationAdministration",
    "id": "medadmin0306",
    "contained": [
        {
            "resourceType": "Medication",
            "id": "med0306",
            "code": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "324252006",
                        "display": "Azithromycin 250mg capsule (product)",
                    }
                ]
            },
        }
    ],
    "partOf": [{"reference": "Medication/med0305"}],
    "status": "completed",
    "medication": {"reference": {"reference": "#med0306"}},
    "subject": {"reference": "Patient/pat1", "display": "Donald Duck"},
    "encounter": {
        "reference": "Encounter/f001",
        "display": "encounter who leads to this prescription",
    },
    "occurencePeriod": {
        "start": "2015-01-15T04:30:00+01:00",
        "end": "2015-01-15T14:30:00+01:00",
    },
    "performer": [
        {
            "function": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/med-admin-perform-function",  # noqa:E501
                        "code": "performer",
                        "display": "Performer",
                    }
                ]
            },
            "actor": {
                "reference": {
                    "reference": "Practitioner/f007",
                    "display": "Patrick Pump",
                }
            },
        }
    ],
    "request": {"reference": "MedicationRequest/medrx0302"},
    "dosage": {
        "text": "Two tablets at once",
        "route": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "26643006",
                    "display": "Oral Route",
                }
            ]
        },
        "method": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "421521009",
                    "display": "Swallow - dosing instruction imperative (qualifier value)",  # noqa:E501
                }
            ]
        },
        "dose": {
            "value": 2,
            "unit": "TAB",
            "system": "http://terminology.hl7.org/CodeSystem/v3-orderableDrugForm",
            "code": "TAB",
        },
    },
}

MEDS_FLAT = {
    "resourceType": "MedicationAdministration",
    "medication.reference": "#med0306",
    "partOf": "Medication/med0305",
    "subject": "Patient/pat1",
    "encounter": "Encounter/f001",
    "occurencePeriod.start": datetime.datetime(
        2015, 1, 15, 4, 30, tzinfo=datetime.timezone(datetime.timedelta(minutes=60))
    ),
    "occurencePeriod.end": datetime.datetime(
        2015, 1, 15, 14, 30, tzinfo=datetime.timezone(datetime.timedelta(minutes=60))
    ),
    "request": "MedicationRequest/medrx0302",
    "dosage.text": "Two tablets at once",
    "dosage.route.code": "http://snomed.info/sct|26643006",
    "dosage.route.text": "Oral Route",
    "dosage.method.code": "http://snomed.info/sct|421521009",
    "dosage.method.text": "Swallow - dosing instruction imperative (qualifier value)",  # noqa:E501
    "dosage.dose.value": 2,
    "dosage.dose.unit": "TAB",
    "dosage.dose.code": "http://terminology.hl7.org/CodeSystem/v3-orderableDrugForm|TAB",  # noqa:E501
}

MEDS_DICT_OUT = {
    "resourceType": "MedicationAdministration",
    "status": "completed",
    "medication": {"reference": {"reference": "#med0306"}},
    "partOf": [{"reference": "Medication/med0305"}],
    "subject": {"reference": "Patient/pat1"},
    "encounter": {"reference": "Encounter/f001"},
    "occurencePeriod": {
        "start": "2015-01-15T04:30:00+01:00",
        "end": "2015-01-15T14:30:00+01:00",
    },
    "request": {"reference": "MedicationRequest/medrx0302"},
    "dosage": {
        "text": "Two tablets at once",
        "route": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "26643006",
                    "display": "Oral Route",
                }
            ]
        },
        "method": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "421521009",
                    "display": "Swallow - dosing instruction imperative (qualifier value)",  # noqa:E501
                }
            ]
        },
        "dose": {
            "value": 2,
            "unit": "TAB",
            "system": "http://terminology.hl7.org/CodeSystem/v3-orderableDrugForm",
            "code": "TAB",
        },
    },
}


def test_medicationadministration_to_flat():
    med = MedicationAdministration(**MEDS_DICT_INPUT)

    med.to_flat("test_medicationadmin.parquet")

    assert_frame_equal(
        pd.read_parquet("test_medicationadmin.parquet"),
        pd.DataFrame(MEDS_FLAT, index=[0]),
        # Date types are off otherwise, pyarrow uses pytz and pandas uses dateutil
        check_dtype=False,
        check_like=True,
    )
    os.remove("test_medicationadmin.parquet")


def test_medicationadministration_from_flat():
    meds = MedicationAdministration(**MEDS_DICT_OUT)

    flat_meds = MedicationAdministration.from_flat(
        "tests/data/medicationadmin_flat.parquet"
    )

    assert meds == flat_meds


@pytest.mark.usefixtures(
    "raises_phase_plus_detail_error", "raises_phase_duplicate_error"
)
def test_extension_raises_errors(
    raises_phase_plus_detail_error, raises_phase_duplicate_error
):
    fhir_input = {
        "resourceType": "MedicationAdministration",
        "status": "completed",
        "medication": {"reference": {"reference": "#med0306"}},
        "subject": {"reference": "Patient/pat1"},
        "occurencePeriod": {
            "start": "2015-01-15T04:30:00+01:00",
            "end": "2015-01-15T14:30:00+01:00",
        },
    }
    raises_phase_plus_detail_error(fhir_input, MedicationAdministration)
    raises_phase_duplicate_error(fhir_input, MedicationAdministration)
