import pandas as pd
from pandas.testing import assert_frame_equal
import os
from fhirflat.resources.medicationstatement import MedicationStatement
import datetime
import pytest

MEDS_DICT_INPUT = {
    "resourceType": "MedicationStatement",
    "id": "example004",
    "status": "recorded",
    "medication": {
        "concept": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "27658006",
                    "display": "Amoxicillin (product)",
                }
            ]
        }
    },
    "subject": {"reference": "Patient/pat1", "display": "Donald Duck"},
    "effectiveDateTime": "2014-01-23",
    "dateAsserted": "2015-02-22",
    "informationSource": [{"reference": "Patient/pat1", "display": "Donald Duck"}],
    "note": [{"text": "Patient indicates they miss the occasional dose"}],
    "dosage": [
        {
            "text": "one capsule three times daily",
            "timing": {"repeat": {"frequency": 3, "period": 1, "periodUnit": "d"}},
            "asNeeded": False,
            "route": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "260548002",
                        "display": "Oral",
                    }
                ]
            },
            "maxDosePerPeriod": [
                {
                    "numerator": {
                        "value": 3,
                        "unit": "capsules",
                        "system": "http://snomed.info/sct",
                        "code": "385055001",
                    },
                    "denominator": {
                        "value": 1,
                        "system": "http://unitsofmeasure.org",
                        "code": "d",
                    },
                }
            ],
        }
    ],
    "adherence": {
        "code": {
            "coding": [
                {
                    "system": "http://hl7.org/fhir/CodeSystem/medication-statement-adherence",  # noqa:E501
                    "code": "not-taking",
                    "display": "Not Taking",
                }
            ]
        },
        "reason": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "266710000",
                    "display": "Drugs not taken/completed",
                }
            ]
        },
    },
}

MEDS_FLAT = {
    "resourceType": "MedicationStatement",
    "effectiveDateTime": datetime.date(2014, 1, 23),
    "dateAsserted": datetime.date(2015, 2, 22),
    "dosage.text": "one capsule three times daily",
    "dosage.asNeeded": False,
    "dosage.maxDosePerPeriod.numerator.value": 3,
    "dosage.maxDosePerPeriod.numerator.unit": "capsules",
    "dosage.maxDosePerPeriod.numerator.code": "http://snomed.info/sct|385055001",
    "dosage.maxDosePerPeriod.denominator.value": 1,
    "dosage.maxDosePerPeriod.denominator.code": "http://unitsofmeasure.org|d",
    "dosage.timing.repeat.frequency": 3,
    "dosage.timing.repeat.period": 1,
    "dosage.timing.repeat.periodUnit": "d",
    "dosage.route.code": "http://snomed.info/sct|260548002",
    "dosage.route.text": "Oral",
    "medication.concept.code": "http://snomed.info/sct|27658006",
    "medication.concept.text": "Amoxicillin (product)",
    "subject": "Patient/pat1",
    "adherence.code.code": "http://hl7.org/fhir/CodeSystem/medication-statement-adherence|not-taking",  # noqa:E501
    "adherence.code.text": "Not Taking",
    "adherence.reason.code": "http://snomed.info/sct|266710000",
    "adherence.reason.text": "Drugs not taken/completed",
}

MEDS_DICT_OUT = {
    "resourceType": "MedicationStatement",
    "status": "recorded",
    "medication": {
        "concept": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "27658006",
                    "display": "Amoxicillin (product)",
                }
            ]
        }
    },
    "subject": {"reference": "Patient/pat1"},
    "effectiveDateTime": "2014-01-23T00:00:00",
    "dateAsserted": "2015-02-22T00:00:00",
    "dosage": [
        {
            "text": "one capsule three times daily",
            "timing": {"repeat": {"frequency": 3, "period": 1, "periodUnit": "d"}},
            "asNeeded": False,
            "route": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "260548002",
                        "display": "Oral",
                    }
                ]
            },
            "maxDosePerPeriod": [
                {
                    "numerator": {
                        "value": 3,
                        "unit": "capsules",
                        "system": "http://snomed.info/sct",
                        "code": "385055001",
                    },
                    "denominator": {
                        "value": 1,
                        "system": "http://unitsofmeasure.org",
                        "code": "d",
                    },
                }
            ],
        }
    ],
    "adherence": {
        "code": {
            "coding": [
                {
                    "system": "http://hl7.org/fhir/CodeSystem/medication-statement-adherence",  # noqa:E501
                    "code": "not-taking",
                    "display": "Not Taking",
                }
            ]
        },
        "reason": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "266710000",
                    "display": "Drugs not taken/completed",
                }
            ]
        },
    },
}


def test_medicationstatement_to_flat():
    med = MedicationStatement(**MEDS_DICT_INPUT)

    med.to_flat("test_medicationstat.parquet")

    assert_frame_equal(
        pd.read_parquet("test_medicationstat.parquet"),
        pd.DataFrame(MEDS_FLAT, index=[0]),
        # Date types are off otherwise, pyarrow uses pytz and pandas uses dateutil
        check_dtype=False,
    )
    os.remove("test_medicationstat.parquet")


def test_medicationstatement_from_flat():
    meds = MedicationStatement(**MEDS_DICT_OUT)

    flat_meds = MedicationStatement.from_flat("tests/data/medicationstat_flat.parquet")

    assert meds == flat_meds


@pytest.mark.usefixtures("raises_phase_duplicate_error")
def test_extension_raises_errors(raises_phase_duplicate_error):
    fhir_input = {
        "resourceType": "MedicationStatement",
        "status": "recorded",
        "medication": {
            "concept": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "27658006",
                        "display": "Amoxicillin (product)",
                    }
                ]
            }
        },
        "subject": {"reference": "Patient/pat1"},
    }
    raises_phase_duplicate_error(fhir_input, MedicationStatement)
