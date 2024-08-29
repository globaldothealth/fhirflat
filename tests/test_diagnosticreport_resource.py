import pandas as pd
from pandas.testing import assert_frame_equal
import os
from fhirflat.resources.diagnosticreport import DiagnosticReport
import datetime
import pytest


DICT_INPUT = {
    "resourceType": "DiagnosticReport",
    "id": "f001",
    "identifier": [
        {
            "use": "official",
            "system": "http://www.bmc.nl/zorgportal/identifiers/reports",
            "value": "nr1239044",
        }
    ],
    "extension": [
        {
            "url": "timingPhase",
            "valueCodeableConcept": {
                "coding": [{"system": "timing.com", "code": "1234"}]
            },
        },
    ],
    "basedOn": [{"reference": "ServiceRequest/req"}],
    "status": "final",
    "category": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "252275004",
                    "display": "Haematology test",
                },
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v2-0074",
                    "code": "HM",
                },
            ]
        }
    ],
    "code": {
        "coding": [
            {
                "system": "http://loinc.org",
                "code": "58410-2",
                "display": "Complete blood count (hemogram) panel - Blood by Automated count",  # noqa: E501
            }
        ]
    },
    "subject": {"reference": "Patient/f001", "display": "P. van den Heuvel"},
    "issued": "2013-05-15T19:32:52+01:00",
    "performer": [
        {
            "reference": "Organization/f001",
            "display": "Burgers University Medical Centre",
        }
    ],
    "result": [
        {"reference": "Observation/f001"},
        {"reference": "Observation/f002"},
        {"reference": "Observation/f003"},
        {"reference": "Observation/f004"},
        {"reference": "Observation/f005"},
    ],
    "conclusion": "Core lab",
}

FLAT = {
    "resourceType": "DiagnosticReport",
    "id": "f001",
    "basedOn": "ServiceRequest/req",
    "category.code": [
        "http://snomed.info/sct|252275004",
        "http://terminology.hl7.org/CodeSystem/v2-0074|HM",
    ],
    "category.text": ["Haematology test", None],
    "issued": datetime.datetime(
        2013,
        5,
        15,
        19,
        32,
        52,
        tzinfo=datetime.timezone(datetime.timedelta(minutes=60)),
    ),
    "performer": "Organization/f001",
    "result_dense": [
        {"reference": "Observation/f001"},
        {"reference": "Observation/f002"},
        {"reference": "Observation/f003"},
        {"reference": "Observation/f004"},
        {"reference": "Observation/f005"},
    ],
    "conclusion": "Core lab",
    "code.code": ["http://loinc.org|58410-2"],
    "code.text": ["Complete blood count (hemogram) panel - Blood by Automated count"],
    "subject": "Patient/f001",
    "extension.timingPhase.code": "timing.com|1234",
    "extension.timingPhase.text": None,
}

DICT_OUT = {
    "resourceType": "DiagnosticReport",
    "id": "f001",
    "extension": [
        {
            "url": "timingPhase",
            "valueCodeableConcept": {
                "coding": [{"system": "timing.com", "code": "1234"}]
            },
        },
    ],
    "basedOn": [{"reference": "ServiceRequest/req"}],
    "status": "final",
    "category": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "252275004",
                    "display": "Haematology test",
                },
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v2-0074",
                    "code": "HM",
                },
            ]
        }
    ],
    "code": {
        "coding": [
            {
                "system": "http://loinc.org",
                "code": "58410-2",
                "display": "Complete blood count (hemogram) panel - Blood by Automated count",  # noqa: E501
            }
        ]
    },
    "subject": {"reference": "Patient/f001"},
    "issued": "2013-05-15T19:32:52+01:00",
    "performer": [
        {
            "reference": "Organization/f001",
        }
    ],
    "result": [
        {"reference": "Observation/f001"},
        {"reference": "Observation/f002"},
        {"reference": "Observation/f003"},
        {"reference": "Observation/f004"},
        {"reference": "Observation/f005"},
    ],
    "conclusion": "Core lab",
}


def test_observation_to_flat():
    report = DiagnosticReport(**DICT_INPUT)

    report.to_flat("test_diagnosticreport.parquet")

    report_flat = pd.read_parquet("test_diagnosticreport.parquet")
    expected = pd.DataFrame([FLAT], index=[0])
    # v, e = DiagnosticReport.validate_fhirflat(expected)
    assert_frame_equal(report_flat, expected, check_dtype=False)

    os.remove("test_diagnosticreport.parquet")


def test_observation_from_flat():
    report = DiagnosticReport(**DICT_OUT)

    flat_report = DiagnosticReport.from_flat("tests/data/diagnosticreport_flat.parquet")

    assert report == flat_report


@pytest.mark.usefixtures(
    "raises_phase_plus_detail_error", "raises_phase_duplicate_error"
)
def test_extension_raises_errors(
    raises_phase_plus_detail_error, raises_phase_duplicate_error
):
    fhir_input = {
        "resourceType": "DiagnosticReport",
        "id": "f001",
        "status": "final",
        "code": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "58410-2",
                    "display": "Complete blood count (hemogram) panel - Blood by Automated count",  # noqa: E501
                }
            ]
        },
    }
    raises_phase_plus_detail_error(fhir_input, DiagnosticReport)
    raises_phase_duplicate_error(fhir_input, DiagnosticReport)
