import pandas as pd
from pandas.testing import assert_frame_equal
import os
import datetime
from fhirflat.resources.condition import Condition

CONDITION_DICT_INPUT = {
    "id": "c201",
    "extension": [
        {
            "url": "presenceAbsence",
            "valueCodeableConcept": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "410605003",
                        "display": "Present",
                    }
                ]
            },
        },
        {"url": "prespecifiedQuery", "valueBoolean": True},
    ],
    "identifier": [{"value": "12345"}],
    "clinicalStatus": {
        "coding": [
            {
                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                "code": "resolved",
            }
        ]
    },
    "verificationStatus": {
        "coding": [
            {
                "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                "code": "confirmed",
            }
        ]
    },
    "category": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "55607006",
                    "display": "Problem",
                },
                {
                    "system": (
                        "http://terminology.hl7.org/CodeSystem/condition-category"
                    ),
                    "code": "problem-list-item",
                },
            ]
        }
    ],
    "severity": {
        "coding": [
            {"system": "http://snomed.info/sct", "code": "255604002", "display": "Mild"}
        ]
    },
    "code": {
        "coding": [
            {
                "system": "http://snomed.info/sct",
                "code": "386661006",
                "display": "Fever",
            }
        ],
        "text": "Fever",
    },
    "bodySite": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "38266002",
                    "display": "Entire body as a whole",
                }
            ],
            "text": "whole body",
        }
    ],
    "subject": {"reference": "Patient/f201", "display": "Roel"},
    "encounter": {"reference": "Encounter/f201"},
    "onsetDateTime": "2013-04-02",
    "abatementString": "around April 9, 2013",
    "recordedDate": "2013-04-04",
    "evidence": [
        {
            "concept": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "258710007",
                        "display": "degrees C",
                    }
                ]
            },
            "reference": {"reference": "Observation/f202", "display": "Temperature"},
        }
    ],
}

CONDITION_FLAT = {
    "resourceType": ["Condition"],
    "extension.presenceAbsence.code": ["http://snomed.info/sct|410605003"],
    "extension.presenceAbsence.text": ["Present"],
    "extension.prespecifiedQuery": [True],
    "category.code": [
        [
            "http://snomed.info/sct|55607006",
            "http://terminology.hl7.org/CodeSystem/condition-category|problem-list-item",  # noqa: E501
        ]
    ],
    "category.text": [["Problem", None]],
    "bodySite.code": ["http://snomed.info/sct|38266002"],
    "bodySite.text": ["whole body"],
    "onsetDateTime": [datetime.date(2013, 4, 2)],
    "abatementString": ["around April 9, 2013"],
    "recordedDate": [datetime.date(2013, 4, 4)],
    "severity.code": ["http://snomed.info/sct|255604002"],
    "severity.text": ["Mild"],
    "code.code": ["http://snomed.info/sct|386661006"],
    "code.text": ["Fever"],
    "subject": ["Patient/f201"],
    "encounter": ["Encounter/f201"],
}

CONDITION_DICT_OUT = {
    "extension": [
        {"url": "prespecifiedQuery", "valueBoolean": True},
        {
            "url": "presenceAbsence",
            "valueCodeableConcept": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "410605003",
                        "display": "Present",
                    }
                ]
            },
        },
    ],
    "clinicalStatus": {
        "coding": [
            {
                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                "code": "unknown",
            }
        ]
    },
    "category": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "55607006",
                    "display": "Problem",
                },
                {
                    "system": (
                        "http://terminology.hl7.org/CodeSystem/condition-category"
                    ),
                    "code": "problem-list-item",
                },
            ]
        }
    ],
    "severity": {
        "coding": [
            {"system": "http://snomed.info/sct", "code": "255604002", "display": "Mild"}
        ]
    },
    "code": {
        "coding": [
            {
                "system": "http://snomed.info/sct",
                "code": "386661006",
                "display": "Fever",
            }
        ],
    },
    "bodySite": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "38266002",
                    "display": "whole body",
                }
            ],
        }
    ],
    "subject": {"reference": "Patient/f201"},
    "encounter": {"reference": "Encounter/f201"},
    "onsetDateTime": "2013-04-02T00:00:00",
    "abatementString": "around April 9, 2013",
    "recordedDate": "2013-04-04T00:00:00",
}


def test_condition_to_flat():
    fever = Condition(**CONDITION_DICT_INPUT)

    fever.to_flat("test_condition.parquet")

    assert_frame_equal(
        pd.read_parquet("test_condition.parquet"),
        pd.DataFrame(CONDITION_FLAT),
        check_like=True,
    )
    os.remove("test_condition.parquet")


def test_condition_from_flat():
    fever = Condition(**CONDITION_DICT_OUT)

    flat_fever = Condition.from_flat("tests/data/condition_flat.parquet")

    assert fever == flat_fever
