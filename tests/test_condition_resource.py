import pandas as pd
from pandas.testing import assert_frame_equal
import os
import datetime
from fhirflat.resources.condition import Condition
import pytest
from pydantic.v1 import ValidationError

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
        {
            "url": "timingPhaseDetail",
            "extension": [
                {
                    "url": "timingPhase",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "281379000",
                                "display": "pre-admission",
                            }
                        ]
                    },
                },
                {
                    "url": "timingDetail",
                    "valueRange": {
                        "low": {"value": -7, "unit": "days"},
                        "high": {"value": 0, "unit": "days"},
                    },
                },
            ],
        },
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
    "resourceType": "Condition",
    "extension.presenceAbsence.code": ["http://snomed.info/sct|410605003"],
    "extension.presenceAbsence.text": ["Present"],
    "extension.prespecifiedQuery": True,
    "extension.timingPhaseDetail.timingPhase.code": [
        "http://snomed.info/sct|281379000"
    ],
    "extension.timingPhaseDetail.timingPhase.text": ["pre-admission"],
    "extension.timingPhaseDetail.timingDetail.low.value": -7,
    "extension.timingPhaseDetail.timingDetail.low.unit": "days",
    "extension.timingPhaseDetail.timingDetail.high.value": 0,
    "extension.timingPhaseDetail.timingDetail.high.unit": "days",
    "category.code": [
        "http://snomed.info/sct|55607006",
        "http://terminology.hl7.org/CodeSystem/condition-category|problem-list-item",  # noqa: E501
    ],
    "category.text": ["Problem", None],
    "bodySite.code": ["http://snomed.info/sct|38266002"],
    "bodySite.text": "whole body",
    "onsetDateTime": datetime.date(2013, 4, 2),
    "abatementString": "around April 9, 2013",
    "recordedDate": datetime.date(2013, 4, 4),
    "severity.code": ["http://snomed.info/sct|255604002"],
    "severity.text": ["Mild"],
    "code.code": ["http://snomed.info/sct|386661006"],
    "code.text": "Fever",
    "subject": "Patient/f201",
    "encounter": "Encounter/f201",
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
        {
            "url": "timingPhaseDetail",
            "extension": [
                {
                    "url": "timingDetail",
                    "valueRange": {
                        "low": {"value": -7, "unit": "days"},
                        "high": {"value": 0, "unit": "days"},
                    },
                },
                {
                    "url": "timingPhase",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "281379000",
                                "display": "pre-admission",
                            }
                        ]
                    },
                },
            ],
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

    fever_flat = pd.read_parquet("test_condition.parquet")
    expected = pd.DataFrame([CONDITION_FLAT], index=[0])
    expected = expected.reindex(sorted(expected.columns), axis=1)
    # v, e = Condition.validate_fhirflat(expected)

    assert_frame_equal(fever_flat, expected, check_dtype=False)
    os.remove("test_condition.parquet")


def test_condition_from_flat():
    fever = Condition(**CONDITION_DICT_OUT)

    flat_fever = Condition.from_flat("tests/data/condition_flat.parquet")

    assert fever == flat_fever


def test_condition_extension_validation_error():
    with pytest.raises(ValueError, match="can only appear once"):
        Condition(
            **{
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
                "subject": {"reference": "Patient/f201"},
                "clinicalStatus": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",  # noqa: E501
                            "code": "resolved",
                        }
                    ]
                },
            }
        )


def test_from_flat_validation_error_single():
    with pytest.raises(ValidationError, match="1 validation error for Condition"):
        Condition.from_flat("tests/data/condition_flat_missing_subject.parquet")


@pytest.mark.usefixtures(
    "raises_phase_plus_detail_error", "raises_phase_duplicate_error"
)
def test_extension_raises_errors(
    raises_phase_plus_detail_error, raises_phase_duplicate_error
):
    fhir_input = {
        "id": "c201",
        "subject": {"reference": "Patient/f201"},
        "clinicalStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",  # noqa: E501
                    "code": "resolved",
                }
            ]
        },
    }
    raises_phase_plus_detail_error(fhir_input, Condition)
    raises_phase_duplicate_error(fhir_input, Condition)
