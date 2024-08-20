import pandas as pd
from pandas.testing import assert_frame_equal
import os
from fhirflat.resources.encounter import Encounter
import datetime
import pytest

ENCOUNTER_DICT_INPUT = {
    "resourceType": "Encounter",
    "id": "f203",
    "identifier": [{"use": "temp", "value": "Encounter_Roel_20130311"}],
    "status": "completed",
    "extension": [
        {
            "url": "timingPhase",
            "valueCodeableConcept": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": 278307001,
                        "display": "on admission",
                    }
                ]
            },
        },
        {
            "url": "relativePeriod",
            "extension": [
                {"url": "relativeStart", "valueInteger": 2},
                {"url": "relativeEnd", "valueInteger": 5},
            ],
        },
    ],
    "class": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                    "code": "IMP",
                    "display": "inpatient encounter",
                }
            ]
        }
    ],
    "priority": {
        "coding": [
            {
                "system": "http://snomed.info/sct",
                "code": "394849002",
                "display": "High priority",
            }
        ]
    },
    "type": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "183807002",
                    "display": "Inpatient stay for nine days",
                }
            ]
        }
    ],
    "subject": {"reference": "Patient/f201", "display": "Roel"},
    "episodeOfCare": [{"reference": "EpisodeOfCare/example"}],
    "basedOn": [{"reference": "ServiceRequest/myringotomy"}],
    "partOf": {"reference": "Encounter/f203"},
    "serviceProvider": {"reference": "Organization/2"},
    "participant": [
        {
            "type": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",  # noqa: E501
                            "code": "PART",
                        }
                    ]
                }
            ],
            "actor": {"reference": "Practitioner/f201"},
        }
    ],
    "appointment": [{"reference": "Appointment/example"}],
    "actualPeriod": {"start": "2013-03-11", "end": "2013-03-20"},
    "reason": [
        {
            "value": [
                {
                    "concept": {
                        "text": "The patient seems to suffer from bilateral pneumonia and renal insufficiency, most likely due to chemotherapy."  # noqa: E501
                    }
                }
            ]
        }
    ],
    "diagnosis": [
        {
            "condition": [{"reference": {"reference": "Condition/stroke"}}],
            "use": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/diagnosis-role",  # noqa: E501
                            "code": "AD",
                            "display": "Admission diagnosis",
                        }
                    ]
                }
            ],
        },
        {
            "condition": [{"reference": {"reference": "Condition/f201"}}],
            "use": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/diagnosis-role",  # noqa: E501
                            "code": "DD",
                            "display": "Discharge diagnosis",
                        }
                    ]
                }
            ],
        },
    ],
    "account": [{"reference": "Account/example"}],
    "dietPreference": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "276026009",
                    "display": "Fluid balance regulation",
                }
            ]
        }
    ],
    "specialArrangement": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/encounter-special-arrangements",  # noqa: E501
                    "code": "wheel",
                    "display": "Wheelchair",
                }
            ]
        }
    ],
    "specialCourtesy": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-EncounterSpecialCourtesy",  # noqa: E501
                    "code": "NRM",
                    "display": "normal courtesy",
                }
            ]
        }
    ],
    "admission": {
        "origin": {"reference": "Location/2"},
        "admitSource": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "309902002",
                    "display": "Clinical Oncology Department",
                }
            ]
        },
        "reAdmission": {"coding": [{"display": "readmitted"}]},
        "destination": {"reference": "Location/2"},
    },
}

ENCOUNTER_FLAT = {
    "resourceType": "Encounter",
    "id": "f203",
    "extension.timingPhase.code": "http://snomed.info/sct|278307001",
    "extension.timingPhase.text": "on admission",
    "extension.relativePeriod.relativeStart": 2,
    "extension.relativePeriod.relativeEnd": 5,
    "class.code": "http://terminology.hl7.org/CodeSystem/v3-ActCode|IMP",
    "class.text": "inpatient encounter",
    "type.code": "http://snomed.info/sct|183807002",
    "type.text": "Inpatient stay for nine days",
    "episodeOfCare": "EpisodeOfCare/example",
    "basedOn": "ServiceRequest/myringotomy",
    "reason.value.concept.text": "The patient seems to suffer from bilateral pneumonia and renal insufficiency, most likely due to chemotherapy.",  # noqa: E501
    "priority.code": "http://snomed.info/sct|394849002",
    "priority.text": "High priority",
    "subject": "Patient/f201",
    "partOf": "Encounter/f203",
    "serviceProvider": "Organization/2",
    "actualPeriod.start": datetime.date(2013, 3, 11),
    "actualPeriod.end": datetime.date(2013, 3, 20),
    "diagnosis_dense": [
        [
            {
                "condition": [{"reference": {"reference": "Condition/stroke"}}],
                "use": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/diagnosis-role",  # noqa: E501
                                "code": "AD",
                                "display": "Admission diagnosis",
                            }
                        ]
                    }
                ],
            },
            {
                "condition": [{"reference": {"reference": "Condition/f201"}}],
                "use": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/diagnosis-role",  # noqa: E501
                                "code": "DD",
                                "display": "Discharge diagnosis",
                            }
                        ]
                    }
                ],
            },
        ]
    ],
    "admission.origin": "Location/2",
    "admission.admitSource.code": "http://snomed.info/sct|309902002",
    "admission.admitSource.text": "Clinical Oncology Department",
    "admission.reAdmission.code": [[]],
    "admission.reAdmission.text": "readmitted",
    "admission.destination": "Location/2",
}

ENCOUNTER_DICT_OUT = {
    "resourceType": "Encounter",
    "id": "f203",
    "status": "completed",
    "extension": [
        {
            "url": "relativePeriod",
            "extension": [
                {"url": "relativeEnd", "valueInteger": 5},
                {"url": "relativeStart", "valueInteger": 2},
            ],
        },
        {
            "url": "timingPhase",
            "valueCodeableConcept": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": 278307001,
                        "display": "on admission",
                    }
                ]
            },
        },
    ],
    "class": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                    "code": "IMP",
                    "display": "inpatient encounter",
                }
            ]
        }
    ],
    "priority": {
        "coding": [
            {
                "system": "http://snomed.info/sct",
                "code": "394849002",
                "display": "High priority",
            }
        ]
    },
    "type": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "183807002",
                    "display": "Inpatient stay for nine days",
                }
            ]
        }
    ],
    "subject": {"reference": "Patient/f201"},
    "episodeOfCare": [{"reference": "EpisodeOfCare/example"}],
    "basedOn": [{"reference": "ServiceRequest/myringotomy"}],
    "partOf": {"reference": "Encounter/f203"},
    "serviceProvider": {"reference": "Organization/2"},
    "actualPeriod": {"start": "2013-03-11T00:00:00", "end": "2013-03-20T00:00:00"},
    "reason": [
        {
            "value": [
                {
                    "concept": {
                        "text": "The patient seems to suffer from bilateral pneumonia and renal insufficiency, most likely due to chemotherapy."  # noqa: E501
                    }
                }
            ]
        }
    ],
    "diagnosis": [
        {
            "condition": [{"reference": {"reference": "Condition/stroke"}}],
            "use": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/diagnosis-role",  # noqa: E501
                            "code": "AD",
                            "display": "Admission diagnosis",
                        }
                    ]
                }
            ],
        },
        {
            "condition": [{"reference": {"reference": "Condition/f201"}}],
            "use": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/diagnosis-role",  # noqa: E501
                            "code": "DD",
                            "display": "Discharge diagnosis",
                        }
                    ]
                }
            ],
        },
    ],
    "admission": {
        "origin": {"reference": "Location/2"},
        "admitSource": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "309902002",
                    "display": "Clinical Oncology Department",
                }
            ]
        },
        "reAdmission": {"coding": [{"display": "readmitted"}]},
        "destination": {"reference": "Location/2"},
    },
}


def test_encounter_to_flat():
    bp = Encounter(**ENCOUNTER_DICT_INPUT)

    bp.to_flat("test_encounter.parquet")

    assert_frame_equal(
        pd.read_parquet("test_encounter.parquet"),
        pd.DataFrame(ENCOUNTER_FLAT, index=[0]),
        # Date types are off otherwise, pyarrow uses pytz and pandas uses dateutil
        check_dtype=False,
        check_like=True,  # ignore column order
    )
    os.remove("test_encounter.parquet")


def test_encounter_from_flat():
    visit = Encounter(**ENCOUNTER_DICT_OUT)

    flat_visit = Encounter.from_flat("tests/data/encounter_flat.parquet")

    assert visit == flat_visit


def test_encounter_extension_validation_error():
    with pytest.raises(ValueError, match="can only appear once"):
        Encounter(
            **{
                "status": "active",
                "extension": [
                    {
                        "url": "timingPhase",
                        "valueCodeableConcept": {
                            "coding": [
                                {
                                    "system": "http://snomed.info/sct",
                                    "code": 278307001,
                                    "display": "on admission",
                                }
                            ]
                        },
                    },
                    {
                        "url": "timingPhase",
                        "valueCodeableConcept": {
                            "coding": [
                                {
                                    "system": "http://snomed.info/sct",
                                    "code": 278307001,
                                    "display": "on admission",
                                }
                            ]
                        },
                    },
                ],
            }
        )


def test_from_flat_validation_error_multi_resources():
    with pytest.warns(UserWarning, match="Validation errors found in the data."):
        fhir_resources = Encounter.from_flat(
            "tests/data/multi_row_encounter_flat_errors.parquet"
        )
        assert len(fhir_resources) == 3

    errors = pd.read_csv("encounter_errors.csv")
    assert len(errors) == 1
    assert "invalid datetime format" in errors.iloc[0]["validation_error"]
    os.remove("encounter_errors.csv")


@pytest.mark.usefixtures(
    "raises_phase_plus_detail_error", "raises_phase_duplicate_error"
)
def test_extension_raises_errors(
    raises_phase_plus_detail_error, raises_phase_duplicate_error
):
    fhir_input = {
        "resourceType": "Encounter",
        "id": "f203",
        "status": "completed",
    }
    raises_phase_plus_detail_error(fhir_input, Encounter)
    raises_phase_duplicate_error(fhir_input, Encounter)
