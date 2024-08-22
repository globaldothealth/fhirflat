import fhirflat.flat2fhir as f2f
import pytest
from fhir.resources.encounter import Encounter
from fhirflat.resources.extensions import timingPhaseDetail


@pytest.mark.parametrize(
    "data_groups, expected",
    [
        (
            (
                {"code.code": ["http://loinc.org|1234"], "code.text": ["Test"]},
                "code",
            ),
            {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "1234",
                        "display": "Test",
                    }
                ]
            },
        ),
        (
            (
                {
                    "code.code": [
                        "http://loinc.org|1234",
                        "http://snomed.info/sct|5678",
                    ],
                    "code.text": ["Test", "Snomed Test"],
                },
                "code",
            ),
            {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "1234",
                        "display": "Test",
                    },
                    {
                        "system": "http://snomed.info/sct",
                        "code": "5678",
                        "display": "Snomed Test",
                    },
                ]
            },
        ),
        (
            (
                {"code.code": [], "code.text": ["Test"]},
                "code",
            ),
            {
                "coding": [
                    {
                        "display": "Test",
                    }
                ]
            },
        ),
        (
            (
                {"concept.text": ["Test"]},
                "concept",
            ),
            {
                "text": "Test",
            },
        ),
    ],
)
def test_create_codeable_concept(data_groups, expected):
    data, groups = data_groups
    result = f2f.create_codeable_concept(data, groups)

    assert result == expected


@pytest.mark.parametrize(
    "data_groups, expected",
    [
        (
            (
                {
                    "code.code": ["1234"],
                    "code.system": ["http://loinc.org"],
                    "code.text": ["Test"],
                },
                "code",
            ),
            {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "1234",
                        "display": "Test",
                    }
                ]
            },
        ),
        (
            (
                {
                    "code.code": ["1234", "5678"],
                    "code.system": ["http://loinc.org", "http://snomed.info/sct"],
                    "code.text": ["Test", "Snomed Test"],
                },
                "code",
            ),
            {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "1234",
                        "display": "Test",
                    },
                    {
                        "system": "http://snomed.info/sct",
                        "code": "5678",
                        "display": "Snomed Test",
                    },
                ]
            },
        ),
    ],
)
def test_create_codeable_concept_ingestion(data_groups, expected):
    data, groups = data_groups
    result = f2f.create_codeable_concept(data, groups)

    assert result == expected


@pytest.mark.parametrize(
    "data_class, expected",
    [
        (
            (
                {
                    "admission.admitSource.code": ["http://snomed.info/sct|309902002"],
                    "admission.admitSource.text": ["Clinical Oncology Department"],
                    "admission.destination": {"reference": "Location/2"},
                    "admission.origin": {"reference": "Location/2"},
                },
                Encounter,
            ),
            {
                "admission": {
                    "admitSource": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "309902002",
                                "display": "Clinical Oncology Department",
                            }
                        ]
                    },
                    "destination": {"reference": "Location/2"},
                    "origin": {"reference": "Location/2"},
                }
            },
        ),
    ],
)
def test_expand_concepts(data_class, expected):
    data, data_class = data_class
    result = f2f.expand_concepts(data, data_class)

    assert result == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            {
                "timingPhaseDetail.timingPhase.code": [
                    "http://snomed.info/sct|281379000"
                ],
                "timingPhaseDetail.timingPhase.text": ["pre-admission"],
            },
            {
                "timingPhase.code": ["http://snomed.info/sct|281379000"],
                "timingPhase.text": ["pre-admission"],
            },
        ),
    ],
)
def test_step_down(data, expected):
    assert f2f.step_down(data) == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            (
                "doseQuantity",
                {
                    "doseQuantity.value": 5,
                    "doseQuantity.code": "http://unitsofmeasure.org|mg",
                },
            ),
            {
                "value": 5,
                "system": "http://unitsofmeasure.org",
                "code": "mg",
            },
        ),
    ],
)
def test_create_quantity(data, expected):
    group_name, data = data
    assert f2f.create_quantity(data, group_name) == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            (
                "approximateDate",
                "month 3",
            ),
            {"url": "approximateDate", "valueString": "month 3"},
        ),
        (
            (
                "birthSex",
                {"code": ["http://snomed.info/sct|1234"], "text": ["female"]},
            ),
            {
                "url": "birthSex",
                "valueCodeableConcept": {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "code": "1234",
                            "display": "female",
                        }
                    ]
                },
            },
        ),
        (
            (
                "timingDetail",
                {
                    "low.value": -7,
                    "low.unit": "days",
                    "high.value": 0,
                    "high.unit": "days",
                },
            ),
            {
                "url": "timingDetail",
                "valueRange": {
                    "low": {"value": -7, "unit": "days"},
                    "high": {"value": 0, "unit": "days"},
                },
            },
        ),
    ],
)
def test_create_single_extension(data, expected):
    k, v = data
    assert f2f.create_single_extension(k, v) == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            (
                "timingPhaseDetail",
                {
                    "timingDetail.high.unit": "days",
                    "timingDetail.high.value": 0.0,
                    "timingDetail.low.unit": "days",
                    "timingDetail.low.value": -7.0,
                    "timingPhase.code": ["http://snomed.info/sct|281379000"],
                    "timingPhase.text": ["pre-admission"],
                },
                timingPhaseDetail,
            ),
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
        ),
        (
            (
                "timingPhaseDetail",
                {
                    "timingDetail": "ever",
                    "timingPhase.code": ["http://snomed.info/sct|281379000"],
                    "timingPhase.text": ["pre-admission"],
                },
                timingPhaseDetail,
            ),
            {
                "url": "timingPhaseDetail",
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
                        "url": "timingDetail",
                        "valueString": "ever",
                    },
                ],
            },
        ),
    ],
)
def test_create_extension(data, expected):
    k, v_dict, klass = data
    f2f.create_extension(k, v_dict, klass) == expected
