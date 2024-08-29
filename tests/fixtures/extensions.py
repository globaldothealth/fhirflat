import pytest


@pytest.fixture
def raises_phase_plus_detail_error():
    def _ext_test(fhir_input, resource):
        fhir_input["extension"] = [
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
        ]
        source = fhir_input
        with pytest.raises(ValueError, match="cannot appear together"):
            resource(**source)

    return _ext_test


@pytest.fixture
def raises_phase_duplicate_error():
    def _ext_test(fhir_input, resource):
        fhir_input["extension"] = [
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
        ]
        source = fhir_input
        with pytest.raises(ValueError, match="can only appear once"):
            resource(**source)

    return _ext_test
