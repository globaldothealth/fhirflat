import pytest
from pytest_unordered import unordered
import fhirflat
from fhirflat.util import (
    group_keys,
    get_fhirtype,
    get_local_extension_type,
    get_local_resource,
    condense_codes,
)
from fhir.resources.quantity import Quantity
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.medicationstatement import MedicationStatementAdherence
from fhir.resources.immunization import ImmunizationProtocolApplied

from fhirflat.resources.extensions import dateTimeExtension, Duration
from fhirflat import MedicationStatement
import pandas as pd
import numpy as np


def test_group_keys():
    data = [
        "code.code",
        "code.text",
        "status",
        "class.code",
        "class.text",
        "priority.code",
        "priority.text",
        "type.code",
        "type.text",
        "participant.type.code",
        "participant.actor.reference",
    ]
    result = group_keys(data)

    assert result == {
        "code": unordered(["code.code", "code.text"]),
        "class": unordered(["class.code", "class.text"]),
        "priority": unordered(["priority.code", "priority.text"]),
        "type": unordered(["type.code", "type.text"]),
        "participant": unordered(
            ["participant.type.code", "participant.actor.reference"]
        ),
    }


@pytest.mark.parametrize(
    "input, expected",
    [
        ("Quantity", Quantity),
        ("CodeableConcept", CodeableConcept),
        ("MedicationStatementAdherence", MedicationStatementAdherence),
        ("dateTimeExtension", dateTimeExtension),
        ("duration", Duration),
        ("ImmunizationProtocolApplied", ImmunizationProtocolApplied),
    ],
)
def test_get_fhirtype(input, expected):
    result = get_fhirtype(input)
    assert result == expected


def test_get_fhirtype_import():
    # if 'Extension' is imported from fhir.resources.extension in this file the test
    # doesn't hit correct test point
    result = get_fhirtype("Extension")
    assert result.__module__ == "fhir.resources.extension"


def test_get_fhirtype_raises():
    with pytest.raises(AttributeError):
        get_fhirtype("NotARealType")


def test_get_local_extension_type_raises():
    with pytest.raises(
        AttributeError, match="Could not find NotARealType in fhirflat extensions"
    ):
        get_local_extension_type("NotARealType")


def test_get_local_resource():
    result = get_local_resource("Patient")
    assert result == fhirflat.Patient


def test_get_local_resource_case_insensitive():
    result = get_local_resource("medicationstatement", case_insensitive=True)
    assert result == MedicationStatement


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            (
                pd.Series(
                    {
                        "test.system": "http://loinc.org",
                        "test.code": "1234",
                        "test.display": "Test",
                    }
                ),
                "test",
            ),
            pd.Series(
                {
                    "test.system": "http://loinc.org",
                    "test.code": "http://loinc.org|1234",
                    "test.display": "Test",
                }
            ),
        ),
        (
            (
                pd.Series(
                    {
                        "test.system": "http://loinc.org",
                        "test.code": np.nan,
                        "test.display": "Test",
                    }
                ),
                "test",
            ),
            pd.Series(
                {
                    "test.system": "http://loinc.org",
                    "test.code": None,
                    "test.display": "Test",
                }
            ),
        ),
        (
            (
                pd.Series(
                    {
                        "test.system": ["http://loinc.org", "http://snomed.info/sct"],
                        "test.code": ["1234", 5678],
                        "test.display": "Test",
                    }
                ),
                "test",
            ),
            pd.Series(
                {
                    "test.system": ["http://loinc.org", "http://snomed.info/sct"],
                    "test.code": [
                        "http://loinc.org|1234",
                        "http://snomed.info/sct|5678",
                    ],
                    "test.display": "Test",
                }
            ),
        ),
    ],
)
def test_condense_codes(input, expected):
    row, col = input
    result = condense_codes(row, col)

    pd.testing.assert_series_equal(result, expected)
