import pandas as pd
import fhirflat.fhir2flat as f2f
import pytest


@pytest.mark.parametrize(
    "data_col, expected",
    [
        (
            (
                pd.DataFrame({"column1": [{"key1": "value1", "key2": "value2"}]}),
                "column1",
            ),
            {"column1.key1": ["value1"], "column1.key2": ["value2"]},
        ),
        (
            (pd.DataFrame({"alias": ["AUMC"]}), "alias"),
            pd.DataFrame({"alias": ["AUMC"]}),
        ),
    ],
)
def test_flatten_column(data_col, expected):
    # Create a mock DataFrame
    data, col_name = data_col

    # Call the function
    result = f2f.flatten_column(data, col_name)

    # Check the result
    expected = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(result, expected)


def test_flatten_column_type_error():
    data = {"column1": [{"key1": "value1", "key2": "value2"}]}
    with pytest.raises(
        TypeError, match="Input data must be a pandas DataFrame or Series."
    ):
        f2f.flatten_column(data, "column1")


@pytest.mark.parametrize(
    "data_lists, expected",
    [
        (
            (
                {
                    "reason": [
                        [{"value": [{"concept": {"text": "bilateral pneumonia."}}]}]
                    ],
                },
                ["reason"],
            ),
            {
                "reason.value.concept.text": ["bilateral pneumonia."],
            },
        )
    ],
)
def test_explode_and_flatten_no_multiples(data_lists, expected):
    # Create a mock DataFrame
    data, lists = data_lists
    df = pd.DataFrame(data, index=[0])

    # Call the function
    result = f2f.explode_and_flatten(df, lists)

    # Check the result
    expected = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            {
                "code.coding": [
                    [{"system": "http://loinc.org", "code": "1234", "display": "Test"}]
                ]
            },
            {"code.code": [["http://loinc.org|1234"]], "code.text": [["Test"]]},
        ),
        (
            {
                "code.coding": [
                    [{"system": "http://loinc.org", "code": "1234", "display": "Test"}]
                ],
                "code.text": ["Test text"],
            },
            {"code.code": [["http://loinc.org|1234"]], "code.text": ["Test text"]},
        ),
        (
            {
                "code.coding": [
                    [
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
                ]
            },
            {
                "code.code": [["http://loinc.org|1234", "http://snomed.info/sct|5678"]],
                "code.text": [["Test", "Snomed Test"]],
            },
        ),
        (
            {"code.coding": [[{"display": "Test"}]]},
            {"code.code": [[]], "code.text": [["Test"]]},
        ),
    ],
)
def test_condenseCoding(data, expected):
    # Create a mock DataFrame
    df = pd.DataFrame(data)

    # Call the function
    result = f2f.condenseCoding(df, "code.coding")

    # Check the result
    expected = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"subject.reference": ["Patient/example"]}, {"subject": ["Patient/example"]}),
        (
            {
                "subject.reference": ["Patient/example"],
                "subject.display": ["Donald Duck"],
            },
            {"subject": ["Patient/example"]},
        ),
    ],
)
def test_condenseReference(data, expected):
    # Create a mock DataFrame
    df = pd.DataFrame(data)

    # Call the function
    result = f2f.condenseReference(df, "subject.reference")

    # Check the result
    expected = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(result, expected)
