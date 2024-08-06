from fhirflat.ingest import (
    create_dictionary,
    convert_data_to_flat,
    find_field_value,
    format_dates,
    create_dict_wide,
    create_dict_long,
    generate_metadata,
    write_metadata,
    checksum,
    main,
    validate,
    validate_cli,
)
from fhirflat.resources.encounter import Encounter
from fhirflat.resources.observation import Observation
import pandas as pd
from pandas.testing import assert_frame_equal
import os
import sys
import shutil
from pathlib import Path
import numpy as np
import pytest

if sys.version_info < (3, 11):  # tomllib was introduced in 3.11
    import tomli  # pragma: no cover
else:
    import tomllib as tomli

METADATA_CHECKSUM = "64c9da8c3bef0174342de0aeb335282ac05bfa6b4ef94ce4a523fb446858a6bf"
FIELD_VAL_ROW_WIDE = pd.Series(
    {
        "dates_enrolment": "2021-04-02",
        "dates_admdate": "2021-04-01",
        "dates_admtime": "18:00",
        "outco_secondiag_oth": "Malaria",
        "outco_outcome": 1,
    }
)

FIELD_VAL_ROW_LONG = pd.Series({"index": 0, "column": "vital_highesttem_c", "value": 2})

FIELD_VAL_ROW_RAW = pd.DataFrame(
    {
        "subjid": 2,
        "visitid": 11,
        "dates_enrolment": "2021-04-02",
        "dates_adm": 1,
        "dates_admdate": "2021-04-01",
        "dates_admtime": "18:00",
        "outco_denguediag": 1,
        "outco_denguediag_main": np.nan,
        "outco_denguediag_class": 2,
        "outco_secondiag_oth": np.nan,
        "outco_date": "2021-04-10",
        "outco_outcome": 1,
    },
    index=[0],
)


@pytest.mark.parametrize(
    "response, fhir_attr, mapp, raw_data, expected",
    [
        ("2021-04-02", "", "<FIELD>", None, "2021-04-02"),  # col=dates_enrolment
        (
            "2021-04-01",
            "",
            "<FIELD>+<dates_admtime>",
            None,
            "2021-04-01 18:00",
        ),  # col=dates_admdate
        (
            "2021-04-02",
            "actualPeriod.start",
            "<FIELD>+<dates_admtime",
            None,
            "2021-04-02T18:00:00+01:00",
        ),  # col=dates_admdate
        (
            "2021-04-02",
            "",
            "<FIELD> if not <dates_admdate>",
            None,
            None,
        ),  # col=dates_enrolment
        ("Malaria", "", "<outco_secondiag_oth>", None, "Malaria"),
        # (2, "id", "<subjid>", FIELD_VAL_ROW_RAW, 2),
        (None, "", "vital-signs", None, "vital-signs"),
    ],
)
def test_find_field_value(response, fhir_attr, mapp, raw_data, expected):
    row = FIELD_VAL_ROW_WIDE
    result = find_field_value(
        row, response, fhir_attr, mapp, "%Y-%m-%d", "Europe/London", raw_data=raw_data
    )
    assert result == expected


def test_find_field_value_long_data():
    result = find_field_value(
        FIELD_VAL_ROW_LONG,
        None,
        "value",
        "<subjid>",
        "%Y-%m-%d",
        "Europe/London",
        raw_data=FIELD_VAL_ROW_RAW,
    )
    assert result == 2


@pytest.mark.parametrize(
    "response, fhir_attr, mapp, raw_data, comment",
    [
        (
            "Fish",
            "",
            "<outco_thirddiag_oth>",
            None,
            "not found in the filtered data",
        ),
        ("Fish", "", "<outco_thirddiag_oth>", FIELD_VAL_ROW_RAW, "not found in data"),
    ],
)
def test_find_field_value_error(response, fhir_attr, mapp, raw_data, comment):
    row = FIELD_VAL_ROW_WIDE
    with pytest.raises(KeyError, match=comment):
        find_field_value(
            row,
            response,
            fhir_attr,
            mapp,
            "%Y-%m-%d",
            "Europe/London",
            raw_data=raw_data,
        )


@pytest.mark.parametrize(
    "date_srt, format, tz, expected",
    [
        ("2021-04-01", "%Y-%m-%d", "Brazil/East", "2021-04-01"),
        ("2021-04-01 18:00", "%Y-%m-%d", "Brazil/East", "2021-04-01T18:00:00-03:00"),
        ("2021-04-01 00:30", "%Y-%m-%d", "UTC", "2021-04-01T00:30:00+00:00"),
        (
            "2021-04-01 12:00",
            "%Y-%m-%d %H:%M",
            "Brazil/East",
            "2021-04-01T12:00:00-03:00",
        ),
        (None, "%Y-%m-%d", "Brazil/East", None),
    ],
)
def test_format_dates(date_srt, format, tz, expected):
    assert format_dates(date_srt, format, tz) == expected


def test_format_dates_warning():
    with pytest.warns(UserWarning, match="could not be converted using date format"):
        format_dates("2021-04-01", "%m/%d/%Y", "Brazil/East")


MAP_DF_MISSING_COLUMNS = pd.DataFrame(
    {
        "raw_variable": ["dates_enrolment", "outco_outcome"],
        "raw_response": [np.nan, 2],
        "actualPeriod.start": ["<FIELD>", np.nan],
        "admission.dischargeDisposition.text": [np.nan, "Discharged"],
    }
)

MAP_DF_MISSING_MAPPING = pd.DataFrame(
    {
        "raw_variable": [
            "dates_enrolment",
            "dates_admdate",
            "dates_admtime",
            "outco_secondiag_oth",
            "outco_outcome",
        ],
        "raw_response": [np.nan, np.nan, np.nan, np.nan, 2],
        "actualPeriod.start": ["<FIELD>", np.nan, np.nan, np.nan, np.nan],
        "admission.dischargeDisposition.text": [
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Discharged",
        ],
    }
)


def test_create_dict_wide_errors():
    map_df = MAP_DF_MISSING_COLUMNS.copy()
    map_df.set_index(["raw_variable", "raw_response"], inplace=True)
    with pytest.raises(ValueError, match="not found in mapping file"):
        create_dict_wide(
            FIELD_VAL_ROW_WIDE,
            map_df,
            "%Y-%m-%d",
            "Brazil/East",
        )

    map_df = MAP_DF_MISSING_MAPPING
    map_df.set_index(["raw_variable", "raw_response"], inplace=True)
    with pytest.warns(UserWarning, match="No mapping for column"):
        create_dict_wide(
            FIELD_VAL_ROW_WIDE,
            map_df,
            "%Y-%m-%d",
            "Brazil/East",
        )


def test_create_dict_long_errors():
    map_df = MAP_DF_MISSING_COLUMNS.copy()
    map_df.set_index(["raw_variable", "raw_response"], inplace=True)
    with pytest.warns(UserWarning, match="No mapping for column"):
        create_dict_long(
            FIELD_VAL_ROW_LONG,
            None,
            map_df,
            "%Y-%m-%d",
            "Brazil/East",
        )


ENCOUNTER_DICT_OUT = {
    "id": 11,
    "subject": "Patient/2",
    "actualPeriod.start": "2021-04-01T18:00:00-03:00",
    "actualPeriod.end": "2021-04-10",
    "extension.timingPhase.system": "https://snomed.info/sct",
    "extension.timingPhase.code": 278307001,
    "extension.timingPhase.text": "On admission (qualifier value)",
    "class.system": "https://snomed.info/sct",
    "class.code": 32485007,
    "class.text": "Hospital admission (procedure)",
    "diagnosis.condition.concept.system": [
        "https://snomed.info/sct",
        "https://snomed.info/sct",
    ],
    "diagnosis.condition.concept.code": [38362002, 722863008],
    "diagnosis.condition.concept.text": [
        "Dengue (disorder)",
        "Dengue with warning signs (disorder)",
    ],
    "diagnosis.use.system": ["https://snomed.info/sct", "https://snomed.info/sct"],
    "diagnosis.use.code": [89100005, 89100005],
    "diagnosis.use.text": [
        "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",
        "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",
    ],
    "admission.dischargeDisposition.system": "https://snomed.info/sct",
    "admission.dischargeDisposition.code": 371827001,
    "admission.dischargeDisposition.text": "Patient discharged alive (finding)",
}


def test_create_dict_one_to_one_single_row():
    df = create_dictionary(
        "tests/dummy_data/encounter_dummy_data_single.csv",
        "tests/dummy_data/encounter_dummy_mapping.csv",
        "Encounter",
        one_to_one=True,
        date_format="%Y-%m-%d",
        timezone="Brazil/East",
    )

    assert df is not None
    dict_out = df["flat_dict"][0]

    assert dict_out == ENCOUNTER_DICT_OUT


def test_create_dict_missing_data_warning():
    with pytest.warns(UserWarning, match="No data found for the Observation resource"):
        create_dictionary(
            "tests/dummy_data/encounter_dummy_data_single.csv",
            "tests/dummy_data/observation_dummy_mapping.csv",
            "Observation",
            one_to_one=True,
            date_format="%Y-%m-%d",
            timezone="Brazil/East",
        )


def test_create_dict_one_to_one_multirow_condense():
    """
    Checks that if the raw data contains multiple rows of data per Patient/Encounter, if
    a single FHIR attribute can be filled by more than one row an error is raised.
    """

    mapped_dict = {
        "subject": "Patient/1",
        "actualPeriod.start": "2021-04-01T18:00:00-03:00",
        "actualPeriod.end": "2021-04-10",
        "extension.timingPhase.system": "https://snomed.info/sct",
        "extension.timingPhase.code": 278307001.0,
        "extension.timingPhase.text": "On admission (qualifier value)",
        "class.system": "https://snomed.info/sct",
        "class.code": 32485007.0,
        "class.text": "Hospital admission (procedure)",
        "diagnosis.condition.concept.system": [
            "https://snomed.info/sct",
            "https://snomed.info/sct",
        ],
        "diagnosis.condition.concept.code": [38362002.0, 722863008.0],
        "diagnosis.condition.concept.text": [
            "Dengue (disorder)",
            "Dengue with warning signs (disorder)",
        ],
        "diagnosis.use.system": ["https://snomed.info/sct", "https://snomed.info/sct"],
        "diagnosis.use.code": [89100005.0, 89100005.0],
        "diagnosis.use.text": [
            "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",
            "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",
        ],
        "admission.dischargeDisposition.system": "https://snomed.info/sct",
        "admission.dischargeDisposition.code": 371827001.0,
        "admission.dischargeDisposition.text": "Patient discharged alive (finding)",
    }

    df = create_dictionary(
        "tests/dummy_data/data_multirow_encounter.csv",
        "tests/dummy_data/encounter_dummy_mapping.csv",
        "Encounter",
        one_to_one=True,
        date_format="%Y-%m-%d",
        timezone="Brazil/East",
    )

    assert df is not None
    dict_out = df["flat_dict"][0]

    assert dict_out == mapped_dict

    with pytest.raises(ValueError, match="Multiple values found in one-to-one mapping"):
        # input data has different admission dates for same patient/encounter on
        # different rows
        create_dictionary(
            "tests/dummy_data/data_multirow_encounter_error.csv",
            "tests/dummy_data/encounter_dummy_mapping.csv",
            "Encounter",
            one_to_one=True,
            date_format="%Y-%m-%d",
            timezone="Brazil/East",
        )


@pytest.mark.parametrize(
    ("file, expected"),
    [
        (
            "tests/dummy_data/data_multirow_encounter_freetext_maindiag.csv",
            {
                "diagnosis.condition.concept.code": [None, 722863008.0],
                "diagnosis.condition.concept.system": [None, "https://snomed.info/sct"],
                "diagnosis.condition.concept.text": [
                    "sepsis",
                    "Dengue with warning signs (disorder)",
                ],
                "diagnosis.use.system": [
                    "https://snomed.info/sct",
                    "https://snomed.info/sct",
                ],
                "diagnosis.use.code": [89100005.0, 89100005.0],
                "diagnosis.use.text": [
                    "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                    "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                ],
            },
        ),
        (
            "tests/dummy_data/data_multirow_encounter_freetext.csv",
            {
                "diagnosis.condition.concept.code": [38362002.0, 20927009.0, None],
                "diagnosis.condition.concept.system": [
                    "https://snomed.info/sct",
                    "https://snomed.info/sct",
                    None,
                ],
                "diagnosis.condition.concept.text": [
                    "Dengue (disorder)",
                    "Dengue hemorrhagic fever (disorder)",
                    "RTI",
                ],
                "diagnosis.use.system": [
                    "https://snomed.info/sct",
                    "https://snomed.info/sct",
                    "https://snomed.info/sct",
                ],
                "diagnosis.use.code": [89100005.0, 89100005.0, 85097005.0],
                "diagnosis.use.text": [
                    "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                    "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                    "Secondary diagnosis (contextual qualifier) (qualifier value)",  # noqa: E501
                ],
            },
        ),
        (
            "tests/dummy_data/data_multirow_encounter_freetext_secdiag.csv",
            {
                "diagnosis.condition.concept.system": ["https://snomed.info/sct", None],
                "diagnosis.condition.concept.code": [261665006.0, None],
                "diagnosis.condition.concept.text": [
                    "Unknown (qualifier value)",
                    "secondary Dengue",
                ],
                "diagnosis.use.system": [
                    "https://snomed.info/sct",
                    "https://snomed.info/sct",
                ],
                "diagnosis.use.code": [89100005.0, 85097005.0],
                "diagnosis.use.text": [
                    "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                    "Secondary diagnosis (contextual qualifier) (qualifier value)",
                ],
            },
        ),
    ],
)
def test_create_dict_one_to_one_dense_freetext(file, expected):
    df = create_dictionary(
        file,
        "tests/dummy_data/encounter_dummy_mapping.csv",
        "Encounter",
        one_to_one=True,
        date_format="%Y-%m-%d",
        timezone="Brazil/East",
    )

    assert df is not None
    dict_out = df["flat_dict"][0]

    diagnosis_cols = [
        "diagnosis.condition.concept.system",
        "diagnosis.condition.concept.code",
        "diagnosis.condition.concept.text",
        "diagnosis.use.system",
        "diagnosis.use.code",
        "diagnosis.use.text",
    ]

    # only interested in the diagnosis (backbone element) columns
    assert {k: dict_out[k] for k in diagnosis_cols} == expected


ENCOUNTER_SINGLE_ROW_FLAT = {
    "id": 11,
    "class.code": ["https://snomed.info/sct|32485007"],
    "class.text": ["Hospital admission (procedure)"],
    "diagnosis_dense": [
        {
            "condition": [
                {
                    "concept": {
                        "coding": [
                            {
                                "code": "38362002",
                                "display": "Dengue (disorder)",
                                "system": "https://snomed.info/sct",
                            }
                        ]
                    }
                }
            ],
            "use": [
                {
                    "coding": [
                        {
                            "code": "89100005",
                            "display": "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                            "system": "https://snomed.info/sct",
                        }
                    ]
                }
            ],
        },
        {
            "condition": [
                {
                    "concept": {
                        "coding": [
                            {
                                "system": "https://snomed.info/sct",
                                "code": "722863008",
                                "display": "Dengue with warning signs (disorder)",
                            }
                        ]
                    }
                }
            ],
            "use": [
                {
                    "coding": [
                        {
                            "code": "89100005",
                            "display": "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                            "system": "https://snomed.info/sct",
                        }
                    ]
                }
            ],
        },
    ],
    "subject": "Patient/2",
    "actualPeriod.start": "2021-04-01T18:00:00-03:00",
    "actualPeriod.end": "2021-04-10",
    "admission.dischargeDisposition.code": ["https://snomed.info/sct|371827001"],
    "admission.dischargeDisposition.text": ["Patient discharged alive (finding)"],
    "extension.timingPhase.code": ["https://snomed.info/sct|278307001"],
    "extension.timingPhase.text": ["On admission (qualifier value)"],
}


def test_load_data_one_to_one_single_row():
    df = create_dictionary(
        "tests/dummy_data/encounter_dummy_data_single.csv",
        "tests/dummy_data/encounter_dummy_mapping.csv",
        "Encounter",
        one_to_one=True,
        date_format="%Y-%m-%d",
        timezone="Brazil/East",
    )

    assert df is not None

    flat_encounter = Encounter.ingest_to_flat(df)
    expected_encounter = pd.DataFrame([ENCOUNTER_SINGLE_ROW_FLAT], index=[0])

    _, error = Encounter.validate_fhirflat(flat_encounter)

    assert error is None

    assert_frame_equal(
        flat_encounter,
        expected_encounter,
        check_like=True,
    )


def test_load_data_one_to_one_dense_single_row():
    df = create_dictionary(
        "tests/dummy_data/data_multirow_encounter_freetext_maindiag.csv",
        "tests/dummy_data/encounter_dummy_mapping.csv",
        "Encounter",
        one_to_one=True,
        date_format="%Y-%m-%d",
        timezone="Brazil/East",
    )

    assert df is not None

    flat_diagnosis = Encounter.ingest_to_flat(df)

    _, e = Encounter.validate_fhirflat(flat_diagnosis)
    assert e is None

    expected_diagnosis = [
        {
            "condition": [{"concept": {"text": "sepsis"}}],
            "use": [
                {
                    "coding": [
                        {
                            "code": "89100005",
                            "display": "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                            "system": "https://snomed.info/sct",
                        }
                    ]
                }
            ],
        },
        {
            "condition": [
                {
                    "concept": {
                        "coding": [
                            {
                                "code": "722863008",
                                "display": "Dengue with warning signs (disorder)",
                                "system": "https://snomed.info/sct",
                            }
                        ],
                    }
                }
            ],
            "use": [
                {
                    "coding": [
                        {
                            "code": "89100005",
                            "display": "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                            "system": "https://snomed.info/sct",
                        }
                    ]
                }
            ],
        },
    ]

    assert flat_diagnosis["diagnosis_dense"].iloc[0] == expected_diagnosis


ENCOUNTER_SINGLE_ROW_MULTI = {
    "class.code": [
        ["https://snomed.info/sct|371883000"],
        ["https://snomed.info/sct|32485007"],
        ["https://snomed.info/sct|32485007"],
        ["https://snomed.info/sct|32485007"],
    ],
    "class.text": [
        ["Outpatient procedure (procedure)"],
        ["Hospital admission (procedure)"],
        ["Hospital admission (procedure)"],
        ["Hospital admission (procedure)"],
    ],
    "diagnosis_dense": [
        None,
        [
            {
                "condition": [
                    {
                        "concept": {
                            "coding": [
                                {
                                    "code": "38362002",
                                    "display": "Dengue (disorder)",
                                    "system": "https://snomed.info/sct",
                                }
                            ]
                        }
                    }
                ],
                "use": [
                    {
                        "coding": [
                            {
                                "code": "89100005",
                                "display": "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                                "system": "https://snomed.info/sct",
                            }
                        ]
                    }
                ],
            },
            {
                "condition": [
                    {
                        "concept": {
                            "coding": [
                                {
                                    "code": "722863008",
                                    "display": "Dengue with warning signs (disorder)",
                                    "system": "https://snomed.info/sct",
                                }
                            ]
                        }
                    }
                ],
                "use": [
                    {
                        "coding": [
                            {
                                "code": "89100005",
                                "display": "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                                "system": "https://snomed.info/sct",
                            }
                        ]
                    }
                ],
            },
        ],
        [
            {
                "condition": [
                    {
                        "concept": {
                            "coding": [
                                {
                                    "code": "38362002",
                                    "display": "Dengue (disorder)",
                                    "system": "https://snomed.info/sct",
                                }
                            ]
                        }
                    }
                ],
                "use": [
                    {
                        "coding": [
                            {
                                "code": "89100005",
                                "display": "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                                "system": "https://snomed.info/sct",
                            }
                        ]
                    }
                ],
            },
            {
                "condition": [
                    {
                        "concept": {
                            "coding": [
                                {
                                    "code": "722862003",
                                    "display": "Dengue without warning signs (disorder)",  # noqa: E501
                                    "system": "https://snomed.info/sct",
                                }
                            ]
                        }
                    }
                ],
                "use": [
                    {
                        "coding": [
                            {
                                "code": "89100005",
                                "display": "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                                "system": "https://snomed.info/sct",
                            }
                        ]
                    }
                ],
            },
        ],
        None,
    ],
    "diagnosis.condition.concept.text": [
        None,
        None,
        None,
        ["Malaria"],
    ],
    "diagnosis.use.code": [
        None,
        None,
        None,
        ["https://snomed.info/sct|89100005"],
    ],
    "diagnosis.use.text": [
        None,
        None,
        None,
        ["Final diagnosis (discharge) (contextual qualifier) (qualifier value)"],
    ],
    "subject": ["Patient/1", "Patient/2", "Patient/3", "Patient/4"],
    "id": [10, 11, 12, 13],
    "actualPeriod.start": [
        "2020-05-01",
        "2021-04-01T18:00:00-03:00",
        "2021-05-10T17:30:00-03:00",
        "2022-06-15T21:00:00-03:00",
    ],
    "actualPeriod.end": [
        "2020-05-01",
        "2021-04-10",
        "2021-05-15",
        "2022-06-20",
    ],
    "admission.dischargeDisposition.code": [
        ["https://snomed.info/sct|371827001"],
        ["https://snomed.info/sct|371827001"],
        ["https://snomed.info/sct|419099009"],
        ["https://snomed.info/sct|32485007"],
    ],
    "admission.dischargeDisposition.text": [
        ["Patient discharged alive (finding)"],
        ["Patient discharged alive (finding)"],
        ["Dead (finding)"],
        ["Hospital admission (procedure)"],
    ],
    "extension.timingPhase.code": [
        ["https://snomed.info/sct|281379000"],
        ["https://snomed.info/sct|278307001"],
        ["https://snomed.info/sct|278307001"],
        ["https://snomed.info/sct|278307001"],
    ],
    "extension.timingPhase.text": [
        ["Pre-admission (qualifier value)"],
        ["On admission (qualifier value)"],
        ["On admission (qualifier value)"],
        ["On admission (qualifier value)"],
    ],
}


def test_load_data_one_to_one_multi_row():
    df = create_dictionary(
        "tests/dummy_data/encounter_dummy_data_multi_patient.csv",
        "tests/dummy_data/encounter_dummy_mapping.csv",
        "Encounter",
        one_to_one=True,
        date_format="%Y-%m-%d",
        timezone="Brazil/East",
    )

    assert df is not None

    flat_encounter = Encounter.ingest_to_flat(df)

    _, e = Encounter.validate_fhirflat(flat_encounter)
    assert e is None

    assert_frame_equal(
        flat_encounter,
        pd.DataFrame(ENCOUNTER_SINGLE_ROW_MULTI),
        check_dtype=False,
        check_like=True,
    )


OBS_FLAT = {
    "category.code": [
        ["http://terminology.hl7.org/CodeSystem/observation-category|vital-signs"],
        ["http://terminology.hl7.org/CodeSystem/observation-category|vital-signs"],
        ["http://terminology.hl7.org/CodeSystem/observation-category|vital-signs"],
        ["http://terminology.hl7.org/CodeSystem/observation-category|vital-signs"],
        ["http://terminology.hl7.org/CodeSystem/observation-category|vital-signs"],
    ],
    "category.text": [
        ["Vital Signs"],
        ["Vital Signs"],
        ["Vital Signs"],
        ["Vital Signs"],
        ["Vital Signs"],
    ],
    "effectiveDateTime": [
        "2020-01-01",
        "2021-02-02",
        "2022-03-03",
        "2020-01-01",
        "2021-02-02",
    ],
    "code.code": [
        ["https://loinc.org|8310-5"],
        ["https://loinc.org|8310-5"],
        ["https://loinc.org|8310-5"],
        ["https://loinc.org|8867-4"],
        ["https://loinc.org|8867-4"],
    ],
    "code.text": [
        ["Body temperature"],
        ["Body temperature"],
        ["Body temperature"],
        ["Heart rate"],
        ["Heart rate"],
    ],
    "subject": ["Patient/1", "Patient/2", "Patient/3", "Patient/1", "Patient/2"],
    "encounter": [
        "Encounter/10",
        "Encounter/11",
        "Encounter/12",
        "Encounter/10",
        "Encounter/11",
    ],
    "valueQuantity.value": [36.2, 37.0, 35.5, 120.0, 100.0],
    "valueQuantity.unit": [
        "DegreesCelsius",
        "DegreesCelsius",
        "DegreesCelsius",
        "Beats/minute (qualifier value)",
        "Beats/minute (qualifier value)",
    ],
    "valueQuantity.code": [
        "http://unitsofmeasure|Cel",
        "http://unitsofmeasure|Cel",
        "http://unitsofmeasure|Cel",
        "https://snomed.info/sct|258983007",
        "https://snomed.info/sct|258983007",
    ],
    "valueCodeableConcept.code": [None, None, None, None, None],
    "valueCodeableConcept.text": [None, None, None, None, None],
    "valueInteger": [np.nan, np.nan, np.nan, np.nan, np.nan],
}


def test_load_data_one_to_many_multi_row():
    df = create_dictionary(
        "tests/dummy_data/vital_signs_dummy_data.csv",
        "tests/dummy_data/observation_dummy_mapping.csv",
        "Observation",
        one_to_one=False,
        date_format="%Y-%m-%d",
        timezone="Brazil/East",
    )

    assert df is not None
    clean_df = df.dropna().copy()

    flat_obs = Observation.ingest_to_flat(clean_df)

    _, e = Observation.validate_fhirflat(flat_obs)
    assert e is None

    assert len(flat_obs) == 33

    df_head = flat_obs.head(5)

    assert_frame_equal(
        df_head,
        pd.DataFrame(OBS_FLAT),
        check_dtype=False,
        check_like=True,
    )


def test_convert_data_to_flat_missing_mapping_error():
    with pytest.raises(
        TypeError, match="Either mapping_files_types or sheet_id must be provided"
    ):
        convert_data_to_flat(
            "tests/dummy_data/combined_dummy_data.csv",
            folder_name="tests/ingestion_output",
            date_format="%Y-%m-%d",
            timezone="Brazil/East",
        )


def test_convert_data_to_flat_wrong_mapping_type_error():
    mappings = {
        Encounter: "tests/dummy_data/encounter_dummy_mapping.csv",
    }
    resource_types = {"Encounter": "three-to-three"}

    with pytest.raises(ValueError, match="Unknown mapping type three-to-three"):
        convert_data_to_flat(
            "tests/dummy_data/combined_dummy_data.csv",
            folder_name="tests/ingestion_output",
            date_format="%Y-%m-%d",
            timezone="Brazil/East",
            mapping_files_types=(mappings, resource_types),
        )


def test_convert_data_to_flat_no_validation_warning():
    mappings = {
        Encounter: "tests/dummy_data/encounter_dummy_mapping.csv",
    }
    resource_types = {"Encounter": "one-to-one"}

    with pytest.warns(
        UserWarning, match="Validation of the FHIRflat files has been disabled"
    ):
        convert_data_to_flat(
            "tests/dummy_data/combined_dummy_data.csv",
            folder_name="tests/ingestion_output",
            date_format="%Y-%m-%d",
            timezone="Brazil/East",
            mapping_files_types=(mappings, resource_types),
            validate=False,
        )
    shutil.rmtree("tests/ingestion_output")


def test_convert_data_to_flat_no_validation_invalid_file_warning():
    mappings = {
        Encounter: "tests/dummy_data/encounter_dummy_mapping.csv",
    }
    resource_types = {"Encounter": "one-to-one"}

    with pytest.warns(UserWarning, match="This is likely due to a validation error"):
        convert_data_to_flat(
            "tests/dummy_data/combined_dummy_data_parquet_error.csv",
            folder_name="tests/ingestion_output_errors",
            date_format="%Y-%m-%d",
            timezone="Brazil/East",
            mapping_files_types=(mappings, resource_types),
            validate=False,
        )
    shutil.rmtree("tests/ingestion_output_errors")


def test_generate_metadata():
    meta = generate_metadata("tests/bundle")
    assert meta[0]["checksum"] == METADATA_CHECKSUM
    assert meta[0]["N"] == 2
    assert meta[1] == {
        "condition.parquet": "cfe0c5f792e47facffda38d0ddaf2f8b87613998eb99aefa56d99551dcb7bf80",  # noqa: E501
        "encounter.parquet": "165f2295e6419f9674c909b53553006e9715691c6a3f1a739f4313c08fde0747",  # noqa: E501
        "patient.parquet": "710c73bf95f3b5d95bfdcfed18c9c41609a3fe00dd32590d43372e3220b8e971",  # noqa: E501
    }


def test_write_metadata():
    meta = generate_metadata("tests/bundle")
    write_metadata(*meta, Path("tests/bundle/fhirflat.toml"))
    metadata = tomli.loads(Path("tests/bundle/fhirflat.toml").read_text())["metadata"]
    assert metadata["checksum"] == METADATA_CHECKSUM
    assert metadata["N"] == 2
    assert checksum("tests/bundle/sha256sums.txt") == METADATA_CHECKSUM
    os.remove("tests/bundle/fhirflat.toml")
    os.remove("tests/bundle/sha256sums.txt")


def test_convert_data_to_flat_local_mapping():
    output_folder = "tests/ingestion_output"
    mappings = {
        Encounter: "tests/dummy_data/encounter_dummy_mapping.csv",
        Observation: "tests/dummy_data/observation_dummy_mapping.csv",
    }
    resource_types = {"Encounter": "one-to-one", "Observation": "one-to-many"}

    convert_data_to_flat(
        "tests/dummy_data/combined_dummy_data.csv",
        folder_name=output_folder,
        date_format="%Y-%m-%d",
        timezone="Brazil/East",
        mapping_files_types=(mappings, resource_types),
    )

    encounter_df = pd.read_parquet("tests/ingestion_output/encounter.parquet")
    obs_df = pd.read_parquet("tests/ingestion_output/observation.parquet")

    assert_frame_equal(
        encounter_df,
        pd.DataFrame(ENCOUNTER_SINGLE_ROW_MULTI),
        check_dtype=False,
        check_like=True,
    )

    assert len(obs_df) == 33

    obs_df_head = obs_df.head(5)

    assert_frame_equal(
        obs_df_head,
        pd.DataFrame(OBS_FLAT),
        check_dtype=False,
        check_like=True,
    )

    shutil.rmtree(output_folder)


def test_convert_data_to_flat_local_mapping_zipped():
    output_folder = "tests/ingestion_output"
    mappings = {
        Encounter: "tests/dummy_data/encounter_dummy_mapping.csv",
    }
    resource_types = {"Encounter": "one-to-one"}

    convert_data_to_flat(
        "tests/dummy_data/combined_dummy_data.csv",
        folder_name=output_folder,
        date_format="%Y-%m-%d",
        timezone="Brazil/East",
        mapping_files_types=(mappings, resource_types),
        compress_format="zip",
    )

    assert os.path.exists("tests/ingestion_output.zip")

    os.remove("tests/ingestion_output.zip")


# This doesn't run intermittantly - because of the "#NAME" error i get with the google
# sheets
# Turns out this is an issue with custom functions in Google Sheets, not a Python thing.
def test_main(capsys, monkeypatch):
    # Simulate command line arguments
    monkeypatch.setattr(
        "sys.argv",
        [
            "ingest.py",
            "tests/dummy_data/combined_dummy_data.csv",
            "15nQwXBIKnXF9lRHbVVdfFxOMGfLqhOfxZ7GkjSu_Kcs",
            "%Y-%m-%d",
            "Brazil/East",
        ],
    )
    with pytest.warns(UserWarning, match="No data found"):
        main()
    captured = capsys.readouterr()
    assert "Encounter took" in captured.out
    assert "Observation took" in captured.out

    shutil.rmtree("fhirflat_output")


def test_validate_cli(capsys, monkeypatch):
    # Simulate command line arguments
    monkeypatch.setattr(
        "sys.argv",
        ["ingest.py", "tests/data/valid_flat_bundle"],
    )
    validate_cli()
    captured = capsys.readouterr()
    assert "encounter.parquet is valid" in captured.out
    assert "condition.parquet is valid" in captured.out
    assert "validation errors" not in captured.out


def test_convert_data_to_flat_local_mapping_errors():
    output_folder = "tests/ingestion_output_errors"
    mappings = {
        Encounter: "tests/dummy_data/encounter_dummy_mapping.csv",
        Observation: "tests/dummy_data/observation_dummy_mapping.csv",
    }
    resource_types = {"Encounter": "one-to-one", "Observation": "one-to-many"}

    with pytest.warns(UserWarning, match="could not be converted using date format"):
        convert_data_to_flat(
            "tests/dummy_data/combined_dummy_data_error.csv",
            folder_name=output_folder,
            date_format="%Y-%m-%d",
            timezone="Brazil/East",
            mapping_files_types=(mappings, resource_types),
        )

    encounter_df = pd.read_parquet("tests/ingestion_output_errors/encounter.parquet")
    obs_df = pd.read_parquet("tests/ingestion_output_errors/observation.parquet")

    expected_encounter_minus_errors = pd.DataFrame(ENCOUNTER_SINGLE_ROW_MULTI).iloc[:-1]

    assert_frame_equal(
        encounter_df,
        expected_encounter_minus_errors,
        check_dtype=False,
        check_like=True,
    )

    assert len(obs_df) == 33

    obs_df_head = obs_df.head(5)

    assert_frame_equal(
        obs_df_head,
        pd.DataFrame(OBS_FLAT),
        check_dtype=False,
        check_like=True,
    )

    encounter_error = pd.read_csv("tests/ingestion_output_errors/encounter_errors.csv")
    assert len(encounter_error) == 1
    assert (
        encounter_error["validation_error"][0]
        == "1 validation error for Encounter\nactualPeriod -> start\n  invalid datetime format (type=value_error.datetime)"  # noqa: E501
    )

    shutil.rmtree(output_folder)


def test_validate_valid(capsys):
    folder = "tests/data/valid_flat_bundle"

    validate(folder)

    captured = capsys.readouterr()
    assert "encounter.parquet is valid" in captured.out
    assert "condition.parquet is valid" in captured.out
    assert "patient.parquet is valid" in captured.out
    assert "Validation complete" in captured.out


def test_validate_compress(capsys):
    folder = "tests/data/valid_flat_compressed.zip"

    validate(folder, compress_format="zip")

    captured = capsys.readouterr()
    assert "patient.parquet is valid" in captured.out
    assert "Validation complete" in captured.out

    assert Path("tests/data/valid_flat_compressed_validated.zip").exists()
    Path.unlink("tests/data/valid_flat_compressed_validated.zip")


def test_validate_invalid(capsys):
    folder = "tests/data/invalid_flat_bundle"

    validate(folder)

    captured = capsys.readouterr()
    assert "encounter.parquet have validation errors" in captured.out
    assert "condition.parquet have validation errors" in captured.out
    assert "Validation complete" in captured.out

    Path.unlink(os.path.join(folder, "encounter_errors.csv"))
    Path.unlink(os.path.join(folder, "encounter_valid.parquet"))
    Path.unlink(os.path.join(folder, "condition_errors.csv"))
    Path.unlink(os.path.join(folder, "condition_valid.parquet"))
