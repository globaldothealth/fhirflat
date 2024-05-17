from fhirflat.ingest import (
    create_dictionary,
    convert_data_to_flat,
)
from fhirflat.resources.encounter import Encounter
from fhirflat.resources.observation import Observation
import pandas as pd
from pandas.testing import assert_frame_equal
import os
import shutil
from decimal import Decimal


ENCOUNTER_DICT_OUT = {
    "id": 11,
    "subject": 2,
    "actualPeriod.start": "2021-04-01 18:00",
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
    )

    dict_out = df["flat_dict"][0]

    assert dict_out == ENCOUNTER_DICT_OUT


ENCOUNTER_SINGLE_ROW_FLAT = {
    "resourceType": "Encounter",
    "id": "11",
    "class.code": "https://snomed.info/sct|32485007",
    "class.text": "Hospital admission (procedure)",
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
    "subject": "2",
    "actualPeriod.start": "2021-04-01 18:00:00",
    "actualPeriod.end": "2021-04-10",
    "admission.dischargeDisposition.code": "https://snomed.info/sct|371827001",
    "admission.dischargeDisposition.text": "Patient discharged alive (finding)",
    "extension.timingPhase.code": ["https://snomed.info/sct|278307001"],
    "extension.timingPhase.text": ["On admission (qualifier value)"],
}


def test_load_data_one_to_one_single_row():
    df = create_dictionary(
        "tests/dummy_data/encounter_dummy_data_single.csv",
        "tests/dummy_data/encounter_dummy_mapping.csv",
        "Encounter",
        one_to_one=True,
    )

    Encounter.ingest_to_flat(df, "encounter_ingestion_single")

    assert_frame_equal(
        pd.read_parquet("encounter_ingestion_single.parquet"),
        pd.DataFrame([ENCOUNTER_SINGLE_ROW_FLAT], index=[0]),
        check_dtype=False,
    )
    os.remove("encounter_ingestion_single.parquet")


ENCOUNTER_SINGLE_ROW_MULTI = {
    "resourceType": ["Encounter", "Encounter", "Encounter", "Encounter"],
    "class.code": [
        "https://snomed.info/sct|371883000",
        "https://snomed.info/sct|32485007",
        "https://snomed.info/sct|32485007",
        "https://snomed.info/sct|32485007",
    ],
    "class.text": [
        "Outpatient procedure (procedure)",
        "Hospital admission (procedure)",
        "Hospital admission (procedure)",
        "Hospital admission (procedure)",
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
        "Malaria",
    ],
    "diagnosis.use.code": [
        None,
        None,
        None,
        "https://snomed.info/sct|89100005",
    ],
    "diagnosis.use.text": [
        None,
        None,
        None,
        "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",
    ],
    "subject": ["p1", "p2", "p3", "p4"],
    "id": ["e10", "e11", "e12", "e13"],
    "actualPeriod.start": [
        "2020-05-01",
        "2021-04-01 18:00:00",
        "2021-05-10 17:30:00",
        "2022-06-15 21:00:00",
    ],
    "actualPeriod.end": [
        "2020-05-01",
        "2021-04-10",
        "2021-05-15",
        "2022-06-20",
    ],
    "admission.dischargeDisposition.code": [
        "https://snomed.info/sct|371827001",
        "https://snomed.info/sct|371827001",
        "https://snomed.info/sct|419099009",
        "https://snomed.info/sct|32485007",
    ],
    "admission.dischargeDisposition.text": [
        "Patient discharged alive (finding)",
        "Patient discharged alive (finding)",
        "Dead (finding)",
        "Hospital admission (procedure)",
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
        "tests/dummy_data/encounter_dummy_data_multi.csv",
        "tests/dummy_data/encounter_dummy_mapping.csv",
        "Encounter",
        one_to_one=True,
    )

    Encounter.ingest_to_flat(df, "encounter_ingestion_multi")

    assert_frame_equal(
        pd.read_parquet("encounter_ingestion_multi.parquet"),
        pd.DataFrame(ENCOUNTER_SINGLE_ROW_MULTI),
        check_dtype=False,
        check_like=True,
    )
    os.remove("encounter_ingestion_multi.parquet")


OBS_FLAT = {
    "resourceType": [
        "Observation",
        "Observation",
        "Observation",
        "Observation",
        "Observation",
    ],
    "category.code": [
        "http://terminology.hl7.org/CodeSystem/observation-category|vital-signs",
        "http://terminology.hl7.org/CodeSystem/observation-category|vital-signs",
        "http://terminology.hl7.org/CodeSystem/observation-category|vital-signs",
        "http://terminology.hl7.org/CodeSystem/observation-category|vital-signs",
        "http://terminology.hl7.org/CodeSystem/observation-category|vital-signs",
    ],
    "category.text": [
        "Vital Signs",
        "Vital Signs",
        "Vital Signs",
        "Vital Signs",
        "Vital Signs",
    ],
    "effectiveDateTime": [
        "2020-01-01",
        "2021-02-02",
        "2022-03-03",
        "2020-01-01",
        "2021-02-02",
    ],
    "code.code": [
        "https://loinc.org|8310-5",
        "https://loinc.org|8310-5",
        "https://loinc.org|8310-5",
        "https://loinc.org|8867-4",
        "https://loinc.org|8867-4",
    ],
    "code.text": [
        "Body temperature",
        "Body temperature",
        "Body temperature",
        "Heart rate",
        "Heart rate",
    ],
    "subject": ["p1", "p2", "p3", "p1", "p2"],
    "encounter": ["e10", "e11", "e12", "e10", "e11"],
    "valueQuantity.value": [Decimal("36.2"), 37.0, 35.5, 120.0, 100.0],
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
    "valueInteger": [None, None, None, None, None],
}


def test_load_data_one_to_many_multi_row():
    df = create_dictionary(
        "tests/dummy_data/vital_signs_dummy_data.csv",
        "tests/dummy_data/observation_dummy_mapping.csv",
        "Observation",
        one_to_one=False,
    )

    Observation.ingest_to_flat(df.dropna(), "observation_ingestion")

    full_df = pd.read_parquet("observation_ingestion.parquet")

    assert len(full_df) == 33

    df_head = full_df.head(5)

    assert_frame_equal(
        df_head,
        pd.DataFrame(OBS_FLAT),
        check_dtype=False,
        check_like=True,
    )
    os.remove("observation_ingestion.parquet")


def test_convert_data_to_flat_local_mapping():
    output_folder = "tests/ingestion_output"
    mappings = {
        Encounter: "tests/dummy_data/encounter_dummy_mapping.csv",
        Observation: "tests/dummy_data/observation_dummy_mapping.csv",
    }
    resource_types = {"Encounter": "one-to-one", "Observation": "one-to-many"}

    convert_data_to_flat(
        "tests/dummy_data/combined_dummy_data.csv",
        mapping_files_types=(mappings, resource_types),
        folder_name=output_folder,
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