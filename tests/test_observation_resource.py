import pandas as pd
from pandas.testing import assert_frame_equal
import os
from fhirflat.resources.observation import Observation
import datetime

# TODO: tests with 'component' lists (in this case systolic and diastolic readings).

# Test Observation --------------------------------------------
#     "component": [
#         {
#             "code": {
#                 "coding": [
#                     {
#                         "system": "http://loinc.org",
#                         "code": "8480-6",
#                         "display": "Systolic blood pressure",
#                     },
#                     {
#                         "system": "http://snomed.info/sct",
#                         "code": "271649006",
#                         "display": "Systolic blood pressure",
#                     },
#                     {
#                         "system": "http://acme.org/devices/clinical-codes",
#                         "code": "bp-s",
#                         "display": "Systolic Blood pressure",
#                     },
#                 ]
#             },
#             "valueQuantity": {
#                 "value": 107,
#                 "unit": "mmHg",
#                 "system": "http://unitsofmeasure.org",
#                 "code": "mm[Hg]",
#             },
#             "interpretation": [
#                 {
#                     "coding": [
#                         {
#                             "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",   # noqa: E501
#                             "code": "N",
#                             "display": "normal",
#                         }
#                     ],
#                     "text": "Normal",
#                 }
#             ],
#         },
#         {
#             "code": {
#                 "coding": [
#                     {
#                         "system": "http://loinc.org",
#                         "code": "8462-4",
#                         "display": "Diastolic blood pressure",
#                     }
#                 ]
#             },
#             "valueQuantity": {
#                 "value": 60,
#                 "unit": "mmHg",
#                 "system": "http://unitsofmeasure.org",
#                 "code": "mm[Hg]",
#             },
#             "interpretation": [
#                 {
#                     "coding": [
#                         {
#                             "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",  # noqa: E501
#                             "code": "L",
#                             "display": "low",
#                         }
#                     ],
#                     "text": "Below low normal",
#                 }
#             ],
#         },
#     ],
# }

OBSERVATION_DICT_INPUT = {
    "resourceType": "Observation",
    "status": "final",
    "category": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",  # noqa: E501
                    "code": "vital-signs",
                    "display": "Vital Signs",
                }
            ]
        }
    ],
    "code": {
        "coding": [
            {
                "system": "http://loinc.org",
                "code": "85354-9",
                "display": "Blood pressure panel with all children optional",
            }
        ],
        "text": "Blood pressure systolic & diastolic",
    },
    "subject": {"reference": "Patient/example"},
    "effectiveDateTime": "2012-09-17",
    "_effectiveDateTime": {
        "extension": [
            {"url": "relativeDay", "valueInteger": 2},
            {"url": "approximateDate", "valueDate": "2012-09"},
        ]
    },
    "performer": [{"reference": "Practitioner/example"}],
    "interpretation": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",  # noqa: E501
                    "code": "L",
                    "display": "low",
                }
            ],
            "text": "Below low normal",
        }
    ],
    "bodySite": {
        "coding": [
            {
                "system": "http://snomed.info/sct",
                "code": "368209003",
                "display": "Right arm",
            }
        ]
    },
}

OBSERVATION_FLAT = {
    "resourceType": "Observation",
    "category.code": "http://terminology.hl7.org/CodeSystem/observation-category|vital-signs",  # noqa: E501
    "category.text": "Vital Signs",
    "effectiveDateTime": datetime.date(2012, 9, 17),
    "_effectiveDateTime.relativeDay": 2.0,
    "_effectiveDateTime.approximateDate": "2012-09",
    "performer": "Practitioner/example",
    "interpretation.code": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation|L",  # noqa: E501
    "interpretation.text": "Below low normal",
    "code.code": "http://loinc.org|85354-9",
    "code.text": "Blood pressure systolic & diastolic",
    "subject": "Patient/example",
    "bodySite.code": "http://snomed.info/sct|368209003",
    "bodySite.text": "Right arm",
}

OBSERVATION_DICT_OUT = {
    "resourceType": "Observation",
    "status": "final",
    "category": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",  # noqa: E501
                    "code": "vital-signs",
                    "display": "Vital Signs",
                }
            ]
        }
    ],
    "code": {
        "coding": [
            {
                "system": "http://loinc.org",
                "code": "85354-9",
                "display": "Blood pressure systolic & diastolic",
            }
        ],
    },
    "subject": {"reference": "Patient/example"},
    "effectiveDateTime": "2012-09-17T00:00:00",
    "_effectiveDateTime": {
        "extension": [
            {"url": "approximateDate", "valueDate": "2012-09"},
            {"url": "relativeDay", "valueInteger": 2},
        ]
    },
    "performer": [{"reference": "Practitioner/example"}],
    "interpretation": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",  # noqa: E501
                    "code": "L",
                    "display": "Below low normal",
                }
            ],
        }
    ],
    "bodySite": {
        "coding": [
            {
                "system": "http://snomed.info/sct",
                "code": "368209003",
                "display": "Right arm",
            }
        ]
    },
}


def test_observation_to_flat():
    bp = Observation(**OBSERVATION_DICT_INPUT)

    bp.to_flat("test_observation.parquet")

    assert_frame_equal(
        pd.read_parquet("test_observation.parquet"),
        pd.DataFrame(OBSERVATION_FLAT, index=[0]),
        check_like=True,  # ignore column order
    )
    os.remove("test_observation.parquet")


def test_observation_from_flat():
    bp = Observation(**OBSERVATION_DICT_OUT)

    flat_bp = Observation.from_flat("tests/data/observation_flat.parquet")

    assert bp == flat_bp
