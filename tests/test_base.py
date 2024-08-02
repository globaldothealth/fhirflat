from fhirflat.resources.encounter import Encounter
import fhirflat
import pandas as pd
import pytest
from pydantic.v1 import ValidationError


def test_flat_fields():
    p = fhirflat.Patient()
    ff = p.flat_fields()

    assert ff == [
        "id",
        "extension",
        "gender",
        "birthDate",
        "deceasedBoolean",
        "deceasedDateTime",
        "maritalStatus",
        "multipleBirthBoolean",
        "multipleBirthInteger",
        "generalPractitioner",
        "managingOrganization",
    ]


def test_validate_fhirflat_single_resource_errors():
    df = pd.DataFrame(
        {
            "subjid": [2],
            "flat_dict": [
                {
                    "subject": "Patient/2",
                    "id": 11,
                    "actualPeriod.start": "NOT A DATE",
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
                    "diagnosis.use.system": [
                        "https://snomed.info/sct",
                        "https://snomed.info/sct",
                    ],
                    "diagnosis.use.code": [89100005.0, 89100005.0],
                    "diagnosis.use.text": [
                        "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                        "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                    ],
                    "admission.dischargeDisposition.system": "https://snomed.info/sct",
                    "admission.dischargeDisposition.code": 371827001.0,
                    "admission.dischargeDisposition.text": "Patient discharged alive (finding)",  # noqa: E501
                }
            ],
        },
        index=[0],
    )

    flat_df = Encounter.ingest_to_flat(df)
    with pytest.raises(ValidationError, match="invalid datetime format"):
        _, _ = Encounter.validate_fhirflat(flat_df)


def test_validate_fhirflat_multi_resource_errors():
    df = pd.DataFrame(
        {
            "subjid": [1, 2],
            "flat_dict": [
                {
                    "subject": "Patient/1",
                    "id": 11,
                    "actualPeriod.start": "2021-04-01",
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
                    "diagnosis.use.system": [
                        "https://snomed.info/sct",
                        "https://snomed.info/sct",
                    ],
                    "diagnosis.use.code": [89100005.0, 89100005.0],
                    "diagnosis.use.text": [
                        "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                        "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                    ],
                    "admission.dischargeDisposition.system": "https://snomed.info/sct",
                    "admission.dischargeDisposition.code": 371827001.0,
                    "admission.dischargeDisposition.text": "Patient discharged alive (finding)",  # noqa: E501
                },
                {
                    "subject": "Patient/2",
                    "id": 12,
                    "actualPeriod.start": ["2021-04-01", None],
                    "actualPeriod.end": [None, "2021-04-10"],
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
                    "diagnosis.use.system": [
                        "https://snomed.info/sct",
                        "https://snomed.info/sct",
                    ],
                    "diagnosis.use.code": [89100005.0, 89100005.0],
                    "diagnosis.use.text": [
                        "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                        "Final diagnosis (discharge) (contextual qualifier) (qualifier value)",  # noqa: E501
                    ],
                    "admission.dischargeDisposition.system": "https://snomed.info/sct",
                    "admission.dischargeDisposition.code": 371827001.0,
                    "admission.dischargeDisposition.text": "Patient discharged alive (finding)",  # noqa: E501
                },
            ],
        },
    )
    flat_df = Encounter.ingest_to_flat(df)

    assert "diagnosis_dense" in flat_df.columns

    valid, errors = Encounter.validate_fhirflat(flat_df)

    assert len(valid) == 1
    assert len(errors) == 1
    assert (
        repr(errors["validation_error"][1].errors())
        == "[{'loc': ('actualPeriod', 'end'), 'msg': 'invalid type; expected datetime, string, bytes, int or float', 'type': 'type_error'}, {'loc': ('actualPeriod', 'start'), 'msg': 'invalid type; expected datetime, string, bytes, int or float', 'type': 'type_error'}]"  # noqa: E501
    )
