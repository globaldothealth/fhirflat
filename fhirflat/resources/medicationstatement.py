from __future__ import annotations

from typing import ClassVar, TypeAlias, Union

from fhir.resources import fhirtypes
from fhir.resources.medicationstatement import (
    MedicationStatement as _MedicationStatement,
)
from pydantic.v1 import Field, validator

from .base import FHIRFlatBase
from .extension_types import (
    presenceAbsenceType,
    prespecifiedQueryType,
    timingPhaseDetailType,
)
from .extensions import presenceAbsence, prespecifiedQuery, timingPhaseDetail

JsonString: TypeAlias = str


class MedicationStatement(_MedicationStatement, FHIRFlatBase):
    extension: list[
        Union[
            presenceAbsenceType,
            prespecifiedQueryType,
            timingPhaseDetailType,
            fhirtypes.ExtensionType,
        ]
    ] = Field(
        None,
        alias="extension",
        title="Additional content defined by implementations",
        description=(
            """
            Contains the G.H 'presenceAbsence', 'prespecifiedQuery' and
            'timingPhaseDetail' extensions, and allows extensions from other
            implementations to be included.
            """
        ),
        # if property is element of this resource.
        element_property=True,
        union_mode="smart",
    )

    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "id",
        "identifier",
        "informationSource",
        "note",
    }

    # required attributes that are not present in the FHIRflat representation
    flat_defaults: ClassVar[list[str]] = [*FHIRFlatBase.flat_defaults, "status"]

    @validator("extension")
    def validate_extension_contents(cls, extensions):
        pa_count = sum(isinstance(item, presenceAbsence) for item in extensions)
        pq_count = sum(isinstance(item, prespecifiedQuery) for item in extensions)
        tpd_count = sum(isinstance(item, timingPhaseDetail) for item in extensions)

        if pa_count > 1 or pq_count > 1 or tpd_count > 1:
            raise ValueError(
                "presenceAbsence, prespecifiedQuery and timingPhaseDetail can only "
                "appear once."
            )

        return extensions

    @classmethod
    def cleanup(cls, data: dict) -> dict:
        """
        Apply resource-specific changes to references and default values
        """

        for field in (
            {
                "partOf",
                "subject",
                "encounter",
                "derivedFrom",
                "relatedClinicalInformation",
            }
            | {x for x in data.keys() if x.endswith(".reference")}
        ).intersection(data.keys()):
            data[field] = {"reference": data[field]}

        # add default status back in
        data["status"] = "recorded"

        return data
