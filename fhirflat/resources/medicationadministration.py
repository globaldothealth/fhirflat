from __future__ import annotations

from typing import ClassVar, TypeAlias, Union

from fhir.resources import fhirtypes
from fhir.resources.medicationadministration import (
    MedicationAdministration as _MedicationAdministration,
)
from fhir.resources.medicationadministration import (
    MedicationAdministrationDosage,
    MedicationAdministrationPerformer,
)
from pydantic.v1 import Field, validator

from .base import FHIRFlatBase
from .extension_types import (
    durationType,
    presenceAbsenceType,
    prespecifiedQueryType,
    timingPhaseDetailType,
    timingPhaseType,
)
from .extensions import (
    Duration,
    presenceAbsence,
    prespecifiedQuery,
    timingPhase,
    timingPhaseDetail,
)

JsonString: TypeAlias = str


class MedicationAdministration(_MedicationAdministration, FHIRFlatBase):
    extension: list[
        Union[
            presenceAbsenceType,
            prespecifiedQueryType,
            timingPhaseType,
            timingPhaseDetailType,
            durationType,
            fhirtypes.ExtensionType,
        ]
    ] = Field(
        None,
        alias="extension",
        title="Additional content defined by implementations",
        description=(
            """
            Contains the G.H 'presenceAbsence', 'prespecifiedQuery', 'duration' and
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
        "basedOn",
        "performer",
        "note",
    }

    # required attributes that are not present in the FHIRflat representation
    flat_defaults: ClassVar[list[str]] = [*FHIRFlatBase.flat_defaults, "status"]

    @validator("extension")
    def validate_extension_contents(cls, extensions):
        pa_count = sum(isinstance(item, presenceAbsence) for item in extensions)
        pq_count = sum(isinstance(item, prespecifiedQuery) for item in extensions)
        tp_count = sum(isinstance(item, timingPhase) for item in extensions)
        tpd_count = sum(isinstance(item, timingPhaseDetail) for item in extensions)
        dur_count = sum(isinstance(item, Duration) for item in extensions)

        if (
            pa_count > 1
            or pq_count > 1
            or tp_count > 1
            or tpd_count > 1
            or dur_count > 1
        ):
            raise ValueError("Each extension can can only appear once.")

        if tp_count > 0 and tpd_count > 0:
            raise ValueError(
                "timingPhase and timingPhaseDetail cannot appear together."
            )

        return extensions

    backbone_elements: ClassVar[dict] = {
        "performer": MedicationAdministrationPerformer,
        "dosage": MedicationAdministrationDosage,
    }

    @classmethod
    def cleanup(cls, data: dict) -> dict:
        """
        Apply resource-specific changes to references and default values
        """

        for field in (
            {
                "basedOn",
                "partOf",
                "subject",
                "encounter",
                "supportingInformation",
                "request",
                "eventHistory",
            }
            | {x for x in data.keys() if x.endswith(".reference")}
        ).intersection(data.keys()):
            data[field] = {"reference": data[field]}

        # add default status back in
        data["status"] = "completed"

        return data
