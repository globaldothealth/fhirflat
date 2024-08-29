from __future__ import annotations

from typing import ClassVar, TypeAlias, Union

from fhir.resources import fhirtypes
from fhir.resources.procedure import (
    Procedure as _Procedure,
)
from fhir.resources.procedure import (
    ProcedureFocalDevice,
    ProcedurePerformer,
)
from pydantic.v1 import Field, validator

from .base import FHIRFlatBase
from .extension_types import (
    dateTimeExtensionType,
    durationType,
    presenceAbsenceType,
    prespecifiedQueryType,
    relativePeriodType,
    timingPhaseDetailType,
    timingPhaseType,
)
from .extensions import (
    Duration,
    presenceAbsence,
    prespecifiedQuery,
    relativePeriod,
    timingPhase,
    timingPhaseDetail,
)

JsonString: TypeAlias = str


class Procedure(_Procedure, FHIRFlatBase):
    extension: list[
        Union[
            durationType,
            timingPhaseType,
            timingPhaseDetailType,
            relativePeriodType,
            presenceAbsenceType,
            prespecifiedQueryType,
            fhirtypes.ExtensionType,
        ]
    ] = Field(
        None,
        alias="extension",
        title="Additional content defined by implementations",
        description=(
            """
            Contains the G.H 'timingPhase', 'timingPhaseDetail', 'relativePeriod' and
             'duration' extensions, and allows extensions from other implementations to
             be included."""
        ),
        # if property is element of this resource.
        element_property=True,
        union_mode="smart",
    )

    occurrenceDateTime__ext: dateTimeExtensionType = Field(
        None,
        alias="_occurrenceDateTime",
        title="Extension field for ``occurrenceDateTime``.",
    )

    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "id",
        "identifier",
        "instantiatesCanonical",
        "instantiatesUri",
        "basedOn",
        "statusReason",
        "focus",
        "reportedBoolean",
        "reportedReference",
        "performer",
        "reason",
        "note",
        "supportingInfo",
    }

    # required attributes that are not present in the FHIRflat representation
    flat_defaults: ClassVar[list[str]] = [*FHIRFlatBase.flat_defaults, "status"]

    backbone_elements: ClassVar[dict] = {
        "performer": ProcedurePerformer,
        "focalDevice": ProcedureFocalDevice,
    }

    @validator("extension")
    def validate_extension_contents(cls, extensions):
        duration_count = sum(isinstance(item, Duration) for item in extensions)
        tim_phase_count = sum(isinstance(item, timingPhase) for item in extensions)
        rel_phase_count = sum(isinstance(item, relativePeriod) for item in extensions)
        detail_count = sum(isinstance(item, timingPhaseDetail) for item in extensions)
        pa_count = sum(isinstance(item, presenceAbsence) for item in extensions)
        pq_count = sum(isinstance(item, prespecifiedQuery) for item in extensions)

        if (
            duration_count > 1
            or tim_phase_count > 1
            or rel_phase_count > 1
            or detail_count > 1
            or pa_count > 1
            or pq_count > 1
        ):
            raise ValueError("Each extension can only appear once.")

        if tim_phase_count > 0 and detail_count > 0:
            raise ValueError(
                "timingPhase and timingPhaseDetail cannot appear together."
            )

        return extensions

    @classmethod
    def cleanup(cls, data: dict) -> dict:
        """
        Apply resource-specific changes to references and default values
        """

        for field in {
            "partOf",
            "encounter",
            "subject",
            "recorder",
            "performer",
            "location",
            "report",
        }.intersection(data.keys()):
            data[field] = {"reference": data[field]}

        # add default status back in
        data["status"] = "completed"

        return data
