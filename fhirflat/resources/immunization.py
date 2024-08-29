from __future__ import annotations

from typing import ClassVar, TypeAlias, Union

from fhir.resources import fhirtypes
from fhir.resources.immunization import Immunization as _Immunization
from fhir.resources.immunization import (
    ImmunizationPerformer,
    ImmunizationProgramEligibility,
    ImmunizationProtocolApplied,
    ImmunizationReaction,
)
from pydantic.v1 import Field, validator

from .base import FHIRFlatBase
from .extension_types import (
    dateTimeExtensionType,
    timingPhaseDetailType,
    timingPhaseType,
)
from .extensions import timingPhase, timingPhaseDetail

JsonString: TypeAlias = str


class Immunization(_Immunization, FHIRFlatBase):
    extension: list[
        Union[timingPhaseType, timingPhaseDetailType, fhirtypes.ExtensionType]
    ] = Field(
        None,
        alias="extension",
        title="List of `Extension` items (represented as `dict` in JSON)",
        description=(
            """
            Contains the G.H 'eventPhase' extension, and allows extensions from other
             implementations to be included."""
        ),
        # if property is element of this resource.
        element_property=True,
        # this trys to match the type of the object to each of the union types
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
        "basedOn",
        "statusReason",
        "administeredProduct",
        "lotNumber",
        "expirationDate",
        "supportingInformation",
        "primarySource",
        "informationSource",
        "performer",
        "note",
    }

    # required attributes that are not present in the FHIRflat representation
    flat_defaults: ClassVar[list[str]] = [*FHIRFlatBase.flat_defaults, "status"]

    backbone_elements: ClassVar[dict] = {
        "performer": ImmunizationPerformer,
        "programEligibility": ImmunizationProgramEligibility,
        "reaction": ImmunizationReaction,
        "protocolApplied": ImmunizationProtocolApplied,
    }

    @validator("extension")
    def validate_extension_contents(cls, extensions):
        timing_count = sum(isinstance(item, timingPhase) for item in extensions)
        detail_count = sum(isinstance(item, timingPhaseDetail) for item in extensions)

        if timing_count > 1 or detail_count > 1:
            raise ValueError("timingPhase and timingPhaseDetail can only appear once.")

        if timing_count > 0 and detail_count > 0:
            raise ValueError(
                "timingPhase and timingPhaseDetail cannot appear together."
            )

        return extensions

    @classmethod
    def cleanup(cls, data: dict) -> dict:
        """
        Apply resource-specific changes to references and default values
        """

        for field in (
            {"patient", "encounter", "location"}
            | {x for x in data.keys() if x.endswith(".reference")}
        ).intersection(data.keys()):
            data[field] = {"reference": data[field]}

        # add default status back in
        data["status"] = "completed"

        return data
