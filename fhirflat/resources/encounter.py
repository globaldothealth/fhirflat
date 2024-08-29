from __future__ import annotations

from typing import ClassVar, TypeAlias, Union

from fhir.resources import fhirtypes
from fhir.resources.encounter import Encounter as _Encounter
from fhir.resources.encounter import (
    EncounterAdmission,
    EncounterDiagnosis,
    EncounterLocation,
    EncounterParticipant,
    EncounterReason,
)
from pydantic.v1 import Field, validator

from .base import FHIRFlatBase
from .extension_types import relativePeriodType, timingPhaseDetailType, timingPhaseType
from .extensions import relativePeriod, timingPhase, timingPhaseDetail

JsonString: TypeAlias = str


class Encounter(_Encounter, FHIRFlatBase):
    extension: list[
        Union[
            relativePeriodType,
            timingPhaseType,
            timingPhaseDetailType,
            fhirtypes.ExtensionType,
        ]
    ] = Field(
        None,
        alias="extension",
        title="List of `Extension` items (represented as `dict` in JSON)",
        description=(
            """
            Contains the Global.health 'eventTiming' and 'relativePeriod' extensions,
            and allows extensions from other implementations to be included.
            """
        ),
        # if property is element of this resource.
        element_property=True,
        # this trys to match the type of the object to each of the union types
        union_mode="smart",
    )

    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "identifier",
        "participant",  # participants other than the patient
        "appointment",  # appointment that scheduled the encounter
        "account",  # relates to billing
        "dietPreference",
        "specialArrangement",  # if translator, streatcher, wheelchair etc. needed
        "specialCourtesy",  # contains ID information, VIP, board member, etc.
    }

    # required attributes that are not present in the FHIRflat representation
    flat_defaults: ClassVar[list[str]] = [*FHIRFlatBase.flat_defaults, "status"]

    backbone_elements: ClassVar[dict] = {
        "participant": EncounterParticipant,
        "reason": EncounterReason,
        "diagnosis": EncounterDiagnosis,
        "admission": EncounterAdmission,
        "location": EncounterLocation,
    }

    @validator("extension")
    def validate_extension_contents(cls, extensions):
        rel_phase_count = sum(isinstance(item, relativePeriod) for item in extensions)
        timing_count = sum(isinstance(item, timingPhase) for item in extensions)
        detail_count = sum(isinstance(item, timingPhaseDetail) for item in extensions)

        if rel_phase_count > 1 or timing_count > 1 or detail_count > 1:
            raise ValueError(
                "relativePeriod, timingPhase and timingPhaseDetail can only appear once."  # noqa E501
            )

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

        for field in {
            "subject",
            "episodeOfCare",
            "basedOn",
            "careTeam",
            "partOf",
            "serviceProvider",
            "admission.destination",
            "admission.origin",
        }.intersection(data.keys()):
            data[field] = {"reference": data[field]}

        # add default status back in
        data["status"] = "completed"

        return data
