from __future__ import annotations

from typing import ClassVar, TypeAlias, Union

from fhir.resources import fhirtypes
from fhir.resources.condition import Condition as _Condition
from fhir.resources.condition import ConditionParticipant, ConditionStage
from pydantic.v1 import Field, validator

from .base import FHIRFlatBase
from .extension_types import presenceAbsenceType, prespecifiedQueryType, timingPhaseType
from .extensions import presenceAbsence, prespecifiedQuery, timingPhase

JsonString: TypeAlias = str

# TODO: Update references to disallow "display" (could contain names)


class Condition(_Condition, FHIRFlatBase):
    extension: list[
        Union[
            presenceAbsenceType,
            prespecifiedQueryType,
            timingPhaseType,
            fhirtypes.ExtensionType,
        ]
    ] = Field(
        None,
        alias="extension",
        title="Additional content defined by implementations",
        description=(
            """
            Contains the G.H 'age' and 'birthSex' extensions,
            and allows extensions from other implementations to be included."""
        ),
        # if property is element of this resource.
        element_property=True,
        union_mode="smart",
    )

    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "id",
        "identifier",
        "verificationStatus",
        "evidence",
        "note",
        "participant",
    }

    backbone_elements: ClassVar[dict] = {
        "participant": ConditionParticipant,
        "stage": ConditionStage,
    }

    # required attributes that are not present in the FHIRflat representation
    flat_defaults: ClassVar[list[str]] = [*FHIRFlatBase.flat_defaults, "clinicalStatus"]

    @validator("extension")
    def validate_extension_contents(cls, extensions):
        present_count = sum(isinstance(item, presenceAbsence) for item in extensions)
        query_count = sum(isinstance(item, prespecifiedQuery) for item in extensions)
        timing_count = sum(isinstance(item, timingPhase) for item in extensions)

        if present_count > 1 or query_count > 1 or timing_count > 1:
            raise ValueError(
                "presenceAbsence, prespecifiedQuery and timingPhase can only appear"
                " once."
            )

        return extensions

    @classmethod
    def cleanup(cls, data: dict) -> dict:
        """
        Apply resource-specific changes to references and default values
        """

        for field in ({"subject", "encounter"}).intersection(data.keys()):
            data[field] = {"reference": data[field]}

        # add default clinicalStatus back in
        data["clinicalStatus"] = {
            "coding": [
                {
                    "system": (
                        "http://terminology.hl7.org/CodeSystem/condition-clinical"
                    ),
                    "code": "unknown",
                }
            ]
        }

        return data
