from __future__ import annotations

from typing import ClassVar, TypeAlias

from fhir.resources.researchsubject import (
    ResearchSubject as _ResearchSubject,
)
from fhir.resources.researchsubject import (
    ResearchSubjectProgress,
)

from .base import FHIRFlatBase

JsonString: TypeAlias = str


class ResearchSubject(_ResearchSubject, FHIRFlatBase):
    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "id",
        "identifier",
    }

    # required attributes that are not present in the FHIRflat representation
    flat_defaults: ClassVar[list[str]] = [*FHIRFlatBase.flat_defaults, "status"]

    backbone_elements: ClassVar[dict] = {"progress": ResearchSubjectProgress}

    @classmethod
    def cleanup(cls, data: dict) -> dict:
        """
        Apply resource-specific changes to references and default values
        """

        for field in (
            {"study", "subject", "consent"}
            | {x for x in data.keys() if x.endswith(".reference")}
        ).intersection(data.keys()):
            data[field] = {"reference": data[field]}

        # add default status back in
        data["status"] = "active"

        return data
