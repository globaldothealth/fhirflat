from __future__ import annotations

from typing import ClassVar, TypeAlias

from fhir.resources.organization import (
    Organization as _Organization,
)
from fhir.resources.organization import (
    OrganizationQualification,
)

from .base import FHIRFlatBase

JsonString: TypeAlias = str


class Organization(_Organization, FHIRFlatBase):
    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "id",
        "identifier",
        "active",
        "contact",  # phone numbers, addresses
    }

    backbone_elements: ClassVar[dict] = {"qualification": OrganizationQualification}

    @classmethod
    def cleanup(cls, data: dict) -> dict:
        """
        Apply resource-specific changes to references and default values
        """

        for field in {
            "partOf",
            "endpoint",
            "qualification.issuer",
        }.intersection(data.keys()):
            data[field] = {"reference": data[field]}

        # add default status back in
        data["active"] = True

        return data
