from __future__ import annotations

from typing import ClassVar, TypeAlias

from fhir.resources.location import Location as _Location
from fhir.resources.location import LocationPosition

from .base import FHIRFlatBase

JsonString: TypeAlias = str


class Location(_Location, FHIRFlatBase):
    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "id",
        "identifier",
        "status",
        "contact",  # phone numbers, addresses,
        "hoursOfOperation",
    }

    backbone_elements: ClassVar[dict] = {"position": LocationPosition}

    @classmethod
    def cleanup(cls, data: dict) -> dict:
        """
        Apply resource-specific changes to references and default values
        """
        for field in {
            "managingOrganization",
            "partOf",
            "endpoint",
        }.intersection(data.keys()):
            data[field] = {"reference": data[field]}

        return data
