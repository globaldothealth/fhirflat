from __future__ import annotations

from typing import ClassVar, TypeAlias

from fhir.resources.medicationadministration import (
    MedicationAdministration as _MedicationAdministration,
)
from fhir.resources.medicationadministration import (
    MedicationAdministrationDosage,
    MedicationAdministrationPerformer,
)

from .base import FHIRFlatBase

JsonString: TypeAlias = str


class MedicationAdministration(_MedicationAdministration, FHIRFlatBase):
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
