from __future__ import annotations

from typing import ClassVar, TypeAlias

from fhir.resources.medicationstatement import (
    MedicationStatement as _MedicationStatement,
)
from fhir.resources.medicationstatement import (
    MedicationStatementAdherence,
)

from .base import FHIRFlatBase

JsonString: TypeAlias = str


class MedicationStatement(_MedicationStatement, FHIRFlatBase):
    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "id",
        "identifier",
        "informationSource",
        "note",
    }

    # required attributes that are not present in the FHIRflat representation
    flat_defaults: ClassVar[list[str]] = [*FHIRFlatBase.flat_defaults, "status"]

    backbone_elements: ClassVar[dict] = {"adherence": MedicationStatementAdherence}

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
