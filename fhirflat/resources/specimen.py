from __future__ import annotations

from typing import ClassVar, TypeAlias

from fhir.resources.specimen import (
    Specimen as _Specimen,
)
from fhir.resources.specimen import (
    SpecimenCollection,
    SpecimenContainer,
    SpecimenFeature,
    SpecimenProcessing,
)

from fhirflat.flat2fhir import expand_concepts

from .base import FHIRFlatBase

JsonString: TypeAlias = str


class Specimen(_Specimen, FHIRFlatBase):
    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "id",
        "identifier",
        "accessionIdentifier",
        "status",
        "note",
    }

    backbone_elements: ClassVar[dict] = {
        "feature": SpecimenFeature,
        "collection": SpecimenCollection,
        "processing": SpecimenProcessing,
        "container": SpecimenContainer,
    }

    @classmethod
    def cleanup(cls, data: dict) -> dict:
        """
        Apply resource-specific changes to references and default values
        """

        for field in (
            {
                "subject",
                "parent",
                "request",
                "collection.collector",
                "collection.procedure",
                "container.device",
                "container.location",
            }
            | {x for x in data.keys() if x.endswith(".reference")}
        ).intersection(data.keys()):
            data[field] = {"reference": data[field]}

        data = expand_concepts(data, cls)

        return data
