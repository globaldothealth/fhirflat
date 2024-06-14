from __future__ import annotations

from typing import ClassVar, TypeAlias

import orjson
from fhir.resources.medicationadministration import (
    MedicationAdministration as _MedicationAdministration,
)
from pydantic.v1 import ValidationError

from fhirflat.flat2fhir import expand_concepts

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

    @classmethod
    def cleanup(
        cls, data_dict: JsonString | dict, json_data=True
    ) -> MedicationAdministration | ValidationError:
        """
        Load data into a dictionary-like structure, then
        apply resource-specific changes and unpack flattened data
        like codeableConcepts back into structured data.
        """
        if json_data and isinstance(data_dict, str):
            data: dict = orjson.loads(data_dict)
        elif isinstance(data_dict, dict):
            data: dict = data_dict

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

        data = expand_concepts(data, cls)

        # create lists for properties which are lists of FHIR types
        for field in [x for x in data.keys() if x in cls.attr_lists()]:
            if not isinstance(data[field], list):
                data[field] = [data[field]]

        try:
            return cls(**data)
        except ValidationError as e:
            return e
