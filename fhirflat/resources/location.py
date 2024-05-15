from __future__ import annotations
from fhir.resources.location import Location as _Location
from .base import FHIRFlatBase
import orjson

from ..flat2fhir import expand_concepts
from typing import TypeAlias, ClassVar

JsonString: TypeAlias = str


class Location(_Location, FHIRFlatBase):

    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions + (
        "id",
        "identifier",
        "status",
        "contact",  # phone numbers, addresses,
        "hoursOfOperation",
    )

    @classmethod
    def cleanup(cls, data: JsonString | dict, json_data=True) -> Location:
        """
        Load data into a dictionary-like structure, then
        apply resource-specific changes and unpack flattened data
        like codeableConcepts back into structured data.
        """
        if json_data:
            data = orjson.loads(data)

        for field in {
            "managingOrganization",
            "partOf",
            "endpoint",
        }.intersection(data.keys()):
            data[field] = {"reference": data[field]}

        data = expand_concepts(data, cls)

        # create lists for properties which are lists of FHIR types
        for field in [x for x in data.keys() if x in cls.attr_lists()]:
            if type(data[field]) is not list:
                data[field] = [data[field]]

        return cls(**data)
