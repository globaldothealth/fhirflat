from __future__ import annotations

from typing import ClassVar, TypeAlias, Union

import orjson
from fhir.resources import fhirtypes
from fhir.resources.observation import Observation as _Observation
from fhir.resources.observation import ObservationComponent as _ObservationComponent
from pydantic.v1 import Field, validator

from fhirflat.flat2fhir import expand_concepts

from .base import FHIRFlatBase
from .extension_types import dateTimeExtensionType, timingPhaseType
from .extensions import timingPhase

JsonString: TypeAlias = str


class ObservationComponent(_ObservationComponent):
    """
    Adds the dateTime extension into the Observation.component class
    """

    valueDateTime__ext: dateTimeExtensionType = Field(
        None,
        alias="_effectiveDateTime",
        title="Extension field for ``effectiveDateTime``.",
    )


class Observation(_Observation, FHIRFlatBase):

    extension: list[Union[timingPhaseType, fhirtypes.ExtensionType]] = Field(
        None,
        alias="extension",
        title="List of `Extension` items (represented as `dict` in JSON)",
        description=(
            """
            Contains the G.H 'eventPhase' extension, and allows extensions from other
             implementations to be included."""
        ),
        # if property is element of this resource.
        element_property=True,
        # this trys to match the type of the object to each of the union types
        union_mode="smart",
    )

    effectiveDateTime__ext: dateTimeExtensionType = Field(
        None,
        alias="_effectiveDateTime",
        title="Extension field for ``effectiveDateTime``.",
    )

    # Update component to include the dateTime extension
    component: list[ObservationComponent] = Field(
        None,
        alias="component",
        title="Component results",
        description=(
            "Some observations have multiple component observations.  These "
            "component observations are expressed as separate code value pairs that"
            " share the same attributes.  Examples include systolic and diastolic "
            "component observations for blood pressure measurement and multiple "
            "component observations for genetics observations."
        ),
        # if property is element of this resource.
        element_property=True,
    )

    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "id",
        "identifier",
        "instantiatesCanonical",
        "instantiatesReference",
        "basedOn",
        "focus",
        "referenceRange",
        "issued",
        "note",
    }

    # required attributes that are not present in the FHIRflat representation
    flat_defaults: ClassVar[list[str]] = [*FHIRFlatBase.flat_defaults, "status"]

    @validator("extension")
    def validate_extension_contents(cls, extensions):
        phase_count = sum(isinstance(item, timingPhase) for item in extensions)

        if phase_count > 1:
            raise ValueError("timingPhase can only appear once.")

        return extensions

    @classmethod
    def cleanup(cls, data_dict: JsonString | dict, json_data=True) -> Observation:
        """
        Load data into a dictionary-like structure, then
        apply resource-specific changes and unpack flattened data
        like codeableConcepts back into structured data.
        """
        if json_data and isinstance(data_dict, str):
            data: dict = orjson.loads(data_dict)
        elif isinstance(data_dict, dict):
            data: dict = data_dict

        for field in {
            "encounter",
            "subject",
            "performer",
            "bodyStructure",
            "specimen",
            "device",
        }.intersection(data.keys()):
            data[field] = {"reference": str(data[field])}

        # add default status back in
        data["status"] = "final"

        data = expand_concepts(data, cls)

        # create lists for properties which are lists of FHIR types
        for field in [x for x in data.keys() if x in cls.attr_lists()]:
            if not isinstance(data[field], list):
                data[field] = [data[field]]

        return cls(**data)
