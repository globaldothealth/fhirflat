from __future__ import annotations

from typing import ClassVar, TypeAlias, Union

from fhir.resources import fhirtypes
from fhir.resources.patient import (
    Patient as _Patient,
)
from fhir.resources.patient import (
    PatientCommunication,
    PatientContact,
    PatientLink,
)
from pydantic.v1 import Field, validator

from .base import FHIRFlatBase
from .extension_types import ageType, birthSexType, raceType
from .extensions import Age, Race, birthSex

JsonString: TypeAlias = str


class Patient(_Patient, FHIRFlatBase):
    extension: list[Union[ageType, birthSexType, raceType, fhirtypes.ExtensionType]] = (
        Field(
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
    )

    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "identifier",
        "active",
        "name",
        "telecom",
        "address",
        "photo",
        "contact",
        "communication",
        "link",
    }

    backbone_elements: ClassVar[dict] = {
        "contact": PatientContact,
        "communication": PatientCommunication,
        "link": PatientLink,
    }

    @validator("extension")
    def validate_extension_contents(cls, extensions):
        age_count = sum(isinstance(item, Age) for item in extensions)
        birthsex_count = sum(isinstance(item, birthSex) for item in extensions)
        race_count = sum(isinstance(item, Race) for item in extensions)

        if age_count > 1 or birthsex_count > 1 or race_count > 1:
            raise ValueError("Age, birthSex and Race can only appear once.")

        return extensions

    @classmethod
    def cleanup(cls, data: dict) -> dict:
        """
        Apply resource-specific changes to references and default values
        """

        data["id"] = str(data["id"])

        # Strip time from the birthDate
        if "birthDate" in data:
            data["birthDate"] = data["birthDate"].split("T", 1)[0]

        return data
