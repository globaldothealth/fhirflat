from __future__ import annotations

from typing import ClassVar, Union

from fhir.resources import fhirtypes
from fhir.resources.diagnosticreport import (
    DiagnosticReport as _DiagnosticReport,
)
from fhir.resources.diagnosticreport import (
    DiagnosticReportMedia,
    DiagnosticReportSupportingInfo,
)
from pydantic.v1 import Field, validator

from fhirflat.flat2fhir import expand_concepts

from .base import FHIRFlatBase
from .extension_types import timingPhaseDetailType, timingPhaseType
from .extensions import timingPhase, timingPhaseDetail


class DiagnosticReport(_DiagnosticReport, FHIRFlatBase):
    extension: list[
        Union[timingPhaseType, timingPhaseDetailType, fhirtypes.ExtensionType]
    ] = Field(
        None,
        alias="extension",
        title="List of `Extension` items (represented as `dict` in JSON)",
        description=(
            """
            Contains the Global.health 'timingPhase' extension,
            and allows extensions from other implementations to be included.
            """
        ),
        # if property is element of this resource.
        element_property=True,
        # this trys to match the type of the object to each of the union types
        union_mode="smart",
    )

    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {
        "identifier",
    }

    # required attributes that are not present in the FHIRflat representation
    flat_defaults: ClassVar[list[str]] = [*FHIRFlatBase.flat_defaults, "status"]

    backbone_elements: ClassVar[dict] = {
        "supportingInfo": DiagnosticReportSupportingInfo,
        "media": DiagnosticReportMedia,
    }

    @validator("extension")
    def validate_extension_contents(cls, extensions):
        timing_count = sum(isinstance(item, timingPhase) for item in extensions)
        detail_count = sum(isinstance(item, timingPhaseDetail) for item in extensions)

        if timing_count > 1:
            raise ValueError("timingPhase can only appear once.")

        if detail_count > 1:
            raise ValueError("timingPhaseDetail can only appear once.")

        if timing_count > 0 and detail_count > 0:
            raise ValueError(
                "timingPhase and timingPhaseDetail cannot appear together."
            )

        return extensions

    @classmethod
    def cleanup(cls, data: dict) -> dict:
        """
        Apply resource-specific changes to references and default values
        """

        for field in (
            {
                "basedOn",
                "subject",
                "performer",
                "resultsInterpreter",
                "specimen",
                "result",
                "study",
                "composition",
            }
            | {x for x in data.keys() if x.endswith(".reference")}
        ).intersection(data.keys()):
            data[field] = {"reference": data[field]}

        # add default status back in
        data["status"] = "final"

        data = expand_concepts(data, cls)

        return data
