from __future__ import annotations

from typing import ClassVar

from fhir.resources.diagnosticreport import DiagnosticReport as _DiagnosticReport

from fhirflat.flat2fhir import expand_concepts

from .base import FHIRFlatBase


class DiagnosticReport(_DiagnosticReport, FHIRFlatBase):
    # attributes to exclude from the flat representation
    flat_exclusions: ClassVar[set[str]] = FHIRFlatBase.flat_exclusions | {"status"}

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
