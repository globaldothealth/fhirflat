"""
Extensions to the base `FHIR resources`_ package that are ISARIC specific.

.. _FHIR resources: https://pypi.org/project/fhir.resources/

"""

from __future__ import annotations

from typing import Any, Union

from fhir.resources import fhirtypes
from fhir.resources.datatype import DataType as _DataType
from fhir.resources.fhirprimitiveextension import (
    FHIRPrimitiveExtension as _FHIRPrimitiveExtension,
)
from pydantic.v1 import Field, root_validator, validator

from . import extension_types as et

# --------- extensions ------------------------------


class timingPhase(_DataType):
    """
    An ISARIC extension collecting data on the phase of admission an event occurred.
    This is typically one of:
    - Pre-admission
    - Admission (i.e. during the hospital stay)
    - Follow-up
    with an appropriate SNOMED (or similar) code.
    """

    resource_type: str = Field(default="timingPhase", const=True)

    url: str = Field("timingPhase", const=True, alias="url")

    valueCodeableConcept: fhirtypes.CodeableConceptType = Field(
        None,
        alias="valueCodeableConcept",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        element_required=True,
    )

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return [
            "id",
            "extension",
            "url",
            "valueCodeableConcept",
        ]


class relativeDay(_DataType):
    """
    An ISARIC extension recording the day an event occurred relative to the admission
    date. For a resources such as Encounter or Procedure, use relativePeriod to record
    both the relative start and end dates instead.
    """

    resource_type: str = Field(default="relativeDay", const=True)

    url: str = Field("relativeDay", const=True, alias="url")

    valueInteger: fhirtypes.Integer = Field(
        None,
        alias="valueInteger",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        element_required=True,
    )

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return [
            "id",
            "extension",
            "url",
            "valueInteger",
        ]


class relativeStart(_DataType):
    """
    An ISARIC extension for use inside the complex `relativePeriod` extension.
    """

    resource_type: str = Field(default="relativeStart", const=True)

    url: str = Field("relativeStart", const=True, alias="url")

    valueInteger: fhirtypes.Integer = Field(
        None,
        alias="valueInteger",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        element_required=True,
    )

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return [
            "id",
            "extension",
            "url",
            "valueInteger",
        ]


class relativeEnd(_DataType):
    """
    An ISARIC extension for use inside the complex `relativePeriod` extension.
    """

    resource_type: str = Field(default="relativeEnd", const=True)

    url: str = Field("relativeEnd", const=True, alias="url")

    valueInteger: fhirtypes.Integer = Field(
        None,
        alias="valueInteger",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        element_required=True,
    )

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return [
            "id",
            "extension",
            "url",
            "valueInteger",
        ]


class relativePeriod(_DataType):
    """
    An ISARIC extension recording the start and end dates an event occurred relative to
    the admission date.

    relativePeriod is comprised of two components: relativeStart, denoting the
    start of the period and relativeEnd, denoting the end of the period.

    E.g. a an Encounter that starts on the 5th of Jan, the same day as admission, and
    ends on the 10th, would have a relativePeriod extension where relativeStart is 1 and
    relativeEnd is 5.
    """

    resource_type: str = Field(default="relativePeriod", const=True)

    url: str = Field("relativePeriod", const=True, alias="url")

    extension: list[Union[et.relativeStartType, et.relativeEndType]] = Field(
        None,
        alias="extension",
        title="List of `Extension` items (represented as `dict` in JSON)",
        description="Additional content defined by implementations",
        # if property is element of this resource.
        element_property=True,
        # this trys to match the type of the object to each of the union types
        union_mode="smart",
    )

    @validator("extension")
    def validate_extension_contents(cls, extensions):
        start_count = sum(isinstance(item, relativeStart) for item in extensions)
        end_count = sum(isinstance(item, relativeEnd) for item in extensions)

        if start_count > 1 or end_count > 1:
            raise ValueError("relativeStart and relativeEnd can only appear once.")

        return extensions

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return [
            "id",
            "extension",
            "url",
        ]


class approximateDate(_DataType):
    """
    An ISARIC extension for recording the approximate date (if the true date is unknown)
    or timeframe of an event.

    E.g. a Follow-up encounter that occured 3 months after admission would have an
    approximateDate extension with a valueString of "3 months".
    """

    resource_type: str = Field(default="approximateDate", const=True)

    url: str = Field("approximateDate", const=True, alias="url")

    valueDate: fhirtypes.Date = Field(
        None,
        alias="valueDate",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        # Choice of Data Types. i.e value[x]
        one_of_many="value",
        one_of_many_required=True,
    )

    valueString: fhirtypes.String = Field(
        None,
        alias="valueString",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        # Choice of Data Types. i.e value[x]
        one_of_many="value",
        one_of_many_required=True,
    )

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return ["id", "extension", "url", "valueDate", "valueString"]

    @root_validator(pre=True, allow_reuse=True)
    def validate_one_of_many_1136(cls, values: dict[str, Any]) -> dict[str, Any]:
        """https://www.hl7.org/fhir/formats.html#choice
        A few elements have a choice of more than one data type for their content.
        All such elements have a name that takes the form nnn[x].
        The "nnn" part of the name is constant, and the "[x]" is replaced with
        the title-cased name of the type that is actually used.
        The table view shows each of these names explicitly.

        Elements that have a choice of data type cannot repeat - they must have a
        maximum cardinality of 1. When constructing an instance of an element with a
        choice of types, the authoring system must create a single element with a
        data type chosen from among the list of permitted data types.
        """
        one_of_many_fields = {
            "value": [
                "valueDate",
                "valueString",
            ]
        }
        for prefix, fields in one_of_many_fields.items():
            assert cls.__fields__[fields[0]].field_info.extra["one_of_many"] == prefix
            required = (
                cls.__fields__[fields[0]].field_info.extra["one_of_many_required"]
                is True
            )
            found = False
            for field in fields:
                if field in values and values[field] is not None:
                    if found is True:
                        raise ValueError(
                            "Any of one field value is expected from "
                            f"this list {fields}, but got multiple!"
                        )
                    else:
                        found = True
            if required is True and found is False:
                raise ValueError(f"Expect any of field value from this list {fields}.")

        return values


class Duration(_DataType):
    """
    An ISARIC extension for recording the length of an event (e.g. 5 days) where
    duration is not an option in the base FHIR specification.
    """

    resource_type: str = Field(default="Duration", const=True)

    url: str = Field("duration", const=True, alias="url")

    valueQuantity: fhirtypes.QuantityType = Field(
        None,
        alias="valueQuantity",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        element_required=True,
    )

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return [
            "id",
            "extension",
            "url",
            "valueQuantity",
        ]


class Age(_DataType):
    """
    An ISARIC extension collecting data on the age of a patient.
    """

    resource_type: str = Field(default="Age", const=True)

    url: str = Field("age", const=True, alias="url")

    valueQuantity: fhirtypes.QuantityType = Field(
        None,
        alias="valueQuantity",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        element_required=True,
    )

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return [
            "id",
            "extension",
            "url",
            "valueQuantity",
        ]


class birthSex(_DataType):
    """
    An ISARIC extension collecting data on the birth sex of a patient.
    """

    resource_type: str = Field(default="birthSex", const=True)

    url: str = Field("birthSex", const=True, alias="url")

    valueCodeableConcept: fhirtypes.CodeableConceptType = Field(
        None,
        alias="valueCodeableConcept",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        element_required=True,
    )

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return [
            "id",
            "extension",
            "url",
            "valueCodeableConcept",
        ]


class Race(_DataType):
    """
    An ISARIC extension collecting data on the race of a patient.
    """

    resource_type: str = Field(default="Race", const=True)

    url: str = Field("race", const=True, alias="url")

    valueCodeableConcept: fhirtypes.CodeableConceptType = Field(
        None,
        alias="valueCodeableConcept",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        element_required=True,
    )

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return [
            "id",
            "extension",
            "url",
            "valueCodeableConcept",
        ]


class presenceAbsence(_DataType):
    """
    An ISARIC extension to indicate if a clinical finding is present, absent or unknown.
    """

    resource_type: str = Field(default="presenceAbsence", const=True)

    url: str = Field("presenceAbsence", const=True, alias="url")

    valueCodeableConcept: fhirtypes.CodeableConceptType = Field(
        None,
        alias="valueCodeableConcept",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        element_required=True,
    )

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return [
            "id",
            "extension",
            "url",
            "valueCodeableConcept",
        ]


class prespecifiedQuery(_DataType):
    """
    An ISARIC extension to indicate if a finding is the result of a prespecified query.
    """

    resource_type: str = Field(default="prespecifiedQuery", const=True)

    url: str = Field("prespecifiedQuery", const=True, alias="url")

    valueBoolean: bool = Field(
        None,
        alias="valueBoolean",
        title="Value of extension",
        description=(
            "Value of extension - must be one of a constrained set of the data "
            "types (see [Extensibility](extensibility.html) for a list)."
        ),
        # if property is element of this resource.
        element_property=True,
        elementRequired=True,
    )

    @classmethod
    def elements_sequence(cls):
        """returning all elements names from
        ``Extension`` according specification,
        with preserving original sequence order.
        """
        return [
            "id",
            "extension",
            "url",
            "valueBoolean",
        ]


# ------------------- extension types ------------------------------


class dateTimeExtension(_FHIRPrimitiveExtension):
    """
    A G.Health specific extension to the FHIR dateTime type
    Allows dates to be specified as either approximate, and/or number of days relative
    to the current date.
    """

    resource_type: str = Field(default="dateTimeExtension", const=True)

    extension: list[
        Union[et.approximateDateType, et.relativeDayType, fhirtypes.ExtensionType]
    ] = Field(
        None,
        alias="extension",
        title="List of `Extension` items (represented as `dict` in JSON)",
        description="Additional content defined by implementations",
        # if property is element of this resource.
        element_property=True,
        # this trys to match the type of the object to each of the union types
        union_mode="smart",
    )

    @validator("extension")
    def validate_extension_contents(cls, extensions):
        approx_date_count = sum(
            isinstance(item, approximateDate) for item in extensions
        )
        rel_day_count = sum(isinstance(item, relativeDay) for item in extensions)

        if approx_date_count > 1 or rel_day_count > 1:
            raise ValueError("approximateDate and relativeDay can only appear once.")

        return extensions
