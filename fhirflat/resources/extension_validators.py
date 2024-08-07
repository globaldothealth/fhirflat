"""
This file is modified from https://github.com/nazrulworld/fhir.resources
to support custom extension types. Original license below:

BSD License

Copyright (c) 2019, Md Nazrul Islam
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this
  list of conditions and the following disclaimer in the documentation and/or
  other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from this
  software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.

"""

import importlib
import typing
from pathlib import Path
from typing import TYPE_CHECKING, Type, Union

from fhir.resources.core.fhirabstractmodel import FHIRAbstractModel
from pydantic.v1.class_validators import make_generic_validator
from pydantic.v1.error_wrappers import ErrorWrapper, ValidationError
from pydantic.v1.types import StrBytes
from pydantic.v1.utils import ROOT_KEY

if typing.TYPE_CHECKING:
    from pydantic.v1 import BaseModel  # pragma: no cover

# TODO: Make the validation error clearer when the error is that a
# valueCodeableConcept is missing the outer "coding":[] list.


class Validators:
    def __init__(self):
        self.MODEL_CLASSES = {
            "timingPhase": (None, ".extensions"),
            "timingDetail": (None, ".extensions"),
            "timingPhaseDetail": (None, ".extensions"),
            "relativeDay": (None, ".extensions"),
            "relativeStart": (None, ".extensions"),
            "relativeEnd": (None, ".extensions"),
            "relativePeriod": (None, ".extensions"),
            "approximateDate": (None, ".extensions"),
            "Duration": (None, ".extensions"),
            "Age": (None, ".extensions"),
            "birthSex": (None, ".extensions"),
            "Race": (None, ".extensions"),
            "presenceAbsence": (None, ".extensions"),
            "prespecifiedQuery": (None, ".extensions"),
            "dateTimeExtension": (None, ".extensions"),
        }

    def get_fhir_model_class(self, model_name: str) -> Type[FHIRAbstractModel]:
        """
        Returns the extension class by finding the 'datetimeextension' file and
        importing the type class.
        Will probably need changing.
        """
        klass, module_name = self.MODEL_CLASSES[model_name]
        if klass is not None:
            return klass
        module = importlib.import_module(module_name, package=__package__)
        klass = getattr(module, model_name)
        self.MODEL_CLASSES[model_name] = (klass, module_name)
        return klass

    def run_validator_for_fhir_type(self, model_type_cls, v, values, config, field):
        """ """
        cls = self.get_fhir_model_class(model_type_cls.__resource_type__)
        for validator in model_type_cls.__get_validators__():
            func = make_generic_validator(validator)
            v = func(cls, v, values, config, field)
        return v

    def fhir_model_validator(
        self, model_name: str, v: Union[StrBytes, dict, Path, FHIRAbstractModel]
    ):
        """ """
        model_class: Type[BaseModel] | Type[FHIRAbstractModel] = (
            self.get_fhir_model_class(model_name)
        )

        if isinstance(v, (str, bytes)):
            try:
                v = model_class.parse_raw(v)
            except ValidationError as exc:
                if TYPE_CHECKING:
                    model_class = typing.cast(
                        Type[BaseModel], model_class
                    )  # pragma: no cover
                errors = exc.errors()
                if (
                    len(errors) == 1
                    and errors[0]["type"] == "value_error.jsondecode"
                    and errors[0]["loc"][0] == ROOT_KEY
                ):
                    raise ValidationError(
                        [
                            ErrorWrapper(
                                ValueError(
                                    "Invalid json str value has been provided for "
                                    f"class {model_class}"
                                ),
                                loc=ROOT_KEY,
                            )
                        ],
                        model_class,
                    ) from exc

                raise

        elif isinstance(v, Path):
            _p = v
            try:
                v = model_class.parse_file(_p)
            except (ValueError, TypeError) as exc:
                if exc.__class__.__name__ in ("JSONDecodeError", "UnicodeDecodeError"):
                    raise ValidationError(
                        [
                            ErrorWrapper(
                                ValueError(
                                    f"Provided file '{_p}' for class "
                                    "'{model_class.__name__}' "
                                    "as value, contains invalid json data. errors from "
                                    f"decoder-> ''{exc!s}''"
                                ),
                                loc=ROOT_KEY,
                            )
                        ],
                        model_class,
                    ) from exc

                raise

            except FileNotFoundError as fe:
                raise ValidationError(
                    [
                        ErrorWrapper(
                            ValueError(
                                f"Provided file '{_p}' for class {model_class} "
                                "as value, doesn't exists."
                            ),
                            loc=ROOT_KEY,
                        )
                    ],
                    model_class,
                ) from fe

        elif isinstance(v, dict):
            v = model_class.parse_obj(v)

        if not isinstance(v, model_class):
            raise ValidationError(
                [
                    ErrorWrapper(
                        ValueError(
                            "Value is expected from the instance of "
                            f"{model_class}, but got type {type(v)}"
                        ),
                        loc=ROOT_KEY,
                    )
                ],
                model_class,
            )
        if model_name != v.resource_type:
            raise ValidationError(
                [
                    ErrorWrapper(
                        ValueError(
                            f"Expected resource_type is '{model_name}', "
                            f"but value has resource_type '{v.resource_type}'"
                        ),
                        loc=ROOT_KEY,
                    )
                ],
                model_class,
            )
        return v


def timingphase_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("timingPhase", v)


def timingdetail_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("timingDetail", v)


def timingphasedetail_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("timingPhaseDetail", v)


def relativeday_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("relativeDay", v)


def relativestart_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("relativeStart", v)


def relativeend_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("relativeEnd", v)


def relativeperiod_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("relativePeriod", v)


def approximatedate_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("approximateDate", v)


def duration_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("Duration", v)


def age_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("Age", v)


def birthsex_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("birthSex", v)


def race_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("Race", v)


def presenceabsence_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("presenceAbsence", v)


def prespecifiedquery_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("prespecifiedQuery", v)


def datetimeextension_validator(v: Union[StrBytes, dict, Path, FHIRAbstractModel]):
    return Validators().fhir_model_validator("dateTimeExtension", v)
