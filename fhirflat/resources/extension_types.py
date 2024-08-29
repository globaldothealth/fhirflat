from __future__ import annotations

from typing import TYPE_CHECKING

from fhir.resources.fhirtypes import AbstractType as _AbstractType

if TYPE_CHECKING:
    from pydantic.v1.typing import CallableGenerator  # pragma: no cover


class AbstractType(_AbstractType):
    @classmethod
    def __get_validators__(cls) -> "CallableGenerator":
        from . import extension_validators as validators

        yield getattr(validators, cls.__resource_type__.lower() + "_validator")


class timingPhaseType(AbstractType):
    __resource_type__ = "timingPhase"


class timingDetailType(AbstractType):
    __resource_type__ = "timingDetail"


class timingPhaseDetailType(AbstractType):
    __resource_type__ = "timingPhaseDetail"


class relativeDayType(AbstractType):
    __resource_type__ = "relativeDay"


class relativeStartType(AbstractType):
    __resource_type__ = "relativeStart"


class relativeEndType(AbstractType):
    __resource_type__ = "relativeEnd"


class relativePeriodType(AbstractType):
    __resource_type__ = "relativePeriod"


class approximateDateType(AbstractType):
    __resource_type__ = "approximateDate"


class durationType(AbstractType):
    __resource_type__ = "Duration"


class ageType(AbstractType):
    __resource_type__ = "Age"


class birthSexType(AbstractType):
    __resource_type__ = "birthSex"


class raceType(AbstractType):
    __resource_type__ = "Race"


class presenceAbsenceType(AbstractType):
    __resource_type__ = "presenceAbsence"


class prespecifiedQueryType(AbstractType):
    __resource_type__ = "prespecifiedQuery"


class dateTimeExtensionType(AbstractType):
    __resource_type__ = "dateTimeExtension"
