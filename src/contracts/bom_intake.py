from __future__ import annotations

from dataclasses import MISSING, asdict, dataclass, fields
from typing import ClassVar


CREATE_PROC_SCALAR_FIELDS = (
    "CustomerName",
    "QuoteNumber",
    "SourceFileName",
    "SourceFilePath",
    "SourceSheetName",
    "SourceType",
    "UploadedBy",
    "ParserVersion",
    "IntakeNotes",
)

PROCESS_PROC_SCALAR_FIELDS = (
    "BomIntakeId",
    "DetectedBy",
)

ROOT_TVP_FIELDS = (
    "RootClientId",
    "RootSequence",
    "SourceRowNumber",
    "CustomerName",
    "Level0PartNumber",
    "Revision",
    "RootDescription",
    "RootItemNumber",
    "RootQuantity",
    "RootUOM",
    "RootMakeBuy",
    "RootMFR",
    "RootMFRNumber",
)

ROW_TVP_FIELDS = (
    "RootClientId",
    "RowSequence",
    "SourceRowNumber",
    "OriginalValue",
    "ParentPart",
    "PartNumber",
    "IndentedPartNumber",
    "BomLevel",
    "Description",
    "Revision",
    "Quantity",
    "UOM",
    "ItemNumber",
    "MakeBuy",
    "MFR",
    "MFRNumber",
    "LeadTimeDays",
    "Cost",
    "ValidationMessage",
)


class BomIntakeContractError(ValueError):
    pass


@dataclass(frozen=True)
class _StrictContractModel:
    _FIELD_NAMES: ClassVar[tuple[str, ...]] = ()

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def field_names(cls) -> tuple[str, ...]:
        return tuple(field.name for field in fields(cls))

    @classmethod
    def from_dict(cls, payload: dict[str, object], *, context: str) -> "_StrictContractModel":
        if not isinstance(payload, dict):
            raise BomIntakeContractError(f"{context} must be an object.")

        field_names = cls.field_names()
        unknown_fields = sorted(set(payload) - set(field_names))
        if unknown_fields:
            raise BomIntakeContractError(
                f"{context} contains unknown fields: {', '.join(unknown_fields)}."
            )

        missing_fields = [
            field.name
            for field in fields(cls)
            if field.name not in payload
            and field.default is MISSING
            and field.default_factory is MISSING
        ]
        if missing_fields:
            raise BomIntakeContractError(
                f"{context} is missing required fields: {', '.join(missing_fields)}."
            )

        return cls(**payload)


@dataclass(frozen=True)
class CreateBomIntakeInput(_StrictContractModel):
    CustomerName: str
    QuoteNumber: str | None = None
    SourceFileName: str | None = None
    SourceFilePath: str | None = None
    SourceSheetName: str | None = None
    SourceType: str | None = None
    UploadedBy: str | None = None
    ParserVersion: str | None = None
    IntakeNotes: str | None = None


@dataclass(frozen=True)
class ProcessStandardizedBomIntakeInput(_StrictContractModel):
    BomIntakeId: int
    DetectedBy: str | None = None


@dataclass(frozen=True)
class BomIntakeRootRow(_StrictContractModel):
    RootClientId: str
    RootSequence: int
    SourceRowNumber: int
    CustomerName: str
    Level0PartNumber: str
    Revision: str
    RootDescription: str | None = None
    RootItemNumber: str | None = None
    RootQuantity: int | float | None = None
    RootUOM: str | None = None
    RootMakeBuy: str | None = None
    RootMFR: str | None = None
    RootMFRNumber: str | None = None


@dataclass(frozen=True)
class BomIntakeRow(_StrictContractModel):
    RootClientId: str
    RowSequence: int
    SourceRowNumber: int
    OriginalValue: str | None = None
    ParentPart: str | None = None
    PartNumber: str | None = None
    IndentedPartNumber: str | None = None
    BomLevel: int | None = None
    Description: str | None = None
    Revision: str | None = None
    Quantity: int | float | None = None
    UOM: str | None = None
    ItemNumber: str | None = None
    MakeBuy: str | None = None
    MFR: str | None = None
    MFRNumber: str | None = None
    LeadTimeDays: int | float | None = None
    Cost: int | float | None = None
    ValidationMessage: str | None = None
