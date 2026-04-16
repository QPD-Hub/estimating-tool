from __future__ import annotations

from dataclasses import dataclass

from src.contracts.bom_intake import (
    BomIntakeRootRow,
    BomIntakeRow,
    CreateBomIntakeInput,
    ProcessStandardizedBomIntakePayload,
    ProcessStandardizedBomIntakeInput,
)


class BomIntakePayloadError(ValueError):
    pass


@dataclass(frozen=True)
class BomIntakeMetadata:
    customer_name: str
    source_file_name: str
    uploaded_by: str
    quote_number: str | None = None
    source_file_path: str | None = None
    source_sheet_name: str | None = None
    source_type: str | None = None
    parser_version: str | None = None
    intake_notes: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "customer_name",
            _normalize_required_text(self.customer_name, "customer_name"),
        )
        object.__setattr__(
            self,
            "source_file_name",
            _normalize_required_text(self.source_file_name, "source_file_name"),
        )
        object.__setattr__(
            self,
            "uploaded_by",
            _normalize_required_text(self.uploaded_by, "uploaded_by"),
        )
        object.__setattr__(self, "quote_number", _normalize_optional_text(self.quote_number))
        object.__setattr__(
            self,
            "source_file_path",
            _normalize_optional_text(self.source_file_path),
        )
        object.__setattr__(
            self,
            "source_sheet_name",
            _normalize_optional_text(self.source_sheet_name),
        )
        object.__setattr__(self, "source_type", _normalize_optional_text(self.source_type))
        object.__setattr__(
            self,
            "parser_version",
            _normalize_optional_text(self.parser_version),
        )
        object.__setattr__(self, "intake_notes", _normalize_optional_text(self.intake_notes))

    def to_create_input(self) -> CreateBomIntakeInput:
        return CreateBomIntakeInput(
            CustomerName=self.customer_name,
            QuoteNumber=self.quote_number,
            SourceFileName=self.source_file_name,
            SourceFilePath=self.source_file_path,
            SourceSheetName=self.source_sheet_name,
            SourceType=self.source_type,
            UploadedBy=self.uploaded_by,
            ParserVersion=self.parser_version,
            IntakeNotes=self.intake_notes,
        )


@dataclass(frozen=True)
class StandardizedBomRow:
    source_row_number: int
    original_value: str | None
    parent_part: str | None
    part_number: str
    indented_part_number: str
    bom_level: int
    description: str
    revision: str | None
    quantity: int | float | None
    uom: str | None
    item_number: str | None
    make_buy: str | None
    mfr: str | None
    mfr_number: str | None
    lead_time_days: int | float | None
    cost: int | float | None
    validation_message: str | None = None

    def __post_init__(self) -> None:
        if self.source_row_number < 1:
            raise BomIntakePayloadError("source_row_number must be 1 or greater.")
        if self.bom_level < 0:
            raise BomIntakePayloadError("bom_level must be 0 or greater.")

        object.__setattr__(
            self,
            "part_number",
            _normalize_required_text(self.part_number, "part_number"),
        )
        object.__setattr__(
            self,
            "indented_part_number",
            _normalize_required_text(
                self.indented_part_number,
                "indented_part_number",
            ),
        )
        object.__setattr__(
            self,
            "description",
            _normalize_required_text(self.description, "description"),
        )
        object.__setattr__(self, "original_value", _normalize_optional_text(self.original_value))
        object.__setattr__(self, "parent_part", _normalize_optional_text(self.parent_part))
        object.__setattr__(self, "revision", _normalize_revision(self.revision))
        object.__setattr__(self, "uom", _normalize_optional_text(self.uom))
        object.__setattr__(self, "item_number", _normalize_optional_text(self.item_number))
        object.__setattr__(self, "make_buy", _normalize_optional_text(self.make_buy))
        object.__setattr__(self, "mfr", _normalize_optional_text(self.mfr))
        object.__setattr__(self, "mfr_number", _normalize_optional_text(self.mfr_number))
        object.__setattr__(
            self,
            "validation_message",
            _normalize_optional_text(self.validation_message),
        )

    @property
    def is_root_row(self) -> bool:
        return self.bom_level == 0


@dataclass(frozen=True)
class BomIntakePayload:
    create_input: CreateBomIntakeInput
    detected_by: str
    roots: list[BomIntakeRootRow]
    rows: list[BomIntakeRow]

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not self.roots:
            raise BomIntakePayloadError("At least one root candidate is required.")
        if not self.rows:
            raise BomIntakePayloadError("At least one BOM row is required.")

        roots_by_id: dict[str, BomIntakeRootRow] = {}
        row_sequences_by_root: dict[str, set[int]] = {}

        for root in self.roots:
            if root.RootClientId in roots_by_id:
                raise BomIntakePayloadError(
                    f"Duplicate RootClientId detected: {root.RootClientId}"
                )
            roots_by_id[root.RootClientId] = root
            row_sequences_by_root[root.RootClientId] = set()

        for row in self.rows:
            if row.RootClientId not in roots_by_id:
                raise BomIntakePayloadError(
                    f"BOM row references unknown RootClientId: {row.RootClientId}"
                )
            if row.RowSequence in row_sequences_by_root[row.RootClientId]:
                raise BomIntakePayloadError(
                    "Duplicate RowSequence detected within RootClientId "
                    f"{row.RootClientId}: {row.RowSequence}"
                )
            row_sequences_by_root[row.RootClientId].add(row.RowSequence)

        for root_client_id, sequences in row_sequences_by_root.items():
            if not sequences:
                raise BomIntakePayloadError(
                    f"RootClientId {root_client_id} has no BOM rows."
                )

    def process_input(self, bom_intake_id: int) -> ProcessStandardizedBomIntakeInput:
        return ProcessStandardizedBomIntakeInput(
            BomIntakeId=bom_intake_id,
            DetectedBy=self.detected_by,
        )

    def process_payload(
        self,
        bom_intake_id: int | None,
    ) -> ProcessStandardizedBomIntakePayload:
        if bom_intake_id is None:
            params = ProcessStandardizedBomIntakeInput(
                BomIntakeId=0,
                DetectedBy=self.detected_by,
            ).to_dict()
            params["BomIntakeId"] = None
        else:
            params = self.process_input(bom_intake_id).to_dict()

        return ProcessStandardizedBomIntakePayload(
            params=ProcessStandardizedBomIntakeInput.from_dict(
                params,
                context="Process standardized procedure params",
            ),
            roots=[
                BomIntakeRootRow.from_dict(
                    root.to_dict(),
                    context=f"Process standardized roots[{index}]",
                )
                for index, root in enumerate(self.roots)
            ],
            rows=[
                BomIntakeRow.from_dict(
                    row.to_dict(),
                    context=f"Process standardized rows[{index}]",
                )
                for index, row in enumerate(self.rows)
            ],
        )

    def to_preview_dict(self) -> dict[str, object]:
        process_payload = self.process_payload(None).to_dict()
        return {
            "createProc": {
                "procedure": "dbo.usp_BOM_Intake_Create",
                "params": self.create_input.to_dict(),
            },
            "processStandardizedProc": {
                "procedure": "dbo.usp_BOM_Intake_ProcessStandardized",
                **process_payload,
            },
        }


def build_bom_intake_payload(
    metadata: BomIntakeMetadata,
    standardized_rows: list[StandardizedBomRow],
) -> BomIntakePayload:
    if not standardized_rows:
        raise BomIntakePayloadError("At least one standardized BOM row is required.")

    roots: list[BomIntakeRootRow] = []
    bom_rows: list[BomIntakeRow] = []
    active_root_client_id: str | None = None
    active_row_sequence = 0
    root_sequence = 0

    for row in standardized_rows:
        if row.is_root_row:
            revision = _normalize_required_text(row.revision, "revision")
            root_sequence += 1
            active_row_sequence = 0
            active_root_client_id = f"R{root_sequence}"
            roots.append(
                BomIntakeRootRow(
                    RootClientId=active_root_client_id,
                    RootSequence=root_sequence,
                    SourceRowNumber=row.source_row_number,
                    CustomerName=metadata.customer_name,
                    Level0PartNumber=row.part_number,
                    Revision=revision,
                    RootDescription=row.description,
                    RootItemNumber=row.item_number,
                    RootQuantity=row.quantity,
                    RootUOM=row.uom,
                    RootMakeBuy=row.make_buy,
                    RootMFR=row.mfr,
                    RootMFRNumber=row.mfr_number,
                )
            )

        if active_root_client_id is None:
            raise BomIntakePayloadError(
                "Every BOM row must belong to a detected root. "
                f"source_row_number {row.source_row_number} appears before the first root row."
            )

        active_row_sequence += 1
        bom_rows.append(
            BomIntakeRow(
                RootClientId=active_root_client_id,
                RowSequence=active_row_sequence,
                SourceRowNumber=row.source_row_number,
                OriginalValue=row.original_value,
                ParentPart=row.parent_part,
                PartNumber=row.part_number,
                IndentedPartNumber=row.indented_part_number,
                BomLevel=row.bom_level,
                Description=row.description,
                Revision=row.revision,
                Quantity=row.quantity,
                UOM=row.uom,
                ItemNumber=row.item_number,
                MakeBuy=row.make_buy,
                MFR=row.mfr,
                MFRNumber=row.mfr_number,
                LeadTimeDays=row.lead_time_days,
                Cost=row.cost,
                ValidationMessage=row.validation_message,
            )
        )

    return BomIntakePayload(
        create_input=metadata.to_create_input(),
        detected_by=metadata.uploaded_by,
        roots=roots,
        rows=bom_rows,
    )


def _normalize_required_text(value: str | None, field_name: str) -> str:
    normalized_value = _normalize_optional_text(value)
    if not normalized_value:
        raise BomIntakePayloadError(f"{field_name} is required.")
    return normalized_value


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def _normalize_revision(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip()
