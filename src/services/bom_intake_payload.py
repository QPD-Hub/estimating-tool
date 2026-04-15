from __future__ import annotations

from dataclasses import dataclass


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
            _normalize_required_text(self.customer_name, "CustomerName"),
        )
        object.__setattr__(
            self,
            "source_file_name",
            _normalize_required_text(self.source_file_name, "SourceFileName"),
        )
        object.__setattr__(
            self,
            "uploaded_by",
            _normalize_required_text(self.uploaded_by, "UploadedBy"),
        )
        object.__setattr__(
            self,
            "quote_number",
            _normalize_optional_text(self.quote_number),
        )
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
        object.__setattr__(
            self,
            "intake_notes",
            _normalize_optional_text(self.intake_notes),
        )

    def to_sql_params(self) -> dict[str, object]:
        return {
            "CustomerName": self.customer_name,
            "QuoteNumber": self.quote_number,
            "SourceFileName": self.source_file_name,
            "SourceFilePath": self.source_file_path,
            "SourceSheetName": self.source_sheet_name,
            "SourceType": self.source_type,
            "UploadedBy": self.uploaded_by,
            "ParserVersion": self.parser_version,
            "IntakeNotes": self.intake_notes,
        }


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
    lead_time_days: int | None
    cost: int | float | None
    validation_message: str | None = None
    is_level_0: bool | None = None

    def __post_init__(self) -> None:
        if self.source_row_number < 1:
            raise BomIntakePayloadError("SourceRowNumber must be 1 or greater.")
        if self.bom_level < 0:
            raise BomIntakePayloadError("BomLevel must be 0 or greater.")

        object.__setattr__(
            self,
            "part_number",
            _normalize_required_text(self.part_number, "PartNumber"),
        )
        object.__setattr__(
            self,
            "indented_part_number",
            _normalize_required_text(
                self.indented_part_number,
                "IndentedPartNumber",
            ),
        )
        object.__setattr__(
            self,
            "description",
            _normalize_required_text(self.description, "Description"),
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

        inferred_level_0 = self.bom_level == 0
        if self.is_level_0 is None:
            object.__setattr__(self, "is_level_0", inferred_level_0)
        elif self.is_level_0 != inferred_level_0:
            raise BomIntakePayloadError(
                "IsLevel0 does not match BomLevel for SourceRowNumber "
                f"{self.source_row_number}."
            )


@dataclass(frozen=True)
class BomRootCandidate:
    root_client_id: str
    root_sequence: int
    source_row_number: int
    customer_name: str
    level_0_part_number: str
    revision: str
    root_description: str | None = None
    root_item_number: str | None = None
    root_quantity: int | float | None = None
    root_uom: str | None = None
    root_make_buy: str | None = None
    root_mfr: str | None = None
    root_mfr_number: str | None = None

    def __post_init__(self) -> None:
        if self.root_sequence < 1:
            raise BomIntakePayloadError("RootSequence must be 1 or greater.")
        if self.source_row_number < 1:
            raise BomIntakePayloadError("SourceRowNumber must be 1 or greater.")

        object.__setattr__(
            self,
            "root_client_id",
            _normalize_required_text(self.root_client_id, "RootClientId"),
        )
        object.__setattr__(
            self,
            "customer_name",
            _normalize_required_text(self.customer_name, "CustomerName"),
        )
        object.__setattr__(
            self,
            "level_0_part_number",
            _normalize_required_text(self.level_0_part_number, "Level0PartNumber"),
        )
        object.__setattr__(self, "revision", _normalize_required_text(self.revision, "Revision"))
        object.__setattr__(
            self,
            "root_description",
            _normalize_optional_text(self.root_description),
        )
        object.__setattr__(
            self,
            "root_item_number",
            _normalize_optional_text(self.root_item_number),
        )
        object.__setattr__(self, "root_uom", _normalize_optional_text(self.root_uom))
        object.__setattr__(
            self,
            "root_make_buy",
            _normalize_optional_text(self.root_make_buy),
        )
        object.__setattr__(self, "root_mfr", _normalize_optional_text(self.root_mfr))
        object.__setattr__(
            self,
            "root_mfr_number",
            _normalize_optional_text(self.root_mfr_number),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "RootClientId": self.root_client_id,
            "RootSequence": self.root_sequence,
            "SourceRowNumber": self.source_row_number,
            "CustomerName": self.customer_name,
            "Level0PartNumber": self.level_0_part_number,
            "Revision": self.revision,
            "RootDescription": self.root_description,
            "RootItemNumber": self.root_item_number,
            "RootQuantity": self.root_quantity,
            "RootUOM": self.root_uom,
            "RootMakeBuy": self.root_make_buy,
            "RootMFR": self.root_mfr,
            "RootMFRNumber": self.root_mfr_number,
        }


@dataclass(frozen=True)
class BomUploadRow:
    root_client_id: str
    row_sequence: int
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
    lead_time_days: int | None
    cost: int | float | None
    is_level_0: bool | None = None
    validation_message: str | None = None

    def __post_init__(self) -> None:
        if self.row_sequence < 1:
            raise BomIntakePayloadError("RowSequence must be 1 or greater.")
        if self.source_row_number < 1:
            raise BomIntakePayloadError("SourceRowNumber must be 1 or greater.")
        if self.bom_level < 0:
            raise BomIntakePayloadError("BomLevel must be 0 or greater.")

        object.__setattr__(
            self,
            "root_client_id",
            _normalize_required_text(self.root_client_id, "RootClientId"),
        )
        object.__setattr__(
            self,
            "part_number",
            _normalize_required_text(self.part_number, "PartNumber"),
        )
        object.__setattr__(
            self,
            "indented_part_number",
            _normalize_required_text(
                self.indented_part_number,
                "IndentedPartNumber",
            ),
        )
        object.__setattr__(
            self,
            "description",
            _normalize_required_text(self.description, "Description"),
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

        inferred_level_0 = self.bom_level == 0
        if self.is_level_0 is None:
            object.__setattr__(self, "is_level_0", inferred_level_0)
        elif self.is_level_0 != inferred_level_0:
            raise BomIntakePayloadError(
                "IsLevel0 does not match BomLevel for RowSequence "
                f"{self.row_sequence}."
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "RootClientId": self.root_client_id,
            "RowSequence": self.row_sequence,
            "SourceRowNumber": self.source_row_number,
            "OriginalValue": self.original_value,
            "ParentPart": self.parent_part,
            "PartNumber": self.part_number,
            "IndentedPartNumber": self.indented_part_number,
            "BomLevel": self.bom_level,
            "Description": self.description,
            "Revision": self.revision,
            "Quantity": self.quantity,
            "UOM": self.uom,
            "ItemNumber": self.item_number,
            "MakeBuy": self.make_buy,
            "MFR": self.mfr,
            "MFRNumber": self.mfr_number,
            "LeadTimeDays": self.lead_time_days,
            "Cost": self.cost,
            "IsLevel0": self.is_level_0,
            "ValidationMessage": self.validation_message,
        }


@dataclass(frozen=True)
class BomIntakePayload:
    metadata: BomIntakeMetadata
    root_candidates: list[BomRootCandidate]
    bom_rows: list[BomUploadRow]

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not self.root_candidates:
            raise BomIntakePayloadError("At least one root candidate is required.")
        if not self.bom_rows:
            raise BomIntakePayloadError("At least one BOM row is required.")

        roots_by_id: dict[str, BomRootCandidate] = {}
        roots_by_identity: set[tuple[str, str, str]] = set()
        root_sequences: set[int] = set()

        for root in self.root_candidates:
            if root.root_client_id in roots_by_id:
                raise BomIntakePayloadError(
                    f"Duplicate RootClientId detected: {root.root_client_id}"
                )
            if root.root_sequence in root_sequences:
                raise BomIntakePayloadError(
                    f"Duplicate RootSequence detected: {root.root_sequence}"
                )

            root_identity = (
                root.customer_name,
                root.level_0_part_number,
                root.revision,
            )
            if root_identity in roots_by_identity:
                raise BomIntakePayloadError(
                    "Duplicate root identity detected in upload: "
                    f"{root.customer_name} / {root.level_0_part_number} / {root.revision}"
                )

            roots_by_id[root.root_client_id] = root
            roots_by_identity.add(root_identity)
            root_sequences.add(root.root_sequence)

        row_sequences_by_root: dict[str, set[int]] = {
            root_id: set() for root_id in roots_by_id
        }
        has_level_0_row_by_root: dict[str, bool] = {
            root_id: False for root_id in roots_by_id
        }

        for row in self.bom_rows:
            root = roots_by_id.get(row.root_client_id)
            if root is None:
                raise BomIntakePayloadError(
                    f"BOM row references unknown RootClientId: {row.root_client_id}"
                )

            existing_sequences = row_sequences_by_root[row.root_client_id]
            if row.row_sequence in existing_sequences:
                raise BomIntakePayloadError(
                    "Duplicate RowSequence detected within RootClientId "
                    f"{row.root_client_id}: {row.row_sequence}"
                )
            existing_sequences.add(row.row_sequence)

            if row.is_level_0:
                has_level_0_row_by_root[row.root_client_id] = True
                if row.part_number != root.level_0_part_number:
                    raise BomIntakePayloadError(
                        "Level 0 row PartNumber does not match root candidate "
                        f"for RootClientId {row.root_client_id}."
                    )
                if row.revision != root.revision:
                    raise BomIntakePayloadError(
                        "Level 0 row Revision does not match root candidate "
                        f"for RootClientId {row.root_client_id}."
                    )

        for root_client_id, sequences in row_sequences_by_root.items():
            if not sequences:
                raise BomIntakePayloadError(
                    f"RootClientId {root_client_id} has no BOM rows."
                )
            if 1 not in sequences:
                raise BomIntakePayloadError(
                    f"RootClientId {root_client_id} is missing RowSequence 1."
                )
            expected_sequences = set(range(1, len(sequences) + 1))
            if sequences != expected_sequences:
                raise BomIntakePayloadError(
                    "RowSequence values must be contiguous within RootClientId "
                    f"{root_client_id}."
                )
            if not has_level_0_row_by_root[root_client_id]:
                raise BomIntakePayloadError(
                    f"RootClientId {root_client_id} is missing a level 0 BOM row."
                )

    def to_sql_payload(self) -> dict[str, object]:
        return {
            "Header": self.metadata.to_sql_params(),
            "RootCandidates": [root.to_dict() for root in self.root_candidates],
            "BomRows": [row.to_dict() for row in self.bom_rows],
        }


def build_bom_intake_payload(
    metadata: BomIntakeMetadata,
    standardized_rows: list[StandardizedBomRow],
) -> BomIntakePayload:
    if not standardized_rows:
        raise BomIntakePayloadError("At least one standardized BOM row is required.")

    root_candidates: list[BomRootCandidate] = []
    bom_rows: list[BomUploadRow] = []
    active_root_client_id: str | None = None
    active_row_sequence = 0
    root_sequence = 0

    for row in standardized_rows:
        if row.is_level_0:
            revision = _normalize_required_text(row.revision, "Revision")
            root_sequence += 1
            active_row_sequence = 0
            active_root_client_id = f"R{root_sequence}"
            root_candidates.append(
                BomRootCandidate(
                    root_client_id=active_root_client_id,
                    root_sequence=root_sequence,
                    source_row_number=row.source_row_number,
                    customer_name=metadata.customer_name,
                    level_0_part_number=row.part_number,
                    revision=revision,
                    root_description=row.description,
                    root_item_number=row.item_number,
                    root_quantity=row.quantity,
                    root_uom=row.uom,
                    root_make_buy=row.make_buy,
                    root_mfr=row.mfr,
                    root_mfr_number=row.mfr_number,
                )
            )

        if active_root_client_id is None:
            raise BomIntakePayloadError(
                "Every BOM row must belong to a detected root. "
                f"SourceRowNumber {row.source_row_number} appears before the first level 0 row."
            )

        active_row_sequence += 1
        bom_rows.append(
            BomUploadRow(
                root_client_id=active_root_client_id,
                row_sequence=active_row_sequence,
                source_row_number=row.source_row_number,
                original_value=row.original_value,
                parent_part=row.parent_part,
                part_number=row.part_number,
                indented_part_number=row.indented_part_number,
                bom_level=row.bom_level,
                description=row.description,
                revision=row.revision,
                quantity=row.quantity,
                uom=row.uom,
                item_number=row.item_number,
                make_buy=row.make_buy,
                mfr=row.mfr,
                mfr_number=row.mfr_number,
                lead_time_days=row.lead_time_days,
                cost=row.cost,
                is_level_0=row.is_level_0,
                validation_message=row.validation_message,
            )
        )

    return BomIntakePayload(
        metadata=metadata,
        root_candidates=root_candidates,
        bom_rows=bom_rows,
    )


def _normalize_required_text(value: str | None, field_name: str) -> str:
    normalized_value = _normalize_optional_text(value)
    if not normalized_value:
        raise BomIntakePayloadError(f"{field_name} is required.")
    return normalized_value


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None

    normalized_value = str(value).strip()
    if not normalized_value:
        return None
    return normalized_value


def _normalize_revision(value: str | None) -> str:
    normalized_value = _normalize_optional_text(value)
    if normalized_value is None:
        return ""
    return normalized_value
