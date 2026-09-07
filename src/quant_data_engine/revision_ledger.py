"""Batch-level source revision ledger hooks for portfolio research cache v2.

This module intentionally duplicates the small Mongo ledger contract consumed by
``stock-scoring-system``.  Keeping the writer contract local avoids a runtime
dependency between the two repositories.
"""

from __future__ import annotations

import os
import socket
import threading
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator, Mapping, Optional
import logging

from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

SOURCE_LEDGER_MODE_ENV = "PORTFOLIO_RESEARCH_SOURCE_LEDGER_MODE"
LEDGER_DB_ENV = "PORTFOLIO_RESEARCH_CACHE_V2_LEDGER_DB"
LEDGER_COLLECTION_ENV = "PORTFOLIO_RESEARCH_CACHE_V2_LEDGER_COLLECTION"
LEDGER_TTL_SECONDS_ENV = "PORTFOLIO_RESEARCH_SOURCE_LEDGER_TTL_SECONDS"
LEDGER_HEARTBEAT_SECONDS_ENV = "PORTFOLIO_RESEARCH_SOURCE_LEDGER_HEARTBEAT_SECONDS"
DEFAULT_LEDGER_DB = "finance"
DEFAULT_LEDGER_COLLECTION = "portfolio_cache_source_revisions_v2"
DEFAULT_LEDGER_TTL_SECONDS = 900.0
DEFAULT_LEDGER_HEARTBEAT_SECONDS = 60.0

log = logging.getLogger(__name__)

VOLUME_PRICE_SOURCE = "quant_data.volume_price"
STOCK_ADJ_FACTOR_SOURCE = "quant_data.stock_adj_factor"
INDEX_CONSTITUENTS_SOURCE = "quant_data.index_constituents"
SW_INDEX_MEMBER_SOURCE = "quant_data.sw_index_member"
LEDGER_SOURCES = frozenset(
    {
        VOLUME_PRICE_SOURCE,
        STOCK_ADJ_FACTOR_SOURCE,
        INDEX_CONSTITUENTS_SOURCE,
        SW_INDEX_MEMBER_SOURCE,
    }
)

# Version-controlled inventory of every known writer for the four tracked
# collections.  Every production writer must be ledgered before its source can
# be audited coverage-complete.
WRITER_MANIFEST: tuple[Mapping[str, Any], ...] = (
    {
        "source": VOLUME_PRICE_SOURCE,
        "writer_id": "stock_daily_eod",
        "entrypoint": "quant_data_engine.tasks.run_stock_daily",
        "coverage_included": True,
    },
    {
        "source": VOLUME_PRICE_SOURCE,
        "writer_id": "volume_price_raw_backfill",
        "entrypoint": "quant_data_engine.tasks.run_volume_price_backfill",
        "coverage_included": True,
    },
    {
        "source": VOLUME_PRICE_SOURCE,
        "writer_id": "realtime_daily_k",
        "entrypoint": "quant_data_engine.tasks.run_realtime_daily_k_sync",
        "coverage_included": True,
    },
    {
        "source": VOLUME_PRICE_SOURCE,
        "writer_id": "watchlist_intraday_eastmoney",
        "entrypoint": "quant_data_engine.tasks.run_watchlist_intraday_eastmoney",
        "coverage_included": True,
    },
    {
        "source": VOLUME_PRICE_SOURCE,
        "writer_id": "miniqmt_realtime_daily_k",
        "entrypoint": "quant_data_engine.bridges.miniqmt.sinks",
        "coverage_included": True,
    },
    {
        "source": STOCK_ADJ_FACTOR_SOURCE,
        "writer_id": "stock_adj_factor_sync",
        "entrypoint": "quant_data_engine.tasks.run_stock_adj_factor_sync",
        "coverage_included": True,
    },
    {
        "source": INDEX_CONSTITUENTS_SOURCE,
        "writer_id": "index_constituents_sync",
        "entrypoint": "quant_data_engine.tasks.run_index_constituents_sync",
        "coverage_included": True,
    },
    {
        "source": INDEX_CONSTITUENTS_SOURCE,
        "writer_id": "index_constituents_backfill",
        "entrypoint": "quant_data_engine.tasks.run_index_constituents_backfill",
        "coverage_included": True,
    },
    {
        "source": SW_INDEX_MEMBER_SOURCE,
        "writer_id": "sw_industry_members_backfill",
        "entrypoint": "quant_data_engine.tasks.run_sw_industry_backfill",
        "coverage_included": True,
        "condition": "--sync members|all",
    },
    {
        "source": VOLUME_PRICE_SOURCE,
        "writer_id": "migrate_volume_price",
        "entrypoint": "tools/migrate_to_quant_data.py",
        "coverage_included": True,
    },
    {
        "source": INDEX_CONSTITUENTS_SOURCE,
        "writer_id": "migrate_index_constituents",
        "entrypoint": "tools/migrate_to_quant_data.py",
        "coverage_included": True,
    },
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def source_ledger_mode() -> str:
    """Return the validated writer mode (``off`` by default)."""

    mode = os.getenv(SOURCE_LEDGER_MODE_ENV, "off").strip().lower()
    if mode not in {"off", "on"}:
        raise RuntimeError(f"{SOURCE_LEDGER_MODE_ENV} must be off or on, got {mode!r}")
    return mode


def ledger_timing() -> tuple[float, float]:
    try:
        ttl = float(
            os.getenv(LEDGER_TTL_SECONDS_ENV, str(DEFAULT_LEDGER_TTL_SECONDS))
        )
        heartbeat = float(
            os.getenv(
                LEDGER_HEARTBEAT_SECONDS_ENV,
                str(DEFAULT_LEDGER_HEARTBEAT_SECONDS),
            )
        )
    except ValueError as exc:
        raise LedgerFatalError("ledger TTL and heartbeat must be numbers") from exc
    if ttl <= 0:
        raise LedgerFatalError(f"{LEDGER_TTL_SECONDS_ENV} must be positive")
    if heartbeat <= 0 or heartbeat >= ttl:
        raise LedgerFatalError(
            f"{LEDGER_HEARTBEAT_SECONDS_ENV} must be positive and less than TTL"
        )
    return ttl, heartbeat


class LedgerFatalError(RuntimeError):
    """A ledger failure that must terminate a writer instead of being retried."""


class LedgerSyncFatalError(LedgerFatalError):
    """A configured secondary mirror failed and must stop the writer."""


def is_ledger_fatal(exc: BaseException) -> bool:
    return isinstance(exc, LedgerFatalError) or bool(
        getattr(exc, "_revision_ledger_secondary_failure", False)
    )


@dataclass
class LedgerMutationCapability:
    source: str
    mutation_id: str
    fatal_error: Optional[LedgerFatalError] = None
    active: bool = True
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def fail(self, error: LedgerFatalError) -> LedgerFatalError:
        with self._lock:
            if self.fatal_error is None:
                self.fatal_error = error
            return self.fatal_error

    def deactivate(self) -> None:
        with self._lock:
            self.active = False

    def failure(self) -> Optional[LedgerFatalError]:
        with self._lock:
            return self.fatal_error

    def require_usable(self, source: str) -> None:
        with self._lock:
            if source != self.source:
                raise LedgerFatalError(
                    f"active mutation token is for {self.source}, not {source}"
                )
            if not self.active:
                raise LedgerFatalError(
                    f"mutation token is no longer active: {source}/{self.mutation_id}"
                )
            if self.fatal_error is not None:
                raise self.fatal_error


_CURRENT_CAPABILITY: ContextVar[Optional[LedgerMutationCapability]] = ContextVar(
    "revision_ledger_current_capability",
    default=None,
)


def current_ledger_capability() -> Optional[LedgerMutationCapability]:
    return _CURRENT_CAPABILITY.get()


def require_active_ledger_token(source: str) -> None:
    """Require the exact mutation capability in the current execution context."""

    try:
        mode = source_ledger_mode()
    except Exception as exc:
        raise LedgerFatalError(f"invalid source ledger mode: {exc}") from exc
    if mode == "off":
        return
    capability = _CURRENT_CAPABILITY.get()
    if capability is None:
        raise LedgerFatalError(
            f"ledger mode is on but no active mutation token exists for {source}"
        )
    capability.require_usable(source)


class SourceRevisionLedger:
    """Mongo implementation compatible with portfolio research cache v2."""

    def __init__(
        self,
        collection: Any,
        *,
        ttl_seconds: float = DEFAULT_LEDGER_TTL_SECONDS,
        now: Callable[[], datetime] = _utcnow,
    ) -> None:
        if float(ttl_seconds) <= 0:
            raise ValueError("ttl_seconds must be positive")
        self.collection = collection
        self.ttl_seconds = float(ttl_seconds)
        self.now = now
        try:
            self.collection.create_index("source", unique=True, sparse=True)
        except Exception as exc:
            raise LedgerFatalError("failed to initialize source revision ledger") from exc

    @staticmethod
    def _require_source(source: str) -> None:
        if source not in LEDGER_SOURCES:
            raise LedgerFatalError(f"unknown portfolio research source: {source}")

    def _ensure_source(self, source: str) -> None:
        try:
            self.collection.update_one(
                {"source": source},
                {
                    "$setOnInsert": {
                        "source": source,
                        "revision": 0,
                        "state": "committed",
                        "active_mutations": [],
                    }
                },
                upsert=True,
            )
        except DuplicateKeyError:
            # A concurrent writer inserted the source document first.
            return
        except Exception as exc:
            raise LedgerFatalError(f"failed to initialize ledger source {source}") from exc

    def _sync_state(self, source: str, document: Mapping[str, Any]) -> None:
        if document.get("active_mutations"):
            return
        try:
            self.collection.update_one(
                {"source": source, "active_mutations": []},
                {"$set": {"state": "committed", "updated_at": self.now()}},
            )
        except Exception as exc:
            raise LedgerFatalError(f"failed to finalize ledger state for {source}") from exc

    def recover_expired(self, source: str) -> Optional[int]:
        """Remove all expired mutations atomically and invalidate once."""

        self._require_source(source)
        now = self.now()
        try:
            document = self.collection.find_one_and_update(
                {
                    "source": source,
                    "active_mutations": {"$elemMatch": {"expires_at": {"$lte": now}}},
                },
                {
                    "$pull": {"active_mutations": {"expires_at": {"$lte": now}}},
                    "$inc": {"revision": 1},
                    "$set": {
                        "coverage_complete": False,
                        "coverage_valid_until": None,
                        "updated_at": now,
                    },
                },
                return_document=ReturnDocument.AFTER,
            )
        except Exception as exc:
            raise LedgerFatalError(
                f"failed to recover expired mutations for {source}"
            ) from exc
        if document is None:
            return None
        self._sync_state(source, document)
        return int(document["revision"])

    def begin(
        self,
        source: str,
        mutation_id: str,
        *,
        owner_id: str,
    ) -> int:
        self._require_source(source)
        self._ensure_source(source)
        self.recover_expired(source)
        now = self.now()
        mutation = {
            "mutation_id": str(mutation_id),
            "owner_id": str(owner_id),
            "started_at": now,
            "expires_at": now + timedelta(seconds=self.ttl_seconds),
        }
        try:
            document = self.collection.find_one_and_update(
                {
                    "source": source,
                    "active_mutations.mutation_id": {"$ne": str(mutation_id)},
                },
                {
                    "$inc": {"revision": 1},
                    "$push": {"active_mutations": mutation},
                    "$set": {
                        "state": "mutating",
                        "coverage_complete": False,
                        "coverage_valid_until": None,
                        "updated_at": now,
                    },
                    "$unset": {
                        "valid_until": "",
                        "mutation_id": "",
                        "writer_id": "",
                    },
                },
                return_document=ReturnDocument.AFTER,
            )
        except Exception as exc:
            raise LedgerFatalError(f"failed to begin source mutation: {source}") from exc
        if document is None:
            raise LedgerFatalError(f"duplicate source mutation id: {source}/{mutation_id}")
        return int(document["revision"])

    def commit(self, source: str, mutation_id: str) -> int:
        self._require_source(source)
        try:
            document = self.collection.find_one_and_update(
                {
                    "source": source,
                    "active_mutations.mutation_id": str(mutation_id),
                },
                {
                    "$pull": {
                        "active_mutations": {"mutation_id": str(mutation_id)}
                    },
                    "$set": {"updated_at": self.now()},
                },
                return_document=ReturnDocument.AFTER,
            )
        except Exception as exc:
            raise LedgerFatalError(f"failed to commit source mutation: {source}") from exc
        if document is None:
            raise LedgerFatalError(f"mutation is not active: {source}/{mutation_id}")
        self._sync_state(source, document)
        return int(document["revision"])

    def abort(self, source: str, mutation_id: str) -> int:
        self._require_source(source)
        try:
            document = self.collection.find_one_and_update(
                {
                    "source": source,
                    "active_mutations.mutation_id": str(mutation_id),
                },
                {
                    "$pull": {
                        "active_mutations": {"mutation_id": str(mutation_id)}
                    },
                    "$inc": {"revision": 1},
                    "$set": {
                        "coverage_complete": False,
                        "coverage_valid_until": None,
                        "updated_at": self.now(),
                        "last_aborted_mutation_id": str(mutation_id),
                    },
                },
                return_document=ReturnDocument.AFTER,
            )
        except Exception as exc:
            raise LedgerFatalError(f"failed to abort source mutation: {source}") from exc
        if document is None:
            raise LedgerFatalError(f"mutation is not active: {source}/{mutation_id}")
        self._sync_state(source, document)
        return int(document["revision"])

    def heartbeat(self, source: str, mutation_id: str) -> datetime:
        self._require_source(source)
        now = self.now()
        expires_at = now + timedelta(seconds=self.ttl_seconds)
        try:
            document = self.collection.find_one_and_update(
                {
                    "source": source,
                    "active_mutations.mutation_id": str(mutation_id),
                },
                {
                    "$set": {
                        "active_mutations.$.expires_at": expires_at,
                        "updated_at": now,
                    }
                },
                return_document=ReturnDocument.AFTER,
            )
        except Exception as exc:
            raise LedgerFatalError(f"failed to heartbeat source mutation: {source}") from exc
        if document is None:
            raise LedgerFatalError(f"mutation is not active: {source}/{mutation_id}")
        return expires_at

    @staticmethod
    def _record_abort_failure(
        primary: BaseException,
        secondary: BaseException,
    ) -> None:
        try:
            setattr(primary, "_revision_ledger_secondary_failure", True)
            setattr(primary, "_revision_ledger_secondary_error", secondary)
        except Exception:
            pass
        note = f"secondary revision-ledger abort failure: {secondary!r}"
        add_note = getattr(primary, "add_note", None)
        if callable(add_note):
            add_note(note)
        log.error(
            "%s",
            note,
            exc_info=(type(secondary), secondary, secondary.__traceback__),
        )

    @contextmanager
    def mutation(
        self,
        source: str,
        writer_id: str,
        *,
        owner_id: str,
        heartbeat_seconds: float,
    ) -> Iterator[int]:
        """Open one revision for an entire writer job, then commit or abort."""

        mutation_id = f"{writer_id}:{uuid.uuid4()}"
        revision = self.begin(source, mutation_id, owner_id=owner_id)
        capability = LedgerMutationCapability(source=source, mutation_id=mutation_id)
        context_token = _CURRENT_CAPABILITY.set(capability)
        stop = threading.Event()

        def heartbeat_loop() -> None:
            while not stop.wait(heartbeat_seconds):
                try:
                    self.heartbeat(source, mutation_id)
                except BaseException as exc:
                    fatal = (
                        exc
                        if isinstance(exc, LedgerFatalError)
                        else LedgerFatalError(
                            f"source mutation heartbeat failed: {source}"
                        )
                    )
                    capability.fail(fatal)
                    return

        heartbeat_thread = threading.Thread(
            target=heartbeat_loop,
            name=f"revision-ledger-{writer_id}",
            daemon=True,
        )
        heartbeat_started = False

        def stop_heartbeat() -> Optional[LedgerFatalError]:
            stop.set()
            if heartbeat_started:
                heartbeat_thread.join(timeout=6.0)
                if heartbeat_thread.is_alive():
                    return capability.fail(
                        LedgerFatalError(
                            f"source mutation heartbeat thread did not stop: {source}"
                        )
                    )
            return capability.failure()

        try:
            try:
                heartbeat_thread.start()
                heartbeat_started = True
            except BaseException as exc:
                fatal = LedgerFatalError(
                    f"failed to start source mutation heartbeat: {source}"
                )
                capability.fail(fatal)
                capability.deactivate()
                try:
                    self.abort(source, mutation_id)
                except BaseException as secondary:
                    self._record_abort_failure(fatal, secondary)
                raise fatal from exc

            try:
                yield revision
            except BaseException as primary:
                fatal = stop_heartbeat()
                capability.deactivate()
                try:
                    self.abort(source, mutation_id)
                except BaseException as secondary:
                    abort_fatal = (
                        secondary
                        if isinstance(secondary, LedgerFatalError)
                        else LedgerFatalError(
                            f"failed to abort source mutation: {source}"
                        )
                    )
                    self._record_abort_failure(primary, abort_fatal)
                    fatal = fatal or abort_fatal

                if isinstance(primary, LedgerFatalError):
                    if fatal is not None and fatal is not primary:
                        self._record_abort_failure(primary, fatal)
                    raise
                if fatal is not None:
                    try:
                        setattr(fatal, "_revision_ledger_business_error", primary)
                    except Exception:
                        pass
                    raise fatal from primary
                raise
            else:
                fatal = stop_heartbeat()
                capability.deactivate()
                if fatal is not None:
                    try:
                        self.abort(source, mutation_id)
                    except BaseException as secondary:
                        self._record_abort_failure(fatal, secondary)
                    raise fatal
                self.commit(source, mutation_id)
        finally:
            stop.set()
            capability.deactivate()
            _CURRENT_CAPABILITY.reset(context_token)


@contextmanager
def revision_ledger_job(
    source: str,
    writer_id: str,
    mongo_uri: Optional[str],
    *,
    collection: Optional[Any] = None,
    owner_id: Optional[str] = None,
) -> Iterator[Optional[int]]:
    """Ledger one job when enabled; remain a zero-I/O no-op when disabled.

    In ``on`` mode all connection, index, begin, commit, and abort failures
    propagate.  Writers therefore fail closed instead of writing without a
    revision record.
    """

    try:
        mode = source_ledger_mode()
    except Exception as exc:
        raise LedgerFatalError(f"invalid source ledger mode: {exc}") from exc
    if mode == "off":
        yield None
        return

    ttl_seconds, heartbeat_seconds = ledger_timing()
    resolved_uri = (mongo_uri or os.getenv("MONGO_URI", "")).strip()
    if collection is None and not resolved_uri:
        raise LedgerFatalError(
            "ledger mode is on but no Mongo URI is available; pass settings.mongo_uri "
            "or set MONGO_URI"
        )
    resolved_owner = owner_id or f"{socket.gethostname()}:{os.getpid()}:{writer_id}"
    client = None
    try:
        if collection is None:
            try:
                client = MongoClient(
                    resolved_uri,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                )
                client.admin.command("ping")
                database = (
                    os.getenv(LEDGER_DB_ENV, DEFAULT_LEDGER_DB).strip()
                    or DEFAULT_LEDGER_DB
                )
                collection_name = (
                    os.getenv(LEDGER_COLLECTION_ENV, DEFAULT_LEDGER_COLLECTION).strip()
                    or DEFAULT_LEDGER_COLLECTION
                )
                collection = client[database][collection_name]
            except Exception as exc:
                raise LedgerFatalError(
                    "failed to connect to source revision ledger"
                ) from exc

        ledger = SourceRevisionLedger(collection, ttl_seconds=ttl_seconds)
        with ledger.mutation(
            source,
            writer_id,
            owner_id=resolved_owner,
            heartbeat_seconds=heartbeat_seconds,
        ) as revision:
            yield revision
    finally:
        if client is not None:
            client.close()


__all__ = [
    "DEFAULT_LEDGER_COLLECTION",
    "DEFAULT_LEDGER_DB",
    "DEFAULT_LEDGER_HEARTBEAT_SECONDS",
    "DEFAULT_LEDGER_TTL_SECONDS",
    "INDEX_CONSTITUENTS_SOURCE",
    "LEDGER_HEARTBEAT_SECONDS_ENV",
    "LEDGER_COLLECTION_ENV",
    "LEDGER_DB_ENV",
    "LEDGER_SOURCES",
    "LEDGER_TTL_SECONDS_ENV",
    "LedgerFatalError",
    "LedgerMutationCapability",
    "LedgerSyncFatalError",
    "SOURCE_LEDGER_MODE_ENV",
    "STOCK_ADJ_FACTOR_SOURCE",
    "SW_INDEX_MEMBER_SOURCE",
    "SourceRevisionLedger",
    "VOLUME_PRICE_SOURCE",
    "WRITER_MANIFEST",
    "current_ledger_capability",
    "is_ledger_fatal",
    "ledger_timing",
    "require_active_ledger_token",
    "revision_ledger_job",
    "source_ledger_mode",
]
