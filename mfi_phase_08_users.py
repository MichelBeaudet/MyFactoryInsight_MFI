#!/usr/bin/env python3
# =============================================================================
# PROJECT      : MyFactoryInsight (MFI)
# FILE         : mfi_phase_08_users.py
# PHASE        : 8 — User Management
# PURPOSE      : Manage users, roles (admin/operator/viewer), and permissions.
#                PBKDF2-SHA256 password hashing (stdlib). Session token store
#                with TTL. AuthService for login/logout/validation. AccessLog
#                audit trail. FastAPI dependency injection stubs for integration
#                with Phase 6 dashboard and future API routes.
# AUTHOR       : Michel Beaudet
# CREATED      : 2026-05-16
# PYTHON       : 3.12+ (3.14 target-compatible syntax)
# DEPENDENCIES : pydantic>=2.0, fastapi (for dependency stubs)
#                stdlib: hashlib, hmac, secrets, base64, time, uuid
# CLI          : python mfi_phase_08_users.py --self-test
#                python mfi_phase_08_users.py --list-users
#                python mfi_phase_08_users.py --add-user <name> <role>
#                python mfi_phase_08_users.py --reset-password <name>
# =============================================================================

# =============================================================================
# SECTION 1 — IMPORTS
# =============================================================================
import argparse
import base64
import dataclasses
import hashlib
import hmac
import json
import logging
import secrets
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional

# --- Pydantic ---
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
except ImportError as exc:
    print(f"[FATAL] pydantic not found: {exc}", file=sys.stderr)
    sys.exit(1)

# --- FastAPI (for dependency stubs) ---
try:
    from fastapi import Depends, HTTPException, status
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False

# =============================================================================
# SECTION 2 — CONFIG / CONSTANTS
# =============================================================================
PHASE_ID            = "08"
PHASE_NAME          = "User Management"
PHASE_VERSION       = "1.0.0"

# ── Roles ─────────────────────────────────────────────────────────────────────
ROLE_ADMIN      = "admin"
ROLE_OPERATOR   = "operator"
ROLE_VIEWER     = "viewer"
VALID_ROLES     = (ROLE_ADMIN, ROLE_OPERATOR, ROLE_VIEWER)

# ── Permissions ───────────────────────────────────────────────────────────────
PERM_READ_DATA      = "read_data"       # View dashboard, machines, KPIs
PERM_VIEW_REPORTS   = "view_reports"    # Access PDF/Excel reports
PERM_ACK_ALERTS     = "ack_alerts"      # Acknowledge alert events
PERM_CONTROL        = "control_machine" # Future: machine control commands
PERM_WRITE_CONFIG   = "write_config"    # Modify thresholds, rules, settings
PERM_MANAGE_USERS   = "manage_users"    # Create/edit/delete users

ALL_PERMISSIONS: tuple[str, ...] = (
    PERM_READ_DATA, PERM_VIEW_REPORTS, PERM_ACK_ALERTS,
    PERM_CONTROL, PERM_WRITE_CONFIG, PERM_MANAGE_USERS,
)

# ── Permission matrix by role ─────────────────────────────────────────────────
ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    ROLE_ADMIN    : frozenset(ALL_PERMISSIONS),
    ROLE_OPERATOR : frozenset({PERM_READ_DATA, PERM_VIEW_REPORTS,
                               PERM_ACK_ALERTS, PERM_CONTROL}),
    ROLE_VIEWER   : frozenset({PERM_READ_DATA, PERM_VIEW_REPORTS}),
}

# ── Password hashing ──────────────────────────────────────────────────────────
PBKDF2_ALGORITHM    = "sha256"
PBKDF2_ITERATIONS   = 260_000      # OWASP 2023 recommendation for SHA-256
SALT_BYTES          = 32           # 256-bit salt
KEY_BYTES           = 32           # 256-bit derived key

# ── Session tokens ────────────────────────────────────────────────────────────
TOKEN_BYTES         = 32           # 256-bit URL-safe token
TOKEN_TTL_SEC       = 3600         # 1 hour default session TTL
TOKEN_MAX_PER_USER  = 5            # Max concurrent sessions per user

# ── Default users (created at startup if store is empty) ─────────────────────
DEFAULT_USERS: list[dict[str, str]] = [
    {"username": "admin",    "password": "admin123",  "role": ROLE_ADMIN},
    {"username": "operator", "password": "operator1", "role": ROLE_OPERATOR},
    {"username": "viewer",   "password": "viewer123", "role": ROLE_VIEWER},
]

# ── Access log ────────────────────────────────────────────────────────────────
ACCESS_LOG_MAX      = 500          # Max access log entries in memory

# =============================================================================
# SECTION 3 — LOGGER SETUP
# =============================================================================
LOG_FORMAT = (
    "[%(asctime)s] "
    "[%(levelname)-8s] "
    "[MFI-P%(phase)s] "
    "[%(funcName)s] "
    "%(message)s"
)


class PhaseAdapter(logging.LoggerAdapter):
    """Injects phase ID into every log record."""

    def process(self, msg: str, kwargs: dict) -> tuple[str, dict]:
        kwargs.setdefault("extra", {})
        kwargs["extra"]["phase"] = PHASE_ID
        return msg, kwargs


def build_logger(name: str, level: int = logging.DEBUG) -> PhaseAdapter:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    base = logging.getLogger(name)
    base.setLevel(level)
    base.handlers.clear()
    base.addHandler(handler)
    base.propagate = False
    return PhaseAdapter(base, extra={"phase": PHASE_ID})


LOG = build_logger("mfi.phase08")

# =============================================================================
# SECTION 4 — PASSWORD HASHER
# =============================================================================

class PasswordHasher:
    """
    PBKDF2-SHA256 password hasher using Python stdlib only.

    Storage format: base64(salt_bytes + derived_key_bytes)
    The salt and key are concatenated then base64-encoded into a single
    opaque string suitable for storage in a user record.

    Security properties:
      - 256-bit random salt per password (prevents rainbow table attacks).
      - 260,000 PBKDF2-SHA256 iterations (OWASP 2023 minimum for SHA-256).
      - Constant-time comparison via hmac.compare_digest (prevents timing attacks).
    """

    @staticmethod
    def hash(password: str) -> str:
        """
        Hash a plaintext password with a fresh random salt.

        Args:
            password : Plaintext password (unicode string).

        Returns:
            Base64-encoded string: salt || derived_key.
        """
        if not password:
            raise ValueError("Password must not be empty.")

        salt    = secrets.token_bytes(SALT_BYTES)
        key     = hashlib.pbkdf2_hmac(
            PBKDF2_ALGORITHM,
            password.encode("utf-8"),
            salt,
            PBKDF2_ITERATIONS,
            dklen=KEY_BYTES,
        )
        return base64.b64encode(salt + key).decode("ascii")

    @staticmethod
    def verify(password: str, stored_hash: str) -> bool:
        """
        Verify a plaintext password against a stored hash.

        Args:
            password    : Plaintext password to verify.
            stored_hash : Previously stored hash string from hash().

        Returns:
            True if password matches, False otherwise.
            Always returns False on any decoding or format error.
        """
        if not password or not stored_hash:
            return False

        try:
            raw     = base64.b64decode(stored_hash.encode("ascii"))
            salt    = raw[:SALT_BYTES]
            stored  = raw[SALT_BYTES:]

            candidate = hashlib.pbkdf2_hmac(
                PBKDF2_ALGORITHM,
                password.encode("utf-8"),
                salt,
                PBKDF2_ITERATIONS,
                dklen=KEY_BYTES,
            )
            return hmac.compare_digest(stored, candidate)

        except Exception:
            return False


# Singleton instance
_HASHER = PasswordHasher()

# =============================================================================
# SECTION 5 — USER MODEL (Pydantic)
# =============================================================================

class User(BaseModel):
    """
    MFI user record — stored in UserStore.

    Fields
    ------
    user_id         : UUID string, auto-generated on creation.
    username        : Unique login name (3–32 chars, alphanum + _ .).
    hashed_password : PBKDF2-SHA256 hash from PasswordHasher.hash().
    role            : One of admin / operator / viewer.
    active          : If False, login is denied regardless of password.
    created_at      : ISO 8601 UTC creation timestamp.
    last_login      : ISO 8601 UTC of most recent successful login (or None).
    """

    model_config = {"validate_assignment": True}

    user_id         : str   = Field(default_factory=lambda: str(uuid.uuid4())[:16])
    username        : str   = Field(..., min_length=3, max_length=32)
    hashed_password : str   = Field(..., min_length=10)
    role            : str   = Field(...)
    active          : bool  = Field(default=True)
    created_at      : str   = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    last_login      : Optional[str] = Field(default=None)

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: str) -> str:
        import re
        if not re.match(r"^[a-zA-Z0-9_.]{3,32}$", v):
            raise ValueError(
                "Username must be 3–32 chars: letters, digits, underscore, dot."
            )
        return v.lower()

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        if v not in VALID_ROLES:
            raise ValueError(
                f"Invalid role '{v}'. Valid: {VALID_ROLES}"
            )
        return v

    def has_permission(self, permission: str) -> bool:
        """
        Check whether this user's role grants the requested permission.

        Args:
            permission : One of the PERM_* constants.

        Returns:
            True if the role has this permission.
        """
        return permission in ROLE_PERMISSIONS.get(self.role, frozenset())

    def permissions(self) -> frozenset[str]:
        """Return the full permission set for this user's role."""
        return ROLE_PERMISSIONS.get(self.role, frozenset())

    def safe_dict(self) -> dict[str, Any]:
        """Return user dict with hashed_password excluded (safe for API responses)."""
        d = self.model_dump()
        d.pop("hashed_password", None)
        d["permissions"] = sorted(self.permissions())
        return d

    def __repr__(self) -> str:
        return (
            f"User(id={self.user_id!r}, "
            f"username={self.username!r}, "
            f"role={self.role!r}, "
            f"active={self.active})"
        )


# =============================================================================
# SECTION 6 — SESSION TOKEN MODEL (Pydantic)
# =============================================================================

class SessionToken(BaseModel):
    """
    MFI session token — issued on successful login, stored in TokenStore.

    Fields
    ------
    token       : URL-safe random string (256-bit).
    user_id     : Owner user_id.
    username    : Owner username (denormalized for fast lookup).
    role        : Owner role at time of token creation.
    issued_at   : Unix timestamp when token was issued.
    expires_at  : Unix timestamp when token expires.
    """

    token       : str   = Field(default_factory=lambda: secrets.token_urlsafe(TOKEN_BYTES))
    user_id     : str
    username    : str
    role        : str
    issued_at   : float = Field(default_factory=time.time)
    expires_at  : float = Field(default=0.0)

    def model_post_init(self, __context: Any) -> None:
        """Set expires_at from issued_at + TTL if not explicitly provided."""
        if self.expires_at == 0.0:
            object.__setattr__(self, "expires_at", self.issued_at + TOKEN_TTL_SEC)

    def is_valid(self) -> bool:
        """Return True if the token has not expired."""
        return time.time() < self.expires_at

    def seconds_remaining(self) -> float:
        """Return seconds until token expires (negative if expired)."""
        return self.expires_at - time.time()

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    def __repr__(self) -> str:
        return (
            f"SessionToken("
            f"user={self.username!r}, "
            f"role={self.role!r}, "
            f"expires_in={self.seconds_remaining():.0f}s)"
        )


# =============================================================================
# SECTION 7 — USER STORE
# =============================================================================

class UserStore:
    """
    In-memory user registry with optional JSON file persistence.

    Stores User objects keyed by username. Loaded from file on init
    if a path is provided and the file exists; saved after mutations.

    Thread safety: basic (single-process assumption for Phase 8).
    """

    def __init__(self, file_path: Optional[str] = None) -> None:
        """
        Initialize the store and optionally load from file.

        Args:
            file_path : JSON file path for persistence. None = in-memory only.
        """
        self._users     : dict[str, User] = {}     # username → User
        self._file_path = file_path

        if file_path:
            self._load()

        LOG.info(
            "UserStore initialized │ users=%d │ file=%s",
            len(self._users),
            file_path or "none",
        )

    # ── Persistence ───────────────────────────────────────────────────────

    def _load(self) -> None:
        """Load users from JSON file. Silently skips if file does not exist."""
        import os
        if not self._file_path or not os.path.exists(self._file_path):
            return
        try:
            with open(self._file_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            for record in data:
                user = User(**record)
                self._users[user.username] = user
            LOG.info("UserStore loaded %d users from %s", len(self._users), self._file_path)
        except (OSError, json.JSONDecodeError, ValidationError) as exc:
            LOG.error("UserStore load ERROR: %s", exc)

    def _save(self) -> None:
        """Save all users to JSON file. Silently skips if no file path set."""
        if not self._file_path:
            return
        try:
            data = [u.model_dump() for u in self._users.values()]
            with open(self._file_path, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
            LOG.debug("UserStore saved %d users to %s", len(self._users), self._file_path)
        except OSError as exc:
            LOG.error("UserStore save ERROR: %s", exc)

    # ── CRUD operations ───────────────────────────────────────────────────

    def add(self, user: User) -> None:
        """
        Add a new user. Raises ValueError if username already exists.

        Args:
            user : Validated User object.

        Raises:
            ValueError : If username is already taken.
        """
        if user.username in self._users:
            raise ValueError(f"Username '{user.username}' already exists.")
        self._users[user.username] = user
        self._save()
        LOG.info("User added │ username=%s │ role=%s", user.username, user.role)

    def get(self, username: str) -> Optional[User]:
        """
        Retrieve a user by username.

        Args:
            username : Username to look up.

        Returns:
            User if found, None otherwise.
        """
        return self._users.get(username.lower())

    def get_by_id(self, user_id: str) -> Optional[User]:
        """
        Retrieve a user by user_id.

        Args:
            user_id : UUID-style user identifier.

        Returns:
            User if found, None otherwise.
        """
        for user in self._users.values():
            if user.user_id == user_id:
                return user
        return None

    def update(self, user: User) -> None:
        """
        Replace an existing user record.

        Args:
            user : Updated User object (must already exist by username).

        Raises:
            ValueError : If username not found in store.
        """
        if user.username not in self._users:
            raise ValueError(f"User '{user.username}' not found.")
        self._users[user.username] = user
        self._save()

    def delete(self, username: str) -> bool:
        """
        Remove a user by username.

        Args:
            username : Username to delete.

        Returns:
            True if deleted, False if not found.
        """
        if username.lower() not in self._users:
            return False
        del self._users[username.lower()]
        self._save()
        LOG.info("User deleted │ username=%s", username)
        return True

    def list_all(self) -> list[User]:
        """Return all users sorted by username."""
        return sorted(self._users.values(), key=lambda u: u.username)

    def count(self) -> int:
        """Return number of users in the store."""
        return len(self._users)

    def set_password(self, username: str, new_password: str) -> bool:
        """
        Hash and store a new password for a user.

        Args:
            username     : Target username.
            new_password : New plaintext password.

        Returns:
            True if updated, False if user not found.
        """
        user = self.get(username)
        if user is None:
            return False
        updated = user.model_copy(
            update={"hashed_password": _HASHER.hash(new_password)}
        )
        self.update(updated)
        LOG.info("Password updated │ username=%s", username)
        return True

    def set_active(self, username: str, active: bool) -> bool:
        """
        Enable or disable a user account.

        Args:
            username : Target username.
            active   : New active state.

        Returns:
            True if updated, False if user not found.
        """
        user = self.get(username)
        if user is None:
            return False
        self.update(user.model_copy(update={"active": active}))
        LOG.info("User active=%s │ username=%s", active, username)
        return True


# =============================================================================
# SECTION 8 — TOKEN STORE
# =============================================================================

class TokenStore:
    """
    In-memory session token registry with automatic expiry cleanup.

    Maps token string → SessionToken. Expired tokens are lazily purged
    on each get() and at explicit cleanup() calls.
    """

    def __init__(self) -> None:
        self._tokens : dict[str, SessionToken] = {}
        LOG.info("TokenStore initialized")

    def issue(self, user: User, ttl_sec: float = TOKEN_TTL_SEC) -> SessionToken:
        """
        Issue a new session token for a user.

        Enforces TOKEN_MAX_PER_USER by revoking the oldest token when
        the user already has the maximum number of active sessions.

        Args:
            user    : Authenticated user.
            ttl_sec : Token lifetime in seconds.

        Returns:
            Newly issued SessionToken.
        """
        # Purge expired tokens first
        self.cleanup()

        # Enforce per-user session limit
        user_tokens = [
            t for t in self._tokens.values()
            if t.user_id == user.user_id and t.is_valid()
        ]
        if len(user_tokens) >= TOKEN_MAX_PER_USER:
            oldest = min(user_tokens, key=lambda t: t.issued_at)
            self.revoke(oldest.token)
            LOG.debug(
                "Session limit reached for %s — oldest token revoked",
                user.username,
            )

        token = SessionToken(
            user_id     = user.user_id,
            username    = user.username,
            role        = user.role,
            expires_at  = time.time() + ttl_sec,
        )
        self._tokens[token.token] = token
        LOG.debug(
            "Token issued │ user=%s │ role=%s │ ttl=%.0fs",
            user.username, user.role, ttl_sec,
        )
        return token

    def get(self, token_str: str) -> Optional[SessionToken]:
        """
        Retrieve and validate a token by its string value.

        Expired tokens are removed and None is returned.

        Args:
            token_str : Raw token string from Authorization header.

        Returns:
            Valid SessionToken, or None if not found / expired.
        """
        token = self._tokens.get(token_str)
        if token is None:
            return None
        if not token.is_valid():
            del self._tokens[token_str]
            LOG.debug("Expired token removed │ user=%s", token.username)
            return None
        return token

    def revoke(self, token_str: str) -> bool:
        """
        Revoke a specific token (logout).

        Args:
            token_str : Token string to revoke.

        Returns:
            True if found and revoked, False if not found.
        """
        if token_str in self._tokens:
            username = self._tokens[token_str].username
            del self._tokens[token_str]
            LOG.debug("Token revoked │ user=%s", username)
            return True
        return False

    def revoke_all(self, user_id: str) -> int:
        """
        Revoke all tokens for a given user (force logout everywhere).

        Args:
            user_id : User whose sessions to terminate.

        Returns:
            Number of tokens revoked.
        """
        to_revoke = [
            k for k, t in self._tokens.items() if t.user_id == user_id
        ]
        for k in to_revoke:
            del self._tokens[k]
        LOG.info("All tokens revoked │ user_id=%s │ count=%d", user_id, len(to_revoke))
        return len(to_revoke)

    def cleanup(self) -> int:
        """
        Remove all expired tokens from the store.

        Returns:
            Number of tokens removed.
        """
        expired = [k for k, t in self._tokens.items() if not t.is_valid()]
        for k in expired:
            del self._tokens[k]
        if expired:
            LOG.debug("Token cleanup │ removed=%d", len(expired))
        return len(expired)

    def active_count(self) -> int:
        """Return number of currently active (non-expired) tokens."""
        self.cleanup()
        return len(self._tokens)

    def active_tokens(self, user_id: Optional[str] = None) -> list[SessionToken]:
        """
        Return list of active tokens, optionally filtered by user.

        Args:
            user_id : Filter by user. None = all users.

        Returns:
            List of valid SessionToken objects.
        """
        self.cleanup()
        tokens = list(self._tokens.values())
        if user_id:
            tokens = [t for t in tokens if t.user_id == user_id]
        return tokens


# =============================================================================
# SECTION 9 — ACCESS LOG
# =============================================================================

@dataclasses.dataclass
class AccessLogEntry:
    """
    Single access log entry — one authentication or authorization event.

    Attributes:
        timestamp   : ISO 8601 UTC timestamp.
        event_type  : LOGIN_OK | LOGIN_FAIL | LOGOUT | ACCESS_DENIED | ACCESS_OK.
        username    : Username involved (or "unknown").
        ip_address  : Client IP address (or "unknown").
        resource    : Resource or endpoint accessed.
        detail      : Additional context string.
    """

    timestamp   : str
    event_type  : str
    username    : str
    ip_address  : str
    resource    : str
    detail      : str

    EVENT_LOGIN_OK      = "LOGIN_OK"
    EVENT_LOGIN_FAIL    = "LOGIN_FAIL"
    EVENT_LOGOUT        = "LOGOUT"
    EVENT_ACCESS_DENIED = "ACCESS_DENIED"
    EVENT_ACCESS_OK     = "ACCESS_OK"
    EVENT_PASSWORD_CHANGE = "PASSWORD_CHANGE"
    EVENT_USER_CREATED  = "USER_CREATED"
    EVENT_USER_DELETED  = "USER_DELETED"

    def to_dict(self) -> dict[str, str]:
        return dataclasses.asdict(self)


class AccessLog:
    """
    In-memory access audit log — records all authentication and access events.

    Stores up to ACCESS_LOG_MAX entries in a deque (oldest dropped first).
    """

    def __init__(self, max_entries: int = ACCESS_LOG_MAX) -> None:
        self._entries: deque[AccessLogEntry] = deque(maxlen=max_entries)

    def record(
        self,
        event_type  : str,
        username    : str,
        resource    : str       = "—",
        ip_address  : str       = "unknown",
        detail      : str       = "",
    ) -> None:
        """
        Record one access event.

        Args:
            event_type : One of AccessLogEntry.EVENT_* constants.
            username   : User involved in the event.
            resource   : Resource or route accessed.
            ip_address : Client IP (from request context in production).
            detail     : Additional context.
        """
        entry = AccessLogEntry(
            timestamp   = datetime.now(timezone.utc).isoformat(),
            event_type  = event_type,
            username    = username,
            ip_address  = ip_address,
            resource    = resource,
            detail      = detail,
        )
        self._entries.append(entry)

        log_fn = LOG.warning if event_type in (
            AccessLogEntry.EVENT_LOGIN_FAIL,
            AccessLogEntry.EVENT_ACCESS_DENIED,
        ) else LOG.info

        log_fn(
            "ACCESS │ %-16s │ user=%-12s │ ip=%-15s │ res=%s %s",
            event_type,
            username,
            ip_address,
            resource,
            f"│ {detail}" if detail else "",
        )

    def query(
        self,
        event_type  : Optional[str] = None,
        username    : Optional[str] = None,
        limit       : int           = 100,
    ) -> list[AccessLogEntry]:
        """
        Query the access log with optional filters.

        Args:
            event_type : Filter by event type. None = all.
            username   : Filter by username. None = all.
            limit      : Maximum results (most recent first).

        Returns:
            List of matching AccessLogEntry (newest first).
        """
        result = list(self._entries)
        if event_type:
            result = [e for e in result if e.event_type == event_type]
        if username:
            result = [e for e in result if e.username == username]
        return list(reversed(result))[:limit]

    def export(self) -> list[dict[str, str]]:
        """Export all entries as JSON-serializable list of dicts."""
        return [e.to_dict() for e in self._entries]

    def stats(self) -> dict[str, Any]:
        """Return counts by event type."""
        counts: dict[str, int] = {}
        for e in self._entries:
            counts[e.event_type] = counts.get(e.event_type, 0) + 1
        return {"total": len(self._entries), "by_event": counts}


# =============================================================================
# SECTION 10 — AUTH SERVICE
# =============================================================================

class AuthError(Exception):
    """Raised when authentication or authorization fails."""
    def __init__(self, message: str, http_status: int = 401) -> None:
        super().__init__(message)
        self.http_status = http_status


class AuthService:
    """
    MFI Authentication and Authorization Service.

    Coordinates UserStore, TokenStore, and AccessLog to provide:
      - login()           : Verify credentials, issue session token.
      - logout()          : Revoke a specific token.
      - validate_token()  : Confirm token is valid and return associated user.
      - check_permission(): Assert that a user holds a required permission.
      - create_user()     : Admin operation to add a new user.
      - change_password() : Update password with old-password verification.

    All security events are recorded in the AccessLog.
    """

    def __init__(
        self,
        user_store  : Optional[UserStore]   = None,
        token_store : Optional[TokenStore]  = None,
        access_log  : Optional[AccessLog]   = None,
    ) -> None:
        """
        Initialize the service with optional injected stores.

        If stores are not provided, fresh in-memory instances are created
        and seeded with the default users.

        Args:
            user_store  : UserStore instance (None = new in-memory store).
            token_store : TokenStore instance (None = new in-memory store).
            access_log  : AccessLog instance (None = new in-memory log).
        """
        self._users     = user_store    or UserStore()
        self._tokens    = token_store   or TokenStore()
        self._log       = access_log    or AccessLog()

        # Seed default users if store is empty
        if self._users.count() == 0:
            self._seed_defaults()

        LOG.info(
            "AuthService initialized │ users=%d │ active_tokens=%d",
            self._users.count(),
            self._tokens.active_count(),
        )

    # ── Default seed ──────────────────────────────────────────────────────

    def _seed_defaults(self) -> None:
        """Create the default admin/operator/viewer accounts."""
        for spec in DEFAULT_USERS:
            try:
                user = User(
                    username        = spec["username"],
                    hashed_password = _HASHER.hash(spec["password"]),
                    role            = spec["role"],
                )
                self._users.add(user)
                self._log.record(
                    AccessLogEntry.EVENT_USER_CREATED,
                    username    = "system",
                    resource    = f"user:{spec['username']}",
                    detail      = f"role={spec['role']} (seeded)",
                )
            except Exception as exc:
                LOG.error("Seed user '%s' failed: %s", spec["username"], exc)

        LOG.info("Default users seeded: %s", [u["username"] for u in DEFAULT_USERS])

    # ── Authentication ────────────────────────────────────────────────────

    def login(
        self,
        username    : str,
        password    : str,
        ip_address  : str   = "unknown",
        ttl_sec     : float = TOKEN_TTL_SEC,
    ) -> SessionToken:
        """
        Authenticate a user and issue a session token.

        Args:
            username   : Username string.
            password   : Plaintext password.
            ip_address : Client IP (for access log).
            ttl_sec    : Token TTL in seconds.

        Returns:
            SessionToken on success.

        Raises:
            AuthError : On invalid credentials or inactive account.
        """
        user = self._users.get(username)

        # Deliberate constant-time path: always hash even on user-not-found
        # to prevent username enumeration via timing
        dummy_hash  = _HASHER.hash("dummy_prevent_timing")
        pw_ok       = _HASHER.verify(
            password,
            user.hashed_password if user else dummy_hash,
        )

        if user is None or not pw_ok:
            self._log.record(
                AccessLogEntry.EVENT_LOGIN_FAIL,
                username    = username,
                ip_address  = ip_address,
                resource    = "login",
                detail      = "invalid credentials",
            )
            raise AuthError("Invalid username or password.", http_status=401)

        if not user.active:
            self._log.record(
                AccessLogEntry.EVENT_LOGIN_FAIL,
                username    = username,
                ip_address  = ip_address,
                resource    = "login",
                detail      = "account disabled",
            )
            raise AuthError("Account is disabled.", http_status=403)

        token = self._tokens.issue(user, ttl_sec=ttl_sec)

        # Update last_login timestamp
        self._users.update(
            user.model_copy(
                update={"last_login": datetime.now(timezone.utc).isoformat()}
            )
        )

        self._log.record(
            AccessLogEntry.EVENT_LOGIN_OK,
            username    = username,
            ip_address  = ip_address,
            resource    = "login",
            detail      = f"role={user.role} ttl={ttl_sec:.0f}s",
        )
        return token

    def logout(self, token_str: str, ip_address: str = "unknown") -> bool:
        """
        Revoke a session token (logout).

        Args:
            token_str  : Token string to revoke.
            ip_address : Client IP (for access log).

        Returns:
            True if token was found and revoked, False if not found.
        """
        token = self._tokens.get(token_str)
        username = token.username if token else "unknown"
        revoked  = self._tokens.revoke(token_str)

        self._log.record(
            AccessLogEntry.EVENT_LOGOUT,
            username    = username,
            ip_address  = ip_address,
            resource    = "logout",
            detail      = f"revoked={revoked}",
        )
        return revoked

    # ── Token validation ──────────────────────────────────────────────────

    def validate_token(self, token_str: str) -> tuple[SessionToken, User]:
        """
        Validate a session token and return its associated user.

        Args:
            token_str : Raw token string from Authorization header.

        Returns:
            (SessionToken, User) tuple on success.

        Raises:
            AuthError : If token is missing, expired, or user not found.
        """
        if not token_str:
            raise AuthError("Authorization token required.", http_status=401)

        token = self._tokens.get(token_str)
        if token is None:
            raise AuthError("Token is invalid or expired.", http_status=401)

        user = self._users.get_by_id(token.user_id)
        if user is None or not user.active:
            self._tokens.revoke(token_str)
            raise AuthError("User account not found or disabled.", http_status=403)

        return token, user

    # ── Authorization ─────────────────────────────────────────────────────

    def check_permission(
        self,
        user        : User,
        permission  : str,
        resource    : str = "—",
        ip_address  : str = "unknown",
    ) -> None:
        """
        Assert that a user holds a required permission.

        Args:
            user       : Authenticated User object.
            permission : Required PERM_* constant.
            resource   : Resource being accessed (for access log).
            ip_address : Client IP (for access log).

        Raises:
            AuthError : If user does not have the required permission.
        """
        if user.has_permission(permission):
            self._log.record(
                AccessLogEntry.EVENT_ACCESS_OK,
                username    = user.username,
                ip_address  = ip_address,
                resource    = resource,
                detail      = f"perm={permission}",
            )
        else:
            self._log.record(
                AccessLogEntry.EVENT_ACCESS_DENIED,
                username    = user.username,
                ip_address  = ip_address,
                resource    = resource,
                detail      = f"perm={permission} denied for role={user.role}",
            )
            raise AuthError(
                f"Permission '{permission}' required. "
                f"Role '{user.role}' does not have this permission.",
                http_status = 403,
            )

    # ── User management ───────────────────────────────────────────────────

    def create_user(
        self,
        username    : str,
        password    : str,
        role        : str,
        created_by  : str = "system",
    ) -> User:
        """
        Create a new user. Caller must hold PERM_MANAGE_USERS.

        Args:
            username   : New username.
            password   : Initial plaintext password.
            role       : Role for the new user.
            created_by : Username of the admin creating this user.

        Returns:
            Newly created User object.

        Raises:
            ValueError : If username already exists or role is invalid.
        """
        user = User(
            username        = username,
            hashed_password = _HASHER.hash(password),
            role            = role,
        )
        self._users.add(user)
        self._log.record(
            AccessLogEntry.EVENT_USER_CREATED,
            username    = created_by,
            resource    = f"user:{username}",
            detail      = f"role={role}",
        )
        return user

    def change_password(
        self,
        username        : str,
        old_password    : str,
        new_password    : str,
        ip_address      : str = "unknown",
    ) -> bool:
        """
        Change a user's password after verifying the old password.

        Args:
            username     : Target username.
            old_password : Current plaintext password for verification.
            new_password : New plaintext password.
            ip_address   : Client IP (for access log).

        Returns:
            True on success.

        Raises:
            AuthError : If old password is incorrect or user not found.
        """
        user = self._users.get(username)
        if user is None:
            raise AuthError("User not found.", http_status=404)
        if not _HASHER.verify(old_password, user.hashed_password):
            self._log.record(
                AccessLogEntry.EVENT_LOGIN_FAIL,
                username    = username,
                ip_address  = ip_address,
                resource    = "change_password",
                detail      = "wrong old password",
            )
            raise AuthError("Old password is incorrect.", http_status=401)

        self._users.set_password(username, new_password)
        self._tokens.revoke_all(user.user_id)   # Invalidate existing sessions
        self._log.record(
            AccessLogEntry.EVENT_PASSWORD_CHANGE,
            username    = username,
            ip_address  = ip_address,
            resource    = "change_password",
            detail      = "all sessions revoked",
        )
        return True

    # ── Accessors ─────────────────────────────────────────────────────────

    def user_store(self) -> UserStore:
        """Return the underlying UserStore."""
        return self._users

    def token_store(self) -> TokenStore:
        """Return the underlying TokenStore."""
        return self._tokens

    def access_log(self) -> AccessLog:
        """Return the underlying AccessLog."""
        return self._log

    def summary(self) -> dict[str, Any]:
        """Return service status summary."""
        return {
            "users"         : self._users.count(),
            "active_sessions": self._tokens.active_count(),
            "access_log"    : self._log.stats(),
        }


# =============================================================================
# SECTION 11 — FASTAPI DEPENDENCY STUBS
# =============================================================================

def make_fastapi_deps(auth_service: AuthService) -> dict[str, Any]:
    """
    Create FastAPI dependency injection callables for a given AuthService.

    These are dependency factories — call each to get a FastAPI Depends()
    compatible function. Designed for injection into Phase 6 dashboard
    routes and future Phase 9+ API routes.

    Usage in FastAPI route:
        deps = make_fastapi_deps(auth_service)
        @app.get("/api/machines")
        async def get_machines(
            current_user = Depends(deps["get_current_user"])
        ):
            ...

    Args:
        auth_service : Configured AuthService instance.

    Returns:
        Dict with "get_current_user" and "require_permission" factory.
    """
    if not _FASTAPI_AVAILABLE:
        LOG.warning("FastAPI not available — dependency stubs are no-ops.")
        return {}

    _bearer = HTTPBearer(auto_error=False)

    async def get_current_user(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
    ) -> User:
        """
        FastAPI dependency: validate Bearer token and return current User.

        Raises:
            HTTPException 401 : If token is missing or invalid.
            HTTPException 403 : If account is disabled.
        """
        if credentials is None:
            raise HTTPException(
                status_code = status.HTTP_401_UNAUTHORIZED,
                detail      = "Authorization Bearer token required.",
                headers     = {"WWW-Authenticate": "Bearer"},
            )
        try:
            _, user = auth_service.validate_token(credentials.credentials)
            return user
        except AuthError as exc:
            raise HTTPException(
                status_code = exc.http_status,
                detail      = str(exc),
                headers     = {"WWW-Authenticate": "Bearer"},
            )

    def require_permission(permission: str):
        """
        FastAPI dependency factory: ensure current user has a permission.

        Usage:
            @app.post("/api/config")
            async def update_config(
                user = Depends(require_permission(PERM_WRITE_CONFIG))
            ):
                ...

        Args:
            permission : Required PERM_* constant.

        Returns:
            FastAPI dependency function.
        """
        async def _dependency(
            current_user: User = Depends(get_current_user),
        ) -> User:
            try:
                auth_service.check_permission(current_user, permission)
            except AuthError as exc:
                raise HTTPException(
                    status_code = exc.http_status,
                    detail      = str(exc),
                )
            return current_user
        return _dependency

    return {
        "get_current_user"  : get_current_user,
        "require_permission": require_permission,
    }


# =============================================================================
# SECTION 12 — SELF-TEST
# =============================================================================

def run_self_test() -> bool:
    """
    Self-test for Phase 8. Validates:
      1.  PasswordHasher.hash() produces non-empty string.
      2.  PasswordHasher.verify() returns True for correct password.
      3.  PasswordHasher.verify() returns False for wrong password.
      4.  Two hashes of same password are different (unique salts).
      5.  User model accepts valid record.
      6.  User model rejects invalid role.
      7.  User model rejects invalid username (special chars).
      8.  User.has_permission() returns True for granted permission.
      9.  User.has_permission() returns False for denied permission.
      10. UserStore.add() stores and retrieves a user.
      11. UserStore.add() raises ValueError on duplicate username.
      12. UserStore.delete() removes a user.
      13. UserStore.set_password() updates password hash.
      14. TokenStore.issue() creates a valid token.
      15. TokenStore.get() returns None for expired token.
      16. TokenStore.revoke() removes token.
      17. TokenStore.revoke_all() clears all user sessions.
      18. AccessLog.record() stores entries.
      19. AccessLog.query(event_type=LOGIN_FAIL) filters correctly.
      20. AuthService seeds default 3 users on init.
      21. AuthService.login() returns SessionToken on correct credentials.
      22. AuthService.login() raises AuthError on wrong password.
      23. AuthService.login() raises AuthError for disabled account.
      24. AuthService.validate_token() returns (token, user) for valid token.
      25. AuthService.validate_token() raises AuthError for expired token.
      26. AuthService.check_permission() passes for admin on all permissions.
      27. AuthService.check_permission() raises AuthError for viewer on WRITE_CONFIG.
      28. AuthService.create_user() creates and stores new user.
      29. AuthService.change_password() rejects wrong old password.
      30. FastAPI dependency stubs are created without error.

    Returns:
        True if all assertions pass, False otherwise.
    """
    LOG.info("══════════ SELF-TEST START ══════════")
    passed = 0
    failed = 0

    def check(label: str, condition: bool, detail: str = "") -> None:
        nonlocal passed, failed
        if condition:
            LOG.info("  ✓ PASS │ %s", label)
            passed += 1
        else:
            LOG.error("  ✗ FAIL │ %s%s", label, f" → {detail}" if detail else "")
            failed += 1

    # ── Tests 1-4: PasswordHasher ─────────────────────────────────────────
    h1 = _HASHER.hash("mypassword")
    check("PasswordHasher.hash() produces non-empty string", len(h1) > 20)
    check("verify() True for correct password",  _HASHER.verify("mypassword", h1))
    check("verify() False for wrong password",   not _HASHER.verify("wrongpass", h1))
    h2 = _HASHER.hash("mypassword")
    check("Two hashes of same password differ (unique salts)", h1 != h2)

    # ── Tests 5-9: User model ─────────────────────────────────────────────
    try:
        u = User(username="testuser", hashed_password=h1, role=ROLE_OPERATOR)
        check("User model accepts valid record", u.role == ROLE_OPERATOR)
    except ValidationError as exc:
        check("User model accepts valid record", False, str(exc))

    try:
        User(username="bad", hashed_password=h1, role="superuser")
        check("User model rejects invalid role", False, "no error")
    except ValidationError:
        check("User model rejects invalid role", True)

    try:
        User(username="bad user!", hashed_password=h1, role=ROLE_VIEWER)
        check("User model rejects invalid username", False, "no error")
    except ValidationError:
        check("User model rejects invalid username", True)

    admin_user = User(username="admin_t", hashed_password=h1, role=ROLE_ADMIN)
    viewer_user = User(username="viewer_t", hashed_password=h1, role=ROLE_VIEWER)
    check(
        "admin has PERM_MANAGE_USERS",
        admin_user.has_permission(PERM_MANAGE_USERS),
    )
    check(
        "viewer does NOT have PERM_WRITE_CONFIG",
        not viewer_user.has_permission(PERM_WRITE_CONFIG),
    )

    # ── Tests 10-13: UserStore ────────────────────────────────────────────
    store = UserStore()
    store.add(admin_user)
    found = store.get("admin_t")
    check("UserStore.add() and get() work", found is not None and found.username == "admin_t")

    try:
        store.add(admin_user)
        check("UserStore.add() raises ValueError on duplicate", False, "no error")
    except ValueError:
        check("UserStore.add() raises ValueError on duplicate", True)

    deleted = store.delete("admin_t")
    check("UserStore.delete() removes user", deleted and store.get("admin_t") is None)

    store.add(viewer_user)
    store.set_password("viewer_t", "newpass456")
    updated = store.get("viewer_t")
    check(
        "UserStore.set_password() updates hash",
        updated is not None and _HASHER.verify("newpass456", updated.hashed_password),
    )

    # ── Tests 14-17: TokenStore ───────────────────────────────────────────
    tstore = TokenStore()
    token  = tstore.issue(admin_user, ttl_sec=3600)
    check("TokenStore.issue() creates valid token", token.is_valid())

    # Manually expire the token
    expired_token = SessionToken(
        user_id="x", username="x", role=ROLE_VIEWER,
        issued_at=time.time() - 7200,
        expires_at=time.time() - 3600,
    )
    tstore._tokens[expired_token.token] = expired_token
    result = tstore.get(expired_token.token)
    check("TokenStore.get() returns None for expired token", result is None)

    revoked = tstore.revoke(token.token)
    check("TokenStore.revoke() removes token", revoked)
    check("TokenStore.get() returns None after revoke", tstore.get(token.token) is None)

    token2 = tstore.issue(admin_user)
    token3 = tstore.issue(admin_user)
    count  = tstore.revoke_all(admin_user.user_id)
    check("TokenStore.revoke_all() clears sessions", count >= 2)

    # ── Tests 18-19: AccessLog ────────────────────────────────────────────
    alog = AccessLog()
    alog.record(AccessLogEntry.EVENT_LOGIN_OK,   username="alice", resource="login")
    alog.record(AccessLogEntry.EVENT_LOGIN_FAIL, username="bob",   resource="login", detail="bad pass")
    alog.record(AccessLogEntry.EVENT_LOGIN_FAIL, username="bob",   resource="login", detail="bad pass")
    check("AccessLog.record() stores entries", alog.stats()["total"] == 3)

    fails = alog.query(event_type=AccessLogEntry.EVENT_LOGIN_FAIL)
    check("AccessLog.query(event_type=LOGIN_FAIL) returns 2", len(fails) == 2)

    # ── Tests 20-29: AuthService ──────────────────────────────────────────
    auth = AuthService()
    check("AuthService seeds 3 default users", auth.user_store().count() == 3)

    token_admin = auth.login("admin", "admin123", ip_address="127.0.0.1")
    check(
        "AuthService.login() returns SessionToken",
        isinstance(token_admin, SessionToken) and token_admin.role == ROLE_ADMIN,
    )

    try:
        auth.login("admin", "wrongpass")
        check("AuthService.login() raises AuthError on wrong password", False)
    except AuthError:
        check("AuthService.login() raises AuthError on wrong password", True)

    # Disable admin and test
    auth.user_store().set_active("admin", False)
    try:
        auth.login("admin", "admin123")
        check("AuthService.login() raises AuthError for disabled account", False)
    except AuthError:
        check("AuthService.login() raises AuthError for disabled account", True)
    auth.user_store().set_active("admin", True)   # Re-enable

    # Validate token
    token_op = auth.login("operator", "operator1")
    t, u = auth.validate_token(token_op.token)
    check(
        "AuthService.validate_token() returns (token, user)",
        u.username == "operator" and u.role == ROLE_OPERATOR,
    )

    try:
        auth.validate_token("totally_fake_token_xyz")
        check("AuthService.validate_token() raises AuthError for fake token", False)
    except AuthError:
        check("AuthService.validate_token() raises AuthError for fake token", True)

    # Permissions
    admin_u = auth.user_store().get("admin")
    auth.check_permission(admin_u, PERM_MANAGE_USERS, resource="/api/users")
    check("check_permission() passes for admin on MANAGE_USERS", True)

    viewer_u = auth.user_store().get("viewer")
    try:
        auth.check_permission(viewer_u, PERM_WRITE_CONFIG)
        check("check_permission() raises AuthError for viewer on WRITE_CONFIG", False)
    except AuthError:
        check("check_permission() raises AuthError for viewer on WRITE_CONFIG", True)

    # Create user
    new_user = auth.create_user("newoper", "pass9876", ROLE_OPERATOR, created_by="admin")
    check(
        "AuthService.create_user() creates and stores user",
        auth.user_store().get("newoper") is not None,
    )

    # Change password with wrong old password
    try:
        auth.change_password("newoper", "wrongold", "newpass")
        check("AuthService.change_password() rejects wrong old password", False)
    except AuthError:
        check("AuthService.change_password() rejects wrong old password", True)

    # ── Test 30: FastAPI deps ─────────────────────────────────────────────
    deps = make_fastapi_deps(auth)
    check(
        "FastAPI dependency stubs created successfully",
        "get_current_user" in deps and "require_permission" in deps,
    )

    # --- Summary ---
    total = passed + failed
    LOG.info(
        "══════════ SELF-TEST RESULT: %d/%d PASSED %s ══════════",
        passed,
        total,
        "✓ OK" if failed == 0 else "✗ FAIL",
    )
    return failed == 0


# =============================================================================
# SECTION 13 — CLI / MAIN ENTRY POINT
# =============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser for Phase 8."""
    parser = argparse.ArgumentParser(
        prog        = "mfi_phase_08_users.py",
        description = (
            f"MFI Phase {PHASE_ID} — {PHASE_NAME} v{PHASE_VERSION}\n"
            "User management: roles, authentication, authorization, access log."
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--self-test",
        action  = "store_true",
        help    = "Run built-in self-test suite and exit.",
    )
    parser.add_argument(
        "--list-users",
        action  = "store_true",
        help    = "List all users in the store and exit.",
    )
    parser.add_argument(
        "--add-user",
        nargs   = 3,
        metavar = ("USERNAME", "PASSWORD", "ROLE"),
        help    = "Add a new user and exit. ROLE: admin|operator|viewer.",
    )
    parser.add_argument(
        "--reset-password",
        nargs   = 2,
        metavar = ("USERNAME", "NEW_PASSWORD"),
        help    = "Reset a user password and exit.",
    )
    parser.add_argument(
        "--user-file",
        type    = str,
        default = None,
        metavar = "FILE",
        help    = "JSON file for user store persistence.",
    )
    return parser


def main() -> None:
    """
    Phase 8 entry point.

    Modes:
      --self-test      : Run validation suite.
      --list-users     : Print all users (safe — no passwords shown).
      --add-user       : Create a new user.
      --reset-password : Change a user's password.
      (default)        : Print service summary and exit.
    """
    parser  = build_arg_parser()
    args    = parser.parse_args()

    # ── Phase header ──────────────────────────────────────────────────────
    LOG.info("╔══════════════════════════════════════════════╗")
    LOG.info("║  MyFactoryInsight  │  Phase %-2s │ %-20s ║", PHASE_ID, PHASE_NAME)
    LOG.info("║  Version %-7s   │                              ║", PHASE_VERSION)
    LOG.info("╚══════════════════════════════════════════════╝")

    # ── Self-test mode ────────────────────────────────────────────────────
    if args.self_test:
        success = run_self_test()
        sys.exit(0 if success else 1)

    # ── Initialize service ────────────────────────────────────────────────
    auth = AuthService(user_store=UserStore(file_path=args.user_file))

    # ── List users ────────────────────────────────────────────────────────
    if args.list_users:
        users = auth.user_store().list_all()
        LOG.info("Users in store (%d):", len(users))
        for u in users:
            LOG.info(
                "  %-16s │ role=%-10s │ active=%-5s │ last_login=%s",
                u.username, u.role, u.active,
                u.last_login or "never",
            )
        sys.exit(0)

    # ── Add user ──────────────────────────────────────────────────────────
    if args.add_user:
        username, password, role = args.add_user
        try:
            user = auth.create_user(username, password, role, created_by="cli")
            LOG.info("User created │ %s │ role=%s", user.username, user.role)
        except (ValueError, ValidationError) as exc:
            LOG.error("Failed to create user: %s", exc)
            sys.exit(1)
        sys.exit(0)

    # ── Reset password ────────────────────────────────────────────────────
    if args.reset_password:
        username, new_password = args.reset_password
        ok = auth.user_store().set_password(username, new_password)
        if ok:
            LOG.info("Password reset │ username=%s", username)
        else:
            LOG.error("User not found: %s", username)
            sys.exit(1)
        sys.exit(0)

    # ── Default: summary ──────────────────────────────────────────────────
    summ = auth.summary()
    LOG.info(
        "AuthService summary │ users=%d │ sessions=%d │ log_events=%d",
        summ["users"],
        summ["active_sessions"],
        summ["access_log"]["total"],
    )
    LOG.info(
        "Default users: %s",
        [u["username"] for u in DEFAULT_USERS],
    )
    LOG.info("Phase %s ready. Use --help for CLI options.", PHASE_ID)


# =============================================================================
# SECTION 14 — ENTRY GUARD
# =============================================================================
if __name__ == "__main__":
    main()
