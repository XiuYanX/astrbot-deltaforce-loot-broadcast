import asyncio
import copy
import json
import os
import tempfile

from astrbot.api import logger
try:
    from astrbot.api.platform import MessageType
except Exception:
    MessageType = None

from .runtime_paths import get_runtime_file_path
from .secret_store import SecretDecryptionError, SecretProtectionError, SecretProtector

DEFAULT_STORAGE_DATA = {
    "group_origins": [],
    "users": {},
}


class Storage:
    PRIVATE_MESSAGE_TYPE = "FriendMessage"
    GROUP_MESSAGE_TYPE = "GroupMessage"
    OTHER_MESSAGE_TYPE = "OtherMessage"
    LEGACY_PRIVATE_NOTIFY_ORIGIN_PREFIXES = (
        "friend:",
        "private:",
        "direct:",
        "dm:",
        "user:",
    )
    LEGACY_PUBLIC_NOTIFY_ORIGIN_PREFIXES = (
        "group:",
        "channel:",
        "guild:",
        "room:",
        "chatroom:",
        "discussion:",
    )

    def __init__(self, filepath=None):
        if filepath is None:
            filepath = get_runtime_file_path("df_red_data.json")
        self.filepath = os.path.abspath(filepath)
        self._lock = asyncio.Lock()
        self.secret_protector = SecretProtector()
        self._logged_secret_failures = set()
        self.data, needs_migration = self._load_from_disk()
        if needs_migration:
            try:
                self._write_atomic_file(self.data)
            except OSError as exc:
                logger.warning(
                    "Failed to persist secret migration for storage "
                    f"{self.filepath}: {type(exc).__name__}: {exc}"
                )

    @staticmethod
    def _normalize_sender_id(sender_id):
        return str(sender_id)

    @staticmethod
    def _normalize_group_origin(origin):
        if origin is None:
            return ""
        return str(origin).strip()

    @classmethod
    def _normalize_message_type(cls, message_type):
        normalized_message_type = cls._normalize_group_origin(message_type)
        if not normalized_message_type:
            return ""

        if MessageType is not None:
            try:
                return MessageType(normalized_message_type).value
            except Exception:
                pass

        lowered_message_type = normalized_message_type.lower()
        if lowered_message_type in {"friendmessage", "friend_message", "friend"}:
            return cls.PRIVATE_MESSAGE_TYPE
        if lowered_message_type in {"groupmessage", "group_message", "group"}:
            return cls.GROUP_MESSAGE_TYPE
        if lowered_message_type in {"othermessage", "other_message", "other"}:
            return cls.OTHER_MESSAGE_TYPE
        return ""

    @classmethod
    def _parse_origin(cls, origin):
        normalized_origin = cls._normalize_group_origin(origin)
        if not normalized_origin:
            return None

        official_parts = normalized_origin.split(":", 2)
        if len(official_parts) == 3:
            platform_id, message_type, session_id = official_parts
            normalized_message_type = cls._normalize_message_type(message_type)
            if platform_id and session_id and normalized_message_type:
                return {
                    "origin": normalized_origin,
                    "platform_id": platform_id,
                    "message_type": normalized_message_type,
                    "session_id": session_id,
                    "is_official": True,
                }

        legacy_parts = normalized_origin.split(":", 1)
        if len(legacy_parts) == 2:
            legacy_prefix, session_id = legacy_parts
            lowered_prefix = f"{legacy_prefix.lower()}:"
            if session_id:
                if lowered_prefix in cls.LEGACY_PRIVATE_NOTIFY_ORIGIN_PREFIXES:
                    return {
                        "origin": normalized_origin,
                        "platform_id": "",
                        "message_type": cls.PRIVATE_MESSAGE_TYPE,
                        "session_id": session_id,
                        "is_official": False,
                    }
                if lowered_prefix in cls.LEGACY_PUBLIC_NOTIFY_ORIGIN_PREFIXES:
                    return {
                        "origin": normalized_origin,
                        "platform_id": "",
                        "message_type": cls.GROUP_MESSAGE_TYPE,
                        "session_id": session_id,
                        "is_official": False,
                    }

        return None

    @classmethod
    def sanitize_private_notify_origin(cls, origin, *, sender_id=""):
        parsed_origin = cls._parse_origin(origin)
        if not parsed_origin:
            return ""
        if parsed_origin["message_type"] == cls.PRIVATE_MESSAGE_TYPE:
            return parsed_origin["origin"]
        return ""

    @classmethod
    def normalize_interaction_origin(cls, origin, *, sender_id=""):
        parsed_origin = cls._parse_origin(origin)
        if parsed_origin:
            return parsed_origin["origin"]
        return ""

    @classmethod
    def extract_platform_id(cls, origin):
        parsed_origin = cls._parse_origin(origin)
        if not parsed_origin:
            return ""
        return parsed_origin["platform_id"]

    @classmethod
    def build_private_origin(cls, platform_id, session_id):
        normalized_platform_id = cls._normalize_group_origin(platform_id)
        normalized_session_id = cls._normalize_group_origin(session_id)
        if not normalized_platform_id or not normalized_session_id:
            return ""
        return (
            f"{normalized_platform_id}:"
            f"{cls.PRIVATE_MESSAGE_TYPE}:"
            f"{normalized_session_id}"
        )

    @classmethod
    def derive_private_origin(cls, sender_id, *, primary_origin="", fallback_origin=""):
        normalized_sender_id = cls._normalize_group_origin(sender_id)
        if not normalized_sender_id:
            return ""

        for candidate_origin in (primary_origin, fallback_origin):
            safe_origin = cls.sanitize_private_notify_origin(
                candidate_origin,
                sender_id=normalized_sender_id,
            )
            if safe_origin:
                return safe_origin

            platform_id = cls.extract_platform_id(candidate_origin)
            if platform_id:
                derived_origin = cls.build_private_origin(
                    platform_id,
                    normalized_sender_id,
                )
                if derived_origin:
                    return derived_origin

        return ""

    def _load_from_disk(self):
        data = copy.deepcopy(DEFAULT_STORAGE_DATA)
        if not os.path.exists(self.filepath):
            return data, False

        try:
            with open(self.filepath, "r", encoding="utf-8") as file:
                loaded_data = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning(f"Failed to load storage from {self.filepath}: {type(exc).__name__}: {exc}")
            return data, False

        if not isinstance(loaded_data, dict):
            logger.warning(f"Storage file {self.filepath} does not contain a JSON object.")
            return data, False

        data.update({
            key: copy.deepcopy(value)
            for key, value in loaded_data.items()
            if key not in data
        })
        needs_migration = False

        group_origins = loaded_data.get("group_origins", [])
        if isinstance(group_origins, list):
            data["group_origins"] = [str(origin) for origin in group_origins if origin]
        else:
            logger.warning(f"Storage file {self.filepath} has invalid group_origins data.")

        users = loaded_data.get("users", {})
        if isinstance(users, dict):
            normalized_users = {}
            for sender_id, user_data in users.items():
                if not isinstance(user_data, dict):
                    continue
                normalized_user, migrated = self._normalize_user_record(
                    copy.deepcopy(user_data),
                    sender_id=str(sender_id),
                )
                normalized_users[str(sender_id)] = normalized_user
                needs_migration = needs_migration or migrated
            data["users"] = normalized_users
        else:
            logger.warning(f"Storage file {self.filepath} has invalid users data.")

        return data, needs_migration

    def _normalize_user_record(self, user_data, *, sender_id=""):
        migrated = False
        normalized = copy.deepcopy(user_data)
        original_notify_origin = self._normalize_group_origin(normalized.get("notify_origin", ""))
        original_interaction_origin = self._normalize_group_origin(
            normalized.get("interaction_origin", "")
        )
        normalized_notify_origin = self.sanitize_private_notify_origin(
            original_notify_origin,
            sender_id=sender_id,
        )
        normalized_interaction_origin = self.normalize_interaction_origin(
            original_interaction_origin or original_notify_origin,
            sender_id=sender_id,
        )
        if normalized_notify_origin:
            normalized["notify_origin"] = normalized_notify_origin
        else:
            normalized.pop("notify_origin", None)
        if normalized_interaction_origin:
            normalized["interaction_origin"] = normalized_interaction_origin
        else:
            normalized.pop("interaction_origin", None)
        if (
            original_notify_origin != normalized_notify_origin
            or original_interaction_origin != normalized_interaction_origin
        ):
            migrated = True

        secret_updates = {}
        try:
            if "openid" in normalized:
                secret_updates["openid_secret"] = self.secret_protector.protect(normalized.get("openid", ""))
            if "access_token" in normalized:
                secret_updates["access_token_secret"] = self.secret_protector.protect(normalized.get("access_token", ""))
        except SecretProtectionError as exc:
            logger.warning(
                "Secure storage unavailable while migrating persisted credentials in "
                f"{self.filepath}; keeping legacy plaintext values until the issue is fixed: {exc}"
            )
            return normalized, False

        if "openid" in normalized:
            normalized["openid_secret"] = secret_updates.get("openid_secret", "")
            normalized.pop("openid", None)
            migrated = True
        if "access_token" in normalized:
            normalized["access_token_secret"] = secret_updates.get("access_token_secret", "")
            normalized.pop("access_token", None)
            migrated = True

        return normalized, migrated

    def _log_secret_hydration_failure(self, sender_id, field_name, exc):
        failure_key = (str(sender_id), str(field_name))
        if failure_key in self._logged_secret_failures:
            return
        logger.warning(
            f"Failed to decrypt persisted {field_name} for sender {sender_id} "
            f"in {self.filepath}: {exc}"
        )
        self._logged_secret_failures.add(failure_key)

    def _hydrate_user_record(self, user_data, *, sender_id="<unknown>"):
        hydrated = copy.deepcopy(user_data)
        secret_errors = {}
        for secret_field, plain_field in (
            ("openid_secret", "openid"),
            ("access_token_secret", "access_token"),
        ):
            if secret_field not in hydrated:
                continue
            try:
                hydrated[plain_field] = self.secret_protector.unprotect(
                    hydrated.get(secret_field, "")
                )
                self._logged_secret_failures.discard((str(sender_id), plain_field))
            except SecretDecryptionError as exc:
                hydrated.pop(plain_field, None)
                secret_errors[plain_field] = str(exc)
                self._log_secret_hydration_failure(sender_id, plain_field, exc)
        if secret_errors:
            hydrated["_secret_errors"] = secret_errors
        else:
            hydrated.pop("_secret_errors", None)
        return hydrated

    def _set_user_secrets(self, user_state, openid=None, access_token=None):
        secret_updates = {}
        secret_removals = set()

        if openid is not None:
            secret_removals.add("openid")
            if openid:
                secret_updates["openid_secret"] = self.secret_protector.protect(openid)
            else:
                secret_updates["openid_secret"] = None

        if access_token is not None:
            secret_removals.add("access_token")
            if access_token:
                secret_updates["access_token_secret"] = self.secret_protector.protect(access_token)
            else:
                secret_updates["access_token_secret"] = None

        for field in secret_removals:
            user_state.pop(field, None)
        for field, value in secret_updates.items():
            if value:
                user_state[field] = value
            else:
                user_state.pop(field, None)

    @staticmethod
    def _restrict_file_permissions(path):
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass

    def _write_atomic_file(self, payload):
        directory = os.path.dirname(self.filepath)
        os.makedirs(directory, exist_ok=True)

        temp_fd, temp_path = tempfile.mkstemp(
            dir=directory,
            prefix=".df_red_",
            suffix=".tmp",
        )
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, indent=2)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp_path, self.filepath)
            self._restrict_file_permissions(self.filepath)
        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

    async def _persist_locked(self, new_data=None):
        snapshot_source = self.data if new_data is None else new_data
        snapshot = copy.deepcopy(snapshot_source)
        try:
            await asyncio.to_thread(self._write_atomic_file, snapshot)
        except OSError as exc:
            logger.error(f"Failed to save storage to {self.filepath}: {type(exc).__name__}: {exc}")
            raise
        if new_data is not None:
            self.data = snapshot

    async def add_user(
        self,
        sender_id,
        openid,
        access_token,
        name="",
        platform="qq",
        role_id="",
        notify_origin="",
        interaction_origin="",
    ):
        sender_id = self._normalize_sender_id(sender_id)
        async with self._lock:
            new_data = copy.deepcopy(self.data)
            users = new_data["users"]
            # Rebinding replaces the account for this sender, so account-specific
            # runtime state must start from a clean baseline.
            user_state = {
                "name": name,
                "platform": platform,
                "role_id": role_id,
                "binding_status": "active",
                "binding_status_reason": "",
                "last_match_time": "",
                "last_room_id": "",
                "last_item_flow_keys": [],
                "pending_broadcasts": [],
                "assets": [],
            }
            normalized_notify_origin = self.sanitize_private_notify_origin(
                notify_origin,
                sender_id=sender_id,
            )
            normalized_interaction_origin = self.normalize_interaction_origin(
                interaction_origin or notify_origin,
                sender_id=sender_id,
            )
            if normalized_notify_origin:
                user_state["notify_origin"] = normalized_notify_origin
            if normalized_interaction_origin:
                user_state["interaction_origin"] = normalized_interaction_origin
            self._set_user_secrets(user_state, openid=openid, access_token=access_token)
            users[sender_id] = user_state
            await self._persist_locked(new_data)

    async def remove_user(self, sender_id):
        sender_id = self._normalize_sender_id(sender_id)
        async with self._lock:
            if sender_id not in self.data["users"]:
                return False
            new_data = copy.deepcopy(self.data)
            del new_data["users"][sender_id]
            await self._persist_locked(new_data)
            return True

    async def add_group(self, origin):
        origin = self._normalize_group_origin(origin)
        if not origin:
            return False
        async with self._lock:
            new_data = copy.deepcopy(self.data)
            groups = new_data["group_origins"]
            if origin in groups:
                return False
            groups.append(origin)
            await self._persist_locked(new_data)
            return True

    async def remove_group(self, origin):
        origin = self._normalize_group_origin(origin)
        if not origin:
            return False
        async with self._lock:
            new_data = copy.deepcopy(self.data)
            groups = new_data["group_origins"]
            if origin not in groups:
                return False
            groups.remove(origin)
            await self._persist_locked(new_data)
            return True

    async def get_user(self, sender_id):
        sender_id = self._normalize_sender_id(sender_id)
        async with self._lock:
            user_data = self.data.get("users", {}).get(sender_id)
            if not isinstance(user_data, dict):
                return None
            return self._hydrate_user_record(user_data, sender_id=sender_id)

    async def get_users(self):
        async with self._lock:
            return {
                sender_id: self._hydrate_user_record(user_data, sender_id=sender_id)
                for sender_id, user_data in self.data.get("users", {}).items()
            }

    async def get_groups(self):
        async with self._lock:
            return list(self.data.get("group_origins", []))

    async def update_user_state(self, sender_id, **fields):
        if not fields:
            return False

        sender_id = self._normalize_sender_id(sender_id)
        async with self._lock:
            user_data = self.data.get("users", {}).get(sender_id)
            if not isinstance(user_data, dict):
                return False
            new_data = copy.deepcopy(self.data)
            user_data = new_data["users"].get(sender_id)
            openid = fields.pop("openid", None) if "openid" in fields else None
            access_token = fields.pop("access_token", None) if "access_token" in fields else None
            if "notify_origin" in fields:
                normalized_notify_origin = self.sanitize_private_notify_origin(
                    fields.pop("notify_origin"),
                    sender_id=sender_id,
                )
                if normalized_notify_origin:
                    user_data["notify_origin"] = normalized_notify_origin
                else:
                    user_data.pop("notify_origin", None)
            if "interaction_origin" in fields:
                normalized_interaction_origin = self.normalize_interaction_origin(
                    fields.pop("interaction_origin"),
                    sender_id=sender_id,
                )
                if normalized_interaction_origin:
                    user_data["interaction_origin"] = normalized_interaction_origin
                else:
                    user_data.pop("interaction_origin", None)
            user_data.update(copy.deepcopy(fields))
            self._set_user_secrets(user_data, openid=openid, access_token=access_token)
            await self._persist_locked(new_data)
            return True
