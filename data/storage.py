import asyncio
import copy
import json
import os
import tempfile

from astrbot.api import logger

from .runtime_paths import get_runtime_file_path
from .secret_store import SecretProtector

DEFAULT_STORAGE_DATA = {
    "group_origins": [],
    "users": {},
}


class Storage:
    def __init__(self, filepath=None):
        if filepath is None:
            filepath = get_runtime_file_path("df_red_data.json")
        self.filepath = os.path.abspath(filepath)
        self._lock = asyncio.Lock()
        self.secret_protector = SecretProtector()
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
                normalized_user, migrated = self._normalize_user_record(copy.deepcopy(user_data))
                normalized_users[str(sender_id)] = normalized_user
                needs_migration = needs_migration or migrated
            data["users"] = normalized_users
        else:
            logger.warning(f"Storage file {self.filepath} has invalid users data.")

        return data, needs_migration

    def _normalize_user_record(self, user_data):
        migrated = False
        normalized = copy.deepcopy(user_data)

        if "openid" in normalized:
            normalized["openid_secret"] = self.secret_protector.protect(normalized.get("openid", ""))
            normalized.pop("openid", None)
            migrated = True
        if "access_token" in normalized:
            normalized["access_token_secret"] = self.secret_protector.protect(normalized.get("access_token", ""))
            normalized.pop("access_token", None)
            migrated = True

        return normalized, migrated

    def _hydrate_user_record(self, user_data):
        hydrated = copy.deepcopy(user_data)
        if "openid_secret" in hydrated:
            hydrated["openid"] = self.secret_protector.unprotect(hydrated.get("openid_secret", ""))
        if "access_token_secret" in hydrated:
            hydrated["access_token"] = self.secret_protector.unprotect(hydrated.get("access_token_secret", ""))
        return hydrated

    def _set_user_secrets(self, user_state, openid=None, access_token=None):
        if openid is not None:
            if openid:
                user_state["openid_secret"] = self.secret_protector.protect(openid)
            else:
                user_state.pop("openid_secret", None)
            user_state.pop("openid", None)

        if access_token is not None:
            if access_token:
                user_state["access_token_secret"] = self.secret_protector.protect(access_token)
            else:
                user_state.pop("access_token_secret", None)
            user_state.pop("access_token", None)

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

    async def add_user(self, sender_id, openid, access_token, name="", platform="qq", role_id=""):
        sender_id = self._normalize_sender_id(sender_id)
        async with self._lock:
            new_data = copy.deepcopy(self.data)
            users = new_data["users"]
            user_state = copy.deepcopy(users.get(sender_id, {}))
            user_state.update({
                "name": name,
                "platform": platform,
                "role_id": role_id or user_state.get("role_id", ""),
                "last_match_time": user_state.get("last_match_time", ""),
                "last_room_id": user_state.get("last_room_id", ""),
                "last_item_flow_keys": list(user_state.get("last_item_flow_keys", [])),
                "assets": list(user_state.get("assets", [])),
            })
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
        async with self._lock:
            new_data = copy.deepcopy(self.data)
            groups = new_data["group_origins"]
            if origin in groups:
                return False
            groups.append(origin)
            await self._persist_locked(new_data)
            return True

    async def remove_group(self, origin):
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
            return self._hydrate_user_record(user_data)

    async def get_users(self):
        async with self._lock:
            return {
                sender_id: self._hydrate_user_record(user_data)
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
            user_data.update(copy.deepcopy(fields))
            self._set_user_secrets(user_data, openid=openid, access_token=access_token)
            await self._persist_locked(new_data)
            return True
