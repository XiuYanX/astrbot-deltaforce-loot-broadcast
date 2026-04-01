import asyncio
from datetime import datetime
from pathlib import Path

from astrbot.api import logger
from astrbot.api.message_components import Plain

from ..api.game_api import GameAPI
from ..data.runtime_paths import get_runtime_debug_dir
from ..data.storage import Storage

try:
    from astrbot.api.event import MessageChain
except Exception:
    MessageChain = None

ITEM_FLOW_BASELINE_LIMIT = 200
MAX_PARALLEL_USER_CHECKS = 4
MAX_PARALLEL_BROADCASTS = 4
CHECK_USER_TIMEOUT_SECONDS = 45
MAX_PENDING_BROADCASTS = 20


class RedDetector:
    def __init__(self, storage: Storage, context, api=None):
        self.storage = storage
        self.api = api or GameAPI()
        self._owns_api = api is None
        self.context = context
        self.check_counters = {}
        self.credential_error_users = set()
        self.debug_dir = Path(get_runtime_debug_dir())
        self.max_parallel_user_checks = MAX_PARALLEL_USER_CHECKS
        self.max_parallel_broadcasts = MAX_PARALLEL_BROADCASTS

    async def close(self):
        if self._owns_api:
            await self.api.close()

    def write_debug_file(self, filename, content):
        path = self.debug_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return str(path)

    def get_runtime_debug_dir(self):
        return str(self.debug_dir)

    def clear_user_runtime_state(self, sender_id):
        self.check_counters.pop(str(sender_id), None)
        self.credential_error_users.discard(str(sender_id))

    @staticmethod
    def _normalize_origin(origin):
        return str(origin or "").strip()

    @classmethod
    def _normalize_origins(cls, origins):
        normalized = []
        for origin in origins or []:
            normalized_origin = cls._normalize_origin(origin)
            if normalized_origin and normalized_origin not in normalized:
                normalized.append(normalized_origin)
        return normalized

    @staticmethod
    def _get_flow_window(item_flows, limit=ITEM_FLOW_BASELINE_LIMIT):
        return list(item_flows[:limit])

    @staticmethod
    def _normalize_text_value(value):
        if value is None or isinstance(value, (dict, list, tuple, set)):
            return ""
        text = str(value).strip()
        if not text or text.lower() in {"none", "null", "unknown"}:
            return ""
        return text

    @classmethod
    def _normalize_pending_broadcasts(cls, pending_broadcasts):
        normalized = []
        if not isinstance(pending_broadcasts, list):
            return normalized

        for entry in pending_broadcasts:
            if not isinstance(entry, dict):
                continue
            message = str(entry.get("message", "")).strip()
            origins = cls._normalize_origins(entry.get("origins", []))
            if not message or not origins:
                continue

            normalized_entry = {
                "message": message,
                "origins": origins,
            }
            event_time = cls._normalize_text_value(entry.get("event_time"))
            room_id = cls._normalize_text_value(entry.get("room_id"))
            if event_time:
                normalized_entry["event_time"] = event_time
            if room_id:
                normalized_entry["room_id"] = room_id
            normalized.append(normalized_entry)

        return normalized

    @classmethod
    def _merge_pending_broadcasts(cls, pending_broadcasts, message, failed_groups, match_info=None):
        normalized_pending = cls._normalize_pending_broadcasts(pending_broadcasts)
        failed_origins = cls._normalize_origins(
            item.get("origin", "")
            for item in failed_groups
            if isinstance(item, dict)
        )
        message_text = str(message or "").strip()
        if not message_text or not failed_origins:
            return normalized_pending

        new_entry = {
            "message": message_text,
            "origins": failed_origins,
        }
        if isinstance(match_info, dict):
            event_time = cls._normalize_text_value(
                match_info.get("dtEventTime") or match_info.get("event_time")
            )
            room_id = cls._normalize_text_value(
                match_info.get("roomId") or match_info.get("RoomId")
            )
            if event_time:
                new_entry["event_time"] = event_time
            if room_id:
                new_entry["room_id"] = room_id

        for existing in normalized_pending:
            same_room = new_entry.get("room_id") and new_entry.get("room_id") == existing.get("room_id")
            same_time = new_entry.get("event_time") and new_entry.get("event_time") == existing.get("event_time")
            if existing.get("message") == message_text or (same_room and same_time):
                existing["origins"] = cls._normalize_origins(
                    [*existing.get("origins", []), *failed_origins]
                )
                if new_entry.get("event_time") and not existing.get("event_time"):
                    existing["event_time"] = new_entry["event_time"]
                if new_entry.get("room_id") and not existing.get("room_id"):
                    existing["room_id"] = new_entry["room_id"]
                break
        else:
            normalized_pending.append(new_entry)

        if len(normalized_pending) > MAX_PENDING_BROADCASTS:
            normalized_pending = normalized_pending[-MAX_PENDING_BROADCASTS:]
        return normalized_pending

    @classmethod
    def _deep_find_text(cls, source, exact_keys=None, fuzzy_tokens=None):
        exact_keys = tuple(exact_keys or ())
        fuzzy_tokens = tuple(fuzzy_tokens or ())

        if isinstance(source, dict):
            for key in exact_keys:
                value = cls._normalize_text_value(source.get(key))
                if value:
                    return value

            for key, value in source.items():
                key_text = str(key).lower()
                if fuzzy_tokens and any(token in key_text for token in fuzzy_tokens) and not key_text.endswith("id"):
                    text = cls._normalize_text_value(value)
                    if text:
                        return text
                nested = cls._deep_find_text(value, exact_keys=exact_keys, fuzzy_tokens=fuzzy_tokens)
                if nested:
                    return nested

        elif isinstance(source, list):
            for item in source:
                nested = cls._deep_find_text(item, exact_keys=exact_keys, fuzzy_tokens=fuzzy_tokens)
                if nested:
                    return nested

        return ""

    @classmethod
    def _extract_map_name(cls, *sources):
        exact_keys = (
            "map_name",
            "MapName",
            "mapName",
            "sMapName",
            "Map",
            "map",
            "BattlefieldName",
            "battlefieldName",
            "SceneName",
            "sceneName",
            "PlaceName",
            "placeName",
        )
        fuzzy_tokens = ("map", "scene", "place", "battlefield")
        for source in sources:
            value = cls._deep_find_text(source, exact_keys=exact_keys, fuzzy_tokens=fuzzy_tokens)
            if value:
                return value
        return ""

    @classmethod
    def _extract_role_id(cls, *sources):
        exact_keys = (
            "role_id",
            "roleId",
            "RoleId",
            "sRoleId",
            "charId",
            "CharId",
        )
        for source in sources:
            value = cls._deep_find_text(source, exact_keys=exact_keys)
            if value:
                return value
        return ""

    @staticmethod
    def _format_item_names(detected_items, limit=3):
        names = [
            str(item.get("name", "")).strip()
            for item in detected_items
            if str(item.get("name", "")).strip()
        ]
        if not names:
            return "未知物品"
        if len(names) <= limit:
            return "、".join(names)
        return f"{'、'.join(names[:limit])} 等 {len(names)} 件物品"

    async def ensure_user_role_id(self, sender_id, user_data, match_info=None):
        role_id = str(user_data.get("role_id", "")).strip()
        if not role_id and match_info:
            role_id = self._extract_role_id(match_info)

        if role_id:
            if user_data.get("role_id") != role_id:
                await self.storage.update_user_state(sender_id, role_id=role_id)
                user_data["role_id"] = role_id
            return role_id

        openid = user_data.get("openid")
        access_token = user_data.get("access_token")
        platform = (user_data.get("platform", "qq") or "qq").strip().lower()
        if not openid or not access_token:
            return ""

        bind_res = await self.api.bind_account(access_token, openid, platform)
        role_id = str(bind_res.get("data", {}).get("role_id", "")).strip()
        if role_id:
            await self.storage.update_user_state(sender_id, role_id=role_id)
            user_data["role_id"] = role_id
        return role_id

    async def _enrich_match_info(self, openid, access_token, match_info, platform="qq"):
        if not isinstance(match_info, dict):
            return match_info

        enriched = dict(match_info)
        map_name = self._extract_map_name(enriched)
        role_id = self._extract_role_id(enriched)
        if map_name:
            enriched["map_name"] = map_name
        if role_id:
            enriched["role_id"] = role_id
        if map_name and role_id:
            return enriched

        room_id = self._extract_room_id(enriched)
        if not room_id:
            return enriched

        room_info_result, room_flow_result = await asyncio.gather(
            self.api.fetch_room_info(openid, access_token, room_id, platform=platform),
            self.api.fetch_room_flow(
                openid,
                access_token,
                room_id,
                type_id=1,
                platform=platform,
            ),
            return_exceptions=True,
        )

        room_info = []
        room_flow = None
        if isinstance(room_info_result, Exception):
            logger.warning(
                f"Failed to enrich room info for room {room_id}: "
                f"{type(room_info_result).__name__}: {room_info_result}"
            )
        else:
            room_info = room_info_result

        if isinstance(room_flow_result, Exception):
            logger.warning(
                f"Failed to enrich room flow for room {room_id}: "
                f"{type(room_flow_result).__name__}: {room_flow_result}"
            )
        else:
            room_flow = room_flow_result

        if room_info:
            enriched["room_info"] = room_info
        if room_flow:
            enriched["room_flow"] = room_flow

        map_name = self._extract_map_name(enriched, room_info, room_flow)
        role_id = self._extract_role_id(enriched, room_info, room_flow)
        if map_name:
            enriched["map_name"] = map_name
        if role_id:
            enriched["role_id"] = role_id
        return enriched

    def _build_broadcast_message(self, user_name, detected_items, match_info=None, role_id=""):
        display_name = str(user_name or "").strip() or "未知玩家"
        display_role_id = str(role_id or self._extract_role_id(match_info) or "").strip() or "未知角色ID"
        event_time = "未知时间"
        if isinstance(match_info, dict):
            event_time = match_info.get("dtEventTime") or match_info.get("event_time") or event_time
        map_name = self._extract_map_name(match_info) or "未知地图"
        item_names = self._format_item_names(detected_items)
        return (
            "【天降洪福大红播报】\n"
            f"恭喜本群玩家 {display_name} / {display_role_id} 在 {event_time} 的 {map_name} 中带出了 {item_names}！"
        )

    async def persist_failed_broadcasts(self, sender_id, user_data, broadcast_result, match_info=None):
        current_pending = self._normalize_pending_broadcasts(user_data.get("pending_broadcasts", []))
        updated_pending = self._merge_pending_broadcasts(
            current_pending,
            broadcast_result.get("message", ""),
            broadcast_result.get("failed_groups", []),
            match_info=match_info,
        )
        if updated_pending == current_pending:
            return updated_pending

        await self.storage.update_user_state(sender_id, pending_broadcasts=updated_pending)
        user_data["pending_broadcasts"] = updated_pending
        return updated_pending

    async def retry_pending_broadcasts(self, sender_id, user_data):
        pending_broadcasts = self._normalize_pending_broadcasts(user_data.get("pending_broadcasts", []))
        if not pending_broadcasts:
            return False

        active_groups = set(self._normalize_origins(await self.storage.get_groups()))
        remaining_pending = []
        changed = False

        for entry in pending_broadcasts:
            retry_origins = [origin for origin in entry.get("origins", []) if origin in active_groups]
            if not retry_origins:
                changed = True
                continue

            result = await self.broadcast_message(
                entry["message"],
                origins=retry_origins,
                write_debug_snapshot=False,
                log_prefix="Retrying pending broadcast",
            )
            failed_origins = self._normalize_origins(
                item.get("origin", "")
                for item in result.get("failed_groups", [])
                if isinstance(item, dict)
            )
            if failed_origins:
                if failed_origins != retry_origins:
                    changed = True
                updated_entry = dict(entry)
                updated_entry["origins"] = failed_origins
                remaining_pending.append(updated_entry)
                continue

            changed = True
            logger.info(
                f"Pending broadcast retry completed for user {sender_id} "
                f"({len(retry_origins)} groups)"
            )

        if changed or remaining_pending != pending_broadcasts:
            await self.storage.update_user_state(sender_id, pending_broadcasts=remaining_pending)
            user_data["pending_broadcasts"] = remaining_pending

        return bool(remaining_pending)

    async def _send_message_to_origin(self, origin, msg):
        errors = []

        if MessageChain is not None:
            try:
                chain = MessageChain().message(msg)
                await self.context.send_message(origin, chain)
                return "MessageChain"
            except Exception as exc:
                errors.append(f"MessageChain: {type(exc).__name__}: {exc}")

        try:
            await self.context.send_message(origin, [Plain(msg)])
            return "PlainList"
        except Exception as exc:
            errors.append(f"PlainList: {type(exc).__name__}: {exc}")

        try:
            await self.context.send_message(origin, msg)
            return "RawText"
        except Exception as exc:
            errors.append(f"RawText: {type(exc).__name__}: {exc}")

        raise RuntimeError(" | ".join(errors) if errors else "unknown send error")

    @staticmethod
    def _parse_time(value):
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    @staticmethod
    def _safe_int(value, default=0):
        try:
            return int(float(str(value)))
        except Exception:
            return default

    @staticmethod
    def _is_positive_change(change_value):
        try:
            return float(str(change_value)) > 0
        except Exception:
            return str(change_value).startswith("+")

    def _match_time_window(self, match_time, item_time, seconds=300):
        match_dt = self._parse_time(match_time)
        item_dt = self._parse_time(item_time)
        if not match_dt or not item_dt:
            return False
        return abs((item_dt - match_dt).total_seconds()) <= seconds

    @staticmethod
    def _extract_category_fields(info):
        if not isinstance(info, dict):
            return []
        fields = []
        props_detail = info.get("propsDetail") if isinstance(info.get("propsDetail"), dict) else {}
        for key in [
            "primary",
            "second",
            "type",
            "subType",
            "category",
            "objectType",
            "itemType",
            "primaryClass",
            "secondClass",
            "secondClassCN",
            "thirdClass",
            "thirdClassCN",
        ]:
            value = info.get(key)
            if value is not None:
                fields.append(str(value).lower())
        for key in ["type", "propsSource", "useMap", "usePlace"]:
            value = props_detail.get(key)
            if value is not None:
                fields.append(str(value).lower())
        return fields

    def _is_collection_item(self, info):
        if not isinstance(info, dict):
            return False
        primary_class = str(info.get("primaryClass", "")).lower()
        second_class = str(info.get("secondClass", "")).lower()
        grade = self._safe_int(info.get("grade", 0))
        return primary_class == "props" and second_class == "collection" and grade == 6

    @staticmethod
    def _summarize_flow_buckets(item_flows):
        summary = {
            "撤离带出+": 0,
            "撤离带出-": 0,
            "带入局内+": 0,
            "带入局内-": 0,
            "其他+": 0,
            "其他-": 0,
        }
        for item in item_flows:
            reason = str(item.get("Reason", ""))
            is_positive = RedDetector._is_positive_change(item.get("AddOrReduce", "0"))
            if "撤离带出" in reason:
                summary["撤离带出+" if is_positive else "撤离带出-"] += 1
            elif "带入局内" in reason:
                summary["带入局内+" if is_positive else "带入局内-"] += 1
            else:
                summary["其他+" if is_positive else "其他-"] += 1
        return summary

    @staticmethod
    def _build_legacy_flow_key(item):
        return "|".join(
            [
                str(item.get("dtEventTime", "")),
                str(item.get("iGoodsId", "")),
                str(item.get("AddOrReduce", "")),
                str(item.get("Reason", "")),
            ]
        )

    @classmethod
    def _build_flow_key(cls, item):
        return "|".join(
            [
                cls._build_legacy_flow_key(item),
                str(item.get("Name", "")),
                str(item.get("AfterCount", "")),
            ]
        )

    @classmethod
    def _build_flow_key_variants(cls, item):
        return {
            cls._build_flow_key(item),
            cls._build_legacy_flow_key(item),
        }

    def _collect_match_window_items(self, item_flows, match_time, reason_keyword, positive_change, seconds=1800):
        result = []
        for item in item_flows:
            reason = str(item.get("Reason", ""))
            if reason_keyword not in reason:
                continue
            is_positive = self._is_positive_change(item.get("AddOrReduce", "0"))
            if is_positive != positive_change:
                continue
            if match_time and not self._match_time_window(match_time, item.get("dtEventTime", ""), seconds=seconds):
                continue
            result.append(item)
        return result

    async def broadcast(self, user_name, detected_items, match_info=None, role_id=""):
        message = self._build_broadcast_message(
            user_name,
            detected_items,
            match_info,
            role_id=role_id,
        )
        return await self.broadcast_message(
            message,
            write_debug_snapshot=True,
            log_prefix="Triggered collection broadcast",
        )

    def _collect_reason_items(self, item_flows, reason_keyword, positive_change):
        result = []
        for item in item_flows:
            reason = str(item.get("Reason", ""))
            if reason_keyword not in reason:
                continue
            is_positive = self._is_positive_change(item.get("AddOrReduce", "0"))
            if is_positive != positive_change:
                continue
            result.append(item)
        return result

    def _extract_room_id(self, match):
        if not isinstance(match, dict):
            return ""
        return str(match.get("roomId") or match.get("RoomId") or "")

    async def _get_item_catalog_map(self, openid, access_token, platform="qq"):
        items = await self.api.fetch_item_catalog(openid, access_token, platform=platform)
        info_map = {}
        for info in items:
            if not isinstance(info, dict):
                continue
            key = str(info.get("objectID") or info.get("item_id") or info.get("id") or "")
            if key:
                info_map[key] = info
        return info_map

    async def build_debug_report(self, openid, access_token, platform="qq"):
        logs_v2 = await self.api.fetch_records_v2(openid, access_token, type_id=4, platform=platform)
        latest_match = logs_v2[0] if logs_v2 else None
        if not latest_match:
            logs = await self.api.fetch_records(openid, access_token, type_id=4, platform=platform)
            latest_match = logs[0] if logs else None
        if not latest_match:
            return {"error": "未获取到最新战绩"}

        current_match_time = latest_match.get("dtEventTime", "")
        current_room_id = self._extract_room_id(latest_match)
        item_flows = await self.api.fetch_all_item_flows(openid, access_token, platform=platform)

        all_carry_out_items = self._collect_reason_items(
            item_flows,
            reason_keyword="撤离带出",
            positive_change=True,
        )
        all_carry_in_items = self._collect_reason_items(
            item_flows,
            reason_keyword="带入局内",
            positive_change=True,
        )
        carry_out_items = self._collect_match_window_items(
            item_flows,
            current_match_time,
            reason_keyword="撤离带出",
            positive_change=True,
            seconds=420,
        )

        info_map = await self._get_item_catalog_map(openid, access_token, platform=platform)
        collection_candidates = []
        for item in carry_out_items:
            item_id = str(item.get("iGoodsId", ""))
            info = info_map.get(item_id, {})
            collection_candidates.append(
                {
                    "id": item_id,
                    "name": item.get("Name", ""),
                    "time": item.get("dtEventTime", ""),
                    "change": item.get("AddOrReduce", ""),
                    "reason": item.get("Reason", ""),
                    "is_collection": self._is_collection_item(info),
                    "grade": self._safe_int(info.get("grade", 0)),
                    "category_fields": self._extract_category_fields(info),
                }
            )

        return {
            "match": {
                "room_id": current_room_id,
                "event_time": current_match_time,
                "final_price": latest_match.get("FinalPrice", "0"),
                "escape_reason": latest_match.get("EscapeFailReason", ""),
            },
            "flow_summary": self._summarize_flow_buckets(item_flows),
            "total_item_flows": len(item_flows),
            "all_carry_out_items": all_carry_out_items,
            "all_carry_in_items": all_carry_in_items,
            "carry_out_items": carry_out_items,
            "collection_candidates": collection_candidates,
        }

    async def build_latest_broadcast_payload(self, openid, access_token, platform="qq"):
        logs_v2 = await self.api.fetch_records_v2(openid, access_token, type_id=4, platform=platform)
        latest_match = logs_v2[0] if logs_v2 else None
        if not latest_match:
            logs = await self.api.fetch_records(openid, access_token, type_id=4, platform=platform)
            latest_match = logs[0] if logs else None
        if not latest_match:
            return {"error": "未获取到最近一局战绩"}

        current_match_time = latest_match.get("dtEventTime", "")
        item_flows = await self.api.fetch_all_item_flows(openid, access_token, platform=platform)
        if not item_flows:
            return {"error": "未获取到道具流水"}

        carry_out_items = self._collect_match_window_items(
            item_flows,
            current_match_time,
            reason_keyword="撤离带出",
            positive_change=True,
            seconds=420,
        )

        info_map = await self._get_item_catalog_map(openid, access_token, platform=platform)
        detected_items = []
        for item in carry_out_items:
            item_id = str(item.get("iGoodsId", ""))
            info = info_map.get(item_id, {})
            if not self._is_collection_item(info):
                continue
            detected_items.append(
                {
                    "id": item_id,
                    "name": item.get("Name") or info.get("name") or f"未知物品({item_id})",
                    "time": item.get("dtEventTime", ""),
                    "change": item.get("AddOrReduce", "+0"),
                    "reason": item.get("Reason", ""),
                }
            )

        detected_items.sort(key=lambda item: item.get("name", ""))
        latest_match = await self._enrich_match_info(openid, access_token, latest_match, platform=platform)
        return {
            "match_info": latest_match,
            "detected_items": detected_items,
        }

    async def check_all_users(self):
        users = await self.storage.get_users()
        if not users:
            self.check_counters.clear()
            return

        active_sender_ids = {str(sender_id) for sender_id in users}
        for sender_id in list(self.check_counters):
            if sender_id not in active_sender_ids:
                self.check_counters.pop(sender_id, None)

        semaphore = asyncio.Semaphore(self.max_parallel_user_checks)

        async def run_check(sender_id, user_data):
            async with semaphore:
                await self.check_user(sender_id, user_data)

        sender_entries = [
            (str(sender_id), user_data)
            for sender_id, user_data in users.items()
        ]
        outcomes = await asyncio.gather(
            *(run_check(sender_id, user_data) for sender_id, user_data in sender_entries),
            return_exceptions=True,
        )
        for (sender_id, _), outcome in zip(sender_entries, outcomes):
            if isinstance(outcome, asyncio.CancelledError):
                logger.warning(f"User check for {sender_id} was cancelled before completion.")
            elif isinstance(outcome, Exception):
                logger.error(
                    f"Unexpected failure escaped user check for {sender_id}: "
                    f"{type(outcome).__name__}: {outcome}"
                )

    async def check_user(self, sender_id, user_data):
        sender_id = str(sender_id)
        try:
            await asyncio.wait_for(
                self._check_user_impl(sender_id, user_data),
                timeout=CHECK_USER_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"Timed out while checking user {sender_id} after "
                f"{CHECK_USER_TIMEOUT_SECONDS}s"
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"Failed to check user {sender_id}: {type(exc).__name__}: {exc}")

    async def _check_user_impl(self, sender_id, user_data):
        if self._normalize_pending_broadcasts(user_data.get("pending_broadcasts", [])):
            await self.retry_pending_broadcasts(sender_id, user_data)

        if user_data.get("_secret_errors"):
            if sender_id not in self.credential_error_users:
                logger.warning(
                    f"Skipping user {sender_id} because stored credentials could not be decrypted."
                )
                self.credential_error_users.add(sender_id)
            return
        self.credential_error_users.discard(sender_id)

        openid = user_data.get("openid")
        access_token = user_data.get("access_token")
        platform = (user_data.get("platform", "qq") or "qq").strip().lower()
        if not openid or not access_token:
            return

        counter = self.check_counters.get(sender_id, 0)
        self.check_counters[sender_id] = counter + 1

        latest_match = None
        logs_v2 = await self.api.fetch_records_v2(
            openid,
            access_token,
            type_id=4,
            platform=platform,
        )
        if logs_v2 and isinstance(logs_v2, list):
            latest_match = logs_v2[0]
        if not latest_match:
            logs = await self.api.fetch_records(
                openid,
                access_token,
                type_id=4,
                platform=platform,
            )
            if logs and isinstance(logs, list):
                latest_match = logs[0]

        if not latest_match:
            return

        current_match_time = latest_match.get("dtEventTime", "")
        current_room_id = self._extract_room_id(latest_match)
        last_match_time = user_data.get("last_match_time", "")
        last_room_id = str(user_data.get("last_room_id", "") or "")
        last_item_flow_keys = set(user_data.get("last_item_flow_keys", []))

        match_updated = False
        if current_room_id:
            match_updated = current_room_id != last_room_id
        elif current_match_time:
            match_updated = current_match_time != last_match_time

        should_check_flow = match_updated or counter % 15 == 0 or not last_item_flow_keys
        if not should_check_flow:
            return

        item_flows = await self.api.fetch_all_item_flows(
            openid,
            access_token,
            platform=platform,
        )
        if not item_flows:
            return

        flow_window = self._get_flow_window(item_flows)
        current_flow_keys = list(
            dict.fromkeys(self._build_flow_key(item) for item in flow_window)
        )

        if not last_item_flow_keys:
            await self.storage.update_user_state(
                sender_id,
                last_item_flow_keys=current_flow_keys,
                last_match_time=current_match_time,
                last_room_id=current_room_id,
            )
            logger.info(
                f"玩家 {sender_id} 首次加载道具流水基线完成 "
                f"({len(current_flow_keys)} 项)"
            )
            return

        new_flow_items = [
            item
            for item in flow_window
            if last_item_flow_keys.isdisjoint(self._build_flow_key_variants(item))
        ]
        carry_out_items = self._collect_match_window_items(
            new_flow_items,
            current_match_time,
            reason_keyword="撤离带出",
            positive_change=True,
            seconds=420,
        )

        info_map = await self._get_item_catalog_map(
            openid,
            access_token,
            platform=platform,
        )
        detected_items = []
        for item in carry_out_items:
            item_id = str(item.get("iGoodsId", ""))
            info = info_map.get(item_id, {})
            display_name = item.get("Name") or info.get("name") or f"未知物品({item_id})"
            if not self._is_collection_item(info):
                continue
            detected_items.append(
                {
                    "id": item_id,
                    "name": display_name,
                    "time": item.get("dtEventTime", ""),
                    "change": item.get("AddOrReduce", "+0"),
                    "reason": item.get("Reason", ""),
                }
            )

        detected_items.sort(key=lambda item: item.get("name", ""))
        update_fields = {
            "last_item_flow_keys": current_flow_keys,
            "last_match_time": current_match_time,
            "last_room_id": current_room_id,
        }
        await self.storage.update_user_state(
            sender_id,
            **update_fields,
        )
        user_data.update(update_fields)
        if detected_items:
            latest_match = await self._enrich_match_info(
                openid,
                access_token,
                latest_match,
                platform=platform,
            )
            role_id = await self.ensure_user_role_id(sender_id, user_data, match_info=latest_match)
            broadcast_result = await self.broadcast(
                user_data.get("name", sender_id),
                detected_items,
                latest_match,
                role_id=role_id,
            )
            if not broadcast_result.get("success_groups"):
                failed_origins = ", ".join(
                    item.get("origin", "")
                    for item in broadcast_result.get("failed_groups", [])
                    if item.get("origin", "")
                )
                logger.warning(
                    f"User {sender_id} detected collection items but all broadcasts failed. "
                    f"Targets: {failed_origins or 'unknown'}"
                )
            if broadcast_result.get("failed_groups"):
                pending_broadcasts = self._merge_pending_broadcasts(
                    user_data.get("pending_broadcasts", []),
                    broadcast_result.get("message", ""),
                    broadcast_result.get("failed_groups", []),
                    match_info=latest_match,
                )
                try:
                    await self.storage.update_user_state(
                        sender_id,
                        pending_broadcasts=pending_broadcasts,
                    )
                except OSError as exc:
                    logger.error(
                        f"Failed to persist pending broadcast retries for user {sender_id}: "
                        f"{type(exc).__name__}: {exc}"
                    )
                else:
                    user_data["pending_broadcasts"] = pending_broadcasts
                    logger.warning(
                        f"Queued retry for {len(broadcast_result.get('failed_groups', []))} "
                        f"failed broadcast targets for user {sender_id}."
                    )

    async def broadcast_message(self, message, origins=None, *, write_debug_snapshot=False, log_prefix="Broadcasting message"):
        if origins is None:
            groups = await self.storage.get_groups()
        else:
            groups = origins
        groups = self._normalize_origins(groups)
        result = {
            "message": message,
            "total_groups": len(groups),
            "success_groups": [],
            "failed_groups": [],
        }
        msg = message

        logger.info(f"{log_prefix}:\n{msg}")
        if write_debug_snapshot:
            try:
                self.write_debug_file("debug_last_broadcast.txt", msg)
            except Exception as exc:
                logger.error(f"Failed to write latest broadcast snapshot: {exc}")

        if not groups:
            return result

        semaphore = asyncio.Semaphore(self.max_parallel_broadcasts)

        async def send_to_group(origin):
            async with semaphore:
                try:
                    send_mode = await self._send_message_to_origin(origin, msg)
                    return True, {
                        "origin": origin,
                        "mode": send_mode,
                    }
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    error_message = str(exc)
                    logger.error(f"Broadcast to group {origin} failed: {error_message}")
                    return False, {
                        "origin": origin,
                        "error": "发送失败，请查看日志。",
                    }

        outcomes = await asyncio.gather(*(send_to_group(origin) for origin in groups))
        for success, payload in outcomes:
            if success:
                result["success_groups"].append(payload)
            else:
                result["failed_groups"].append(payload)

        logger.info(
            f"{log_prefix} result: success {len(result['success_groups'])}/{len(groups)}, "
            f"failed {len(result['failed_groups'])}/{len(groups)}"
        )
        return result
