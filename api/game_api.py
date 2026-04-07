import asyncio
import base64
import json
import os
import re
import tempfile
import time
import uuid
from http.cookies import SimpleCookie
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin, urlparse

import aiohttp
from astrbot.api import logger

from ..data.runtime_paths import get_runtime_file_path

APPID = 101491592
BASE_URL = "https://comm.ams.game.qq.com/ide/"
GAME_API_URL = "https://comm.aci.game.qq.com/main"
LOGIN_APP_ID = 716027609
QQ_CONNECT_AUTHORIZE_URL = "https://graph.qq.com/oauth2.0/authorize"
QQ_CONNECT_REDIRECT_URI = "https://milo.qq.com/comm-htdocs/login/qc_redirect.html"
QQ_CONNECT_SCOPE = "get_user_info"
QQ_CONNECT_STATE = "STATE"
QQ_LOGIN_S_URL = "https://graph.qq.com/oauth2.0/login_jump"
QQ_QR_SHOW_URL = "https://xui.ptlogin2.qq.com/ssl/ptqrshow"
QQ_LOGIN_TICKET_URL = "https://xui.ptlogin2.qq.com/cgi-bin/xlogin"
QQ_LOGIN_STATUS_URL = "https://ssl.ptlogin2.qq.com/ptqrlogin"
QQ_LEGACY_AUTHORIZE_UI = "979D48F3-6CE2-4E95-A789-3BD3187648B6"
QQ_AUTHORIZE_SHOW_FLOW_ENABLED = False
WECHAT_QR_URL = "https://open.weixin.qq.com/connect/qrconnect"
WECHAT_QR_STATUS_URL = "https://lp.open.weixin.qq.com/connect/l/qrconnect"
OBJECT_LIST_PARAMS = {
    "iChartId": "316969",
    "iSubChartId": "316969",
    "sIdeToken": "NoOapI",
    "method": "dfm/object.list",
    "source": "2",
}
REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=20,
    connect=5,
    sock_connect=5,
    sock_read=15,
)
REQUEST_EXCEPTIONS = (
    aiohttp.ClientError,
    asyncio.TimeoutError,
    aiohttp.ContentTypeError,
    json.JSONDecodeError,
)
REDIRECT_STATUS_CODES = frozenset({301, 302, 303, 307, 308})
ALLOWED_REDIRECT_HOSTS = frozenset(
    {
        "graph.qq.com",
        "milo.qq.com",
        "ptlogin2.graph.qq.com",
        "ssl.ptlogin2.graph.qq.com",
        "ssl.ptlogin2.qq.com",
        "xui.ptlogin2.qq.com",
    }
)
ITEM_CATALOG_CACHE_TTL_SECONDS = 12 * 60 * 60
DEFAULT_QQ_LOGIN_CONFIG = {
    "appid": str(LOGIN_APP_ID),
    "s_url": QQ_LOGIN_S_URL,
    "href": "",
    "login_sig": "",
    "ptui_version": "",
    "lang": "2052",
    "style": "40",
    "pt_3rd_aid": "0",
    "daid": "",
    "target": "1",
}

class GameAPI:
    def __init__(self, platform="qq", timeout=REQUEST_TIMEOUT):
        self.platform = platform
        self.timeout = timeout
        self._session = None
        self._session_lock = asyncio.Lock()

    @staticmethod
    def _create_cookie_jar():
        cookie_jar_cls = getattr(aiohttp, "DummyCookieJar", None)
        if cookie_jar_cls is not None:
            return cookie_jar_cls()
        return aiohttp.CookieJar()

    async def close(self):
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
            self._session = None

    async def _get_session(self):
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    timeout=self.timeout,
                    # Requests pass cookies explicitly so multiple bound accounts
                    # do not share implicit session state through one CookieJar.
                    cookie_jar=self._create_cookie_jar(),
                )
            return self._session

    @staticmethod
    def _get_item_catalog_cache_path():
        return get_runtime_file_path("item_catalog_cache.json")

    @staticmethod
    def _safe_json_loads(value, default=None):
        if default is None:
            default = {}
        try:
            return json.loads(value)
        except Exception:
            return default

    @staticmethod
    def _get_headers():
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://df.qq.com/",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    @staticmethod
    def _normalize_message_text(value):
        if value is None or isinstance(value, (dict, list, tuple, set)):
            return ""
        text = str(value).strip()
        if not text or text.lower() in {"none", "null", "unknown"}:
            return ""
        return text

    @staticmethod
    def _decode_js_string_literal(value):
        text = str(value or "")
        if not text:
            return ""
        try:
            return bytes(text, "utf-8").decode("unicode_escape")
        except Exception:
            return text

    @classmethod
    def _extract_qq_login_config_from_xlogin_page(cls, payload):
        payload_text = str(payload or "")
        config = dict(DEFAULT_QQ_LOGIN_CONFIG)
        if not payload_text:
            return config

        pattern_map = {
            "s_url": r's_url:"([^"]*)"',
            "href": r'href:"([^"]*)"',
            "login_sig": r'login_sig:"([^"]*)"',
            "ptui_version": r'ptui_version:encodeURIComponent\("([^"]*)"\)',
            "lang": r'lang:encodeURIComponent\("([^"]*)"\)',
            "style": r'style:encodeURIComponent\("([^"]*)"\)',
            "pt_3rd_aid": r'pt_3rd_aid:encodeURIComponent\("([^"]*)"\)',
            "appid": r'appid:encodeURIComponent\("([^"]*)"\)',
            "daid": r'daid:encodeURIComponent\("([^"]*)"\)',
            "target": r'target:isNaN\(parseInt\("([^"]*)"\)\)',
        }
        for key, pattern in pattern_map.items():
            match = re.search(pattern, payload_text)
            if match:
                config[key] = cls._decode_js_string_literal(match.group(1))

        return config

    @classmethod
    def _normalize_qq_login_config(cls, config=None):
        normalized = dict(DEFAULT_QQ_LOGIN_CONFIG)
        if isinstance(config, dict):
            for key in normalized:
                value = cls._normalize_message_text(config.get(key))
                if value:
                    normalized[key] = value
        return normalized

    @classmethod
    def _build_qq_login_headers(cls, login_config=None):
        headers = cls._get_headers()
        normalized = cls._normalize_qq_login_config(login_config)
        headers["Referer"] = normalized.get("href") or QQ_LOGIN_TICKET_URL
        return headers

    @staticmethod
    def _build_qq_connect_authorize_params():
        return {
            "response_type": "code",
            "client_id": str(APPID),
            "redirect_uri": QQ_CONNECT_REDIRECT_URI,
            "scope": QQ_CONNECT_SCOPE,
            "state": QQ_CONNECT_STATE,
            "display": "pc",
        }

    @classmethod
    def _build_qq_connect_authorize_show_params(cls):
        params = cls._build_qq_connect_authorize_params()
        params.update(
            {
                "src": "1",
                "auth_time": str(int(time.time() * 1000)),
                "which": "Login",
            }
        )
        params["redirect_uri"] = (
            f"{QQ_CONNECT_REDIRECT_URI}?parent_domain=https://df.qq.com&isMiloSDK=1&isPc=1"
        )
        return params

    @classmethod
    def _extract_qq_connect_xlogin_url_from_authorize_page(cls, payload):
        payload_text = str(payload or "")
        if not payload_text:
            return ""

        inner_login_jump = QQ_LOGIN_S_URL
        login_jump_match = re.search(r"var s_url = '([^']+login_jump[^']*)';", payload_text)
        if login_jump_match:
            inner_login_jump = cls._decode_js_string_literal(login_jump_match.group(1))

        xlogin_prefix_match = re.search(
            r"s_url\s*=\s*'(https://xui\.ptlogin2\.qq\.com/cgi-bin/xlogin\?[^']*s_url=)'\s*\+\s*encodeURIComponent\(s_url\)",
            payload_text,
        )
        if not xlogin_prefix_match:
            return ""

        xlogin_url = (
            cls._decode_js_string_literal(xlogin_prefix_match.group(1))
            + quote(inner_login_jump, safe="")
        )

        if "pt_3rd_aid=" not in xlogin_url:
            xlogin_url += f"&pt_3rd_aid={quote(str(APPID), safe='')}"

        return xlogin_url

    @classmethod
    def _extract_qq_connect_authorize_need_login(cls, payload):
        payload_text = str(payload or "")
        if not payload_text:
            return None

        match = re.search(
            r"Q\.isNeedLogin\s*=\s*(true|false)\s*;",
            payload_text,
            re.IGNORECASE,
        )
        if not match:
            return None
        return match.group(1).lower() == "true"

    @classmethod
    def _merge_qq_login_config_from_url(cls, config, url):
        merged = dict(config or {})
        parsed_url = urlparse(str(url or ""))
        params = parse_qs(parsed_url.query)
        field_map = {
            "appid": "appid",
            "s_url": "s_url",
            "style": "style",
            "pt_3rd_aid": "pt_3rd_aid",
            "daid": "daid",
            "target": "target",
        }
        for key, param_name in field_map.items():
            value = cls._normalize_message_text((params.get(param_name) or [""])[0])
            if value:
                if (
                    key == "target"
                    and str(merged.get("target", "")).strip()
                    and value.lower() == "self"
                ):
                    continue
                merged[key] = value
        if str(url or "").strip():
            merged["href"] = str(url)
        return merged

    @staticmethod
    def _parse_authorize_payload(payload):
        payload_text = str(payload or "").strip()
        if not payload_text:
            return {}
        try:
            parsed = json.loads(payload_text)
        except Exception:
            parsed = None
        return parsed if isinstance(parsed, dict) else {}

    @classmethod
    def _extract_authorize_callback_url(cls, response, payload):
        response_snapshot = response if isinstance(response, dict) else {}
        payload_dict = cls._parse_authorize_payload(payload)
        callback = cls._normalize_message_text(payload_dict.get("callback"))
        if callback:
            return callback, payload_dict
        location = cls._normalize_message_text(response_snapshot.get("headers", {}).get("Location", ""))
        return location, payload_dict

    @staticmethod
    def _is_qq_authorize_show_url(url):
        parsed = urlparse(str(url or ""))
        return (
            parsed.scheme == "https"
            and (parsed.hostname or "").lower() == "graph.qq.com"
            and parsed.path == "/oauth2.0/show"
        )

    @classmethod
    def _build_legacy_qq_authorize_headers(cls, login_config, cookies):
        headers = cls._build_qq_login_headers(login_config)
        headers["Referer"] = (
            cls._normalize_message_text((login_config or {}).get("href"))
            or "https://xui.ptlogin2.qq.com/"
        )
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers["X-G-TK"] = str(cls._get_gtk((cookies or {}).get("p_skey", "")))
        headers.pop("Origin", None)
        return headers

    @classmethod
    def _build_legacy_qq_authorize_form_data(cls, cookies):
        return {
            "response_type": "code",
            "client_id": str(APPID),
            "redirect_uri": (
                f"{QQ_CONNECT_REDIRECT_URI}?parent_domain=https://df.qq.com"
                "&isMiloSDK=1&isPc=1"
            ),
            "scope": "",
            "state": QQ_CONNECT_STATE,
            "switch": "",
            "form_plogin": 1,
            "src": 1,
            "update_auth": 1,
            "openapi": 1010,
            "g_tk": cls._get_gtk((cookies or {}).get("p_skey", "")),
            "auth_time": int(time.time()),
            "ui": QQ_LEGACY_AUTHORIZE_UI,
        }

    async def _perform_qq_authorize_exchange(self, cookies, headers, form_data):
        try:
            response, response_text = await self._request_text(
                "POST",
                QQ_CONNECT_AUTHORIZE_URL,
                data=form_data,
                headers=headers,
                cookies=cookies,
                allow_redirects=False,
                error_context="Failed to start QQ access token exchange",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "获取access token失败", "data": {}}

        location, payload_dict = self._extract_authorize_callback_url(response, response_text)
        auth_code = self._extract_query_param(location, "code")
        if "ret" in payload_dict and str(payload_dict.get("ret")) != "0":
            message = self._extract_response_message(payload_dict) or "获取access token失败"
            return {"status": False, "message": message, "data": {}}

        merged_cookies = self._merge_cookies(cookies, response["cookies"])
        if location and not self._is_allowed_redirect_target(location):
            logger.warning(
                "Rejected unexpected QQ authorize redirect target while exchanging access token: "
                f"{location}"
            )
            return {"status": False, "message": "获取access token失败", "data": {}}

        redirect_response = None
        if location:
            try:
                redirect_response, _ = await self._request_get_with_allowed_redirects(
                    location,
                    headers=headers,
                    cookies=merged_cookies,
                    error_context="Failed to complete QQ authorize redirect",
                )
            except REQUEST_EXCEPTIONS:
                return {"status": False, "message": "获取access token失败", "data": {}}
            merged_cookies = self._merge_cookies(merged_cookies, redirect_response["cookies"])
            auth_code = (
                auth_code
                or self._extract_query_param(redirect_response.get("url", ""), "code")
                or self._extract_query_param(
                    redirect_response.get("headers", {}).get("Location", ""),
                    "code",
                )
            )

        return {
            "status": True,
            "message": "获取成功",
            "data": {
                "auth_code": auth_code,
                "location": location,
                "response": response,
                "redirect_response": redirect_response,
                "cookies": merged_cookies,
            },
        }

    @classmethod
    def _extract_response_message(cls, payload):
        if not isinstance(payload, dict):
            return ""

        candidates = [payload]
        seen = set()
        while candidates:
            current = candidates.pop(0)
            current_id = id(current)
            if current_id in seen:
                continue
            seen.add(current_id)

            for key in ("message", "msg", "sMsg", "errMsg", "errmsg", "retMsg"):
                message = cls._normalize_message_text(current.get(key))
                if message:
                    return message

            for key in ("jData", "data", "bindarea"):
                value = current.get(key)
                if isinstance(value, dict):
                    candidates.append(value)

        return ""

    @staticmethod
    def _is_credential_expired_message(message):
        message_text = str(message or "").strip()
        if not message_text:
            return False
        invalid_tokens = (
            "鉴权",
            "过期",
            "重新扫码登录",
            "cookie无效",
            "cookie过期",
            "登录失效",
        )
        lowered_message = message_text.lower()
        return any(token in message_text for token in invalid_tokens) or any(
            token in lowered_message for token in ("cookie invalid", "cookie expired")
        )

    @staticmethod
    def _get_gtk(p_skey, h=5381):
        for c in str(p_skey or ""):
            h += (h << 5) + ord(c)
        return h & 0x7FFFFFFF

    @staticmethod
    def _get_micro_time():
        return int(time.time() * 1000000)

    @staticmethod
    def _get_cookies(openid, access_token, platform="qq"):
        return {
            "openid": openid,
            "access_token": access_token,
            "acctype": "qc" if platform == "qq" else "wx",
            "appid": str(APPID),
        }

    def create_cookie(self, openid, access_token, platform=None):
        return self._get_cookies(openid, access_token, platform or self.platform)

    @staticmethod
    def _parse_cookies(cookie):
        if isinstance(cookie, dict):
            return {str(k): str(v) for k, v in cookie.items() if v not in ("", None)}
        if isinstance(cookie, str):
            cookie_text = cookie.strip()
            if not cookie_text:
                return {}
            try:
                parsed = json.loads(cookie_text)
            except Exception:
                parsed = None
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items() if v not in ("", None)}
            if isinstance(parsed, str) and parsed != cookie_text:
                return GameAPI._parse_cookies(parsed)
            header_cookies = {}
            cookie_attributes = {
                "comment",
                "domain",
                "expires",
                "httponly",
                "max-age",
                "path",
                "samesite",
                "secure",
                "version",
            }
            for part in cookie_text.split(";"):
                key, separator, value = part.partition("=")
                key = key.strip()
                if not separator or not key or key.lower() in cookie_attributes:
                    continue
                header_cookies[key] = value.strip().strip('"')
            simple_cookie = SimpleCookie()
            try:
                simple_cookie.load(cookie_text)
            except Exception:
                simple_cookie = None
            if simple_cookie is not None:
                for key, morsel in simple_cookie.items():
                    if morsel.value not in ("", None):
                        header_cookies[str(key)] = str(morsel.value)
            return header_cookies
        return {}

    @staticmethod
    def _collect_response_cookies(response):
        cookies = {}
        for history_response in [*response.history, response]:
            for key, morsel in history_response.cookies.items():
                cookies[str(key)] = str(morsel.value)
        return cookies

    @classmethod
    def _merge_cookies(cls, *cookie_sources):
        merged = {}
        for source in cookie_sources:
            merged.update(cls._parse_cookies(source))
        return merged

    @classmethod
    def _snapshot_response(cls, response, *, session=None):
        snapshot = {
            "status": response.status,
            "url": str(response.url),
            "headers": dict(response.headers),
            "cookies": cls._collect_response_cookies(response),
        }
        if session is not None:
            try:
                session_cookies = session.cookie_jar.filter_cookies(response.url)
                for key, morsel in session_cookies.items():
                    snapshot["cookies"][str(key)] = str(morsel.value)
            except Exception:
                pass
        return snapshot

    @classmethod
    def _decode_response_bytes(cls, response, body):
        if body is None:
            return ""
        if isinstance(body, str):
            return body
        if not isinstance(body, (bytes, bytearray)):
            return str(body)

        candidate_encodings = []
        charset = cls._normalize_message_text(getattr(response, "charset", ""))
        if charset:
            candidate_encodings.append(charset)

        content_type = ""
        headers = getattr(response, "headers", None)
        if headers:
            content_type = cls._normalize_message_text(headers.get("Content-Type", ""))
        charset_match = re.search(r"charset=([\w-]+)", content_type, re.IGNORECASE)
        if charset_match:
            candidate_encodings.append(charset_match.group(1))

        for encoding in ("utf-8", "utf-8-sig", "gb18030", "gbk", "big5", "latin1"):
            if encoding not in candidate_encodings:
                candidate_encodings.append(encoding)

        for encoding in candidate_encodings:
            try:
                return bytes(body).decode(encoding)
            except (UnicodeDecodeError, LookupError):
                continue

        return bytes(body).decode("utf-8", errors="replace")

    @staticmethod
    def _is_allowed_redirect_target(url):
        parsed = urlparse(str(url or ""))
        hostname = (parsed.hostname or "").lower()
        return parsed.scheme == "https" and hostname in ALLOWED_REDIRECT_HOSTS

    @staticmethod
    def _extract_query_param(url, name):
        if not url:
            return ""
        try:
            values = parse_qs(urlparse(url).query, keep_blank_values=True).get(name, [])
        except ValueError:
            return ""
        return values[0] if values else ""

    @staticmethod
    def _resolve_redirect_url(base_url, location):
        location_text = str(location or "").strip()
        if not location_text:
            return ""
        return urljoin(str(base_url or ""), location_text)

    @staticmethod
    def _restrict_file_permissions(path):
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass

    @classmethod
    def _write_cache_atomic(cls, path, payload):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_fd, temp_path = tempfile.mkstemp(
            dir=str(path.parent),
            prefix=".item_catalog_",
            suffix=".tmp",
        )
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, indent=2)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp_path, path)
            cls._restrict_file_permissions(path)
        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

    async def _request_text(self, method, url, *, error_context, **kwargs):
        session = await self._get_session()
        try:
            async with session.request(method, url, **kwargs) as response:
                body = await response.read()
                decoded_text = self._decode_response_bytes(response, body)
                return self._snapshot_response(response, session=session), decoded_text
        except REQUEST_EXCEPTIONS as exc:
            logger.warning(f"{error_context}: {type(exc).__name__}: {exc}")
            raise

    async def _request_get_with_allowed_redirects(
        self,
        url,
        *,
        error_context,
        cookies=None,
        max_redirects=5,
        **kwargs,
    ):
        current_url = str(url or "")
        current_cookies = self._parse_cookies(cookies)
        response_snapshot = None
        response_text = ""

        for _ in range(max_redirects + 1):
            response_snapshot, response_text = await self._request_text(
                "GET",
                current_url,
                headers=kwargs.get("headers"),
                params=kwargs.get("params"),
                cookies=current_cookies,
                allow_redirects=False,
                error_context=error_context,
            )
            current_cookies = self._merge_cookies(current_cookies, response_snapshot["cookies"])
            location = response_snapshot["headers"].get("Location", "")
            if response_snapshot["status"] not in REDIRECT_STATUS_CODES or not location:
                response_snapshot["cookies"] = current_cookies
                return response_snapshot, response_text

            next_url = self._resolve_redirect_url(current_url, location)
            if not self._is_allowed_redirect_target(next_url):
                logger.warning(
                    "Rejected unexpected redirect target while following QQ login redirect chain: "
                    f"{next_url}"
                )
                raise aiohttp.ClientError(f"Blocked redirect target: {next_url}")
            current_url = next_url

        raise aiohttp.ClientError(
            f"Exceeded {max_redirects} redirects while requesting {url}"
        )

    async def _request_json(self, method, url, *, error_context, **kwargs):
        session = await self._get_session()
        try:
            async with session.request(method, url, **kwargs) as response:
                return self._snapshot_response(response, session=session), await response.json(content_type=None)
        except REQUEST_EXCEPTIONS as exc:
            logger.warning(f"{error_context}: {type(exc).__name__}: {exc}")
            raise

    async def _request_bytes(self, method, url, *, error_context, **kwargs):
        session = await self._get_session()
        try:
            async with session.request(method, url, **kwargs) as response:
                return self._snapshot_response(response, session=session), await response.read()
        except REQUEST_EXCEPTIONS as exc:
            logger.warning(f"{error_context}: {type(exc).__name__}: {exc}")
            raise

    async def _post_base_json(self, data, *, cookies=None, error_context):
        return await self._request_json(
            "POST",
            BASE_URL,
            data=data,
            headers=self._get_headers(),
            cookies=cookies,
            error_context=error_context,
        )

    async def _fetch_role_profile(self, access_token, openid, access_type):
        params = {
            "needGopenid": 1,
            "sAMSAcctype": access_type,
            "sAMSAccessToken": access_token,
            "sAMSAppOpenId": openid,
            "sAMSSourceAppId": LOGIN_APP_ID,
            "game": "dfm",
            "sCloudApiName": "ams.gameattr.role",
            "area": 36,
            "platid": 1,
            "partition": 36,
        }
        headers = {"referer": "https://df.qq.com/"}
        try:
            _, result = await self._request_text(
                "GET",
                GAME_API_URL,
                params=params,
                headers=headers,
                error_context="Failed to fetch role profile",
            )
        except REQUEST_EXCEPTIONS:
            return {}

        pattern = r"\{([^}]*)\}"
        matches = re.search(pattern, result)
        if not matches:
            return {}

        pairs_pattern = r"(\w+):('[^']*'|-?\d+|[^,]*)"
        pairs = re.findall(pairs_pattern, matches.group(1))
        role_data = {}
        for key, value in pairs:
            value = value.strip("'")
            if key == "msg":
                try:
                    role_data[key] = value.encode("latin1").decode("gbk")
                except (UnicodeDecodeError, LookupError):
                    role_data[key] = value
            else:
                role_data[key] = value

        checkparam = role_data.get("checkparam", "")
        checkparam_parts = checkparam.split("|")
        if len(checkparam_parts) >= 3:
            role_data["role_id"] = checkparam_parts[2]
        return role_data

    @staticmethod
    def _load_item_catalog_cache():
        cache_path = Path(GameAPI._get_item_catalog_cache_path())
        if not cache_path.exists():
            return None
        try:
            with cache_path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            logger.debug(f"Failed to load item catalog cache: {type(exc).__name__}: {exc}")
            return None

        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return data
        return None

    @staticmethod
    def _save_item_catalog_cache(items):
        cache_path = GameAPI._get_item_catalog_cache_path()
        try:
            GameAPI._write_cache_atomic(
                cache_path,
                {
                    "updated_at": int(time.time()),
                    "count": len(items),
                    "items": items,
                },
            )
        except OSError as exc:
            logger.debug(f"Failed to save item catalog cache: {type(exc).__name__}: {exc}")

    @staticmethod
    def _get_item_catalog_cache_updated_at(cache_data):
        if not isinstance(cache_data, dict):
            return 0
        try:
            return int(cache_data.get("updated_at", 0) or 0)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _is_item_catalog_cache_fresh(cls, cache_data, now=None):
        if not isinstance(cache_data, dict) or not isinstance(cache_data.get("items"), list):
            return False
        updated_at = cls._get_item_catalog_cache_updated_at(cache_data)
        if updated_at <= 0:
            return False
        if now is None:
            now = time.time()
        return (float(now) - updated_at) <= ITEM_CATALOG_CACHE_TTL_SECONDS

    async def get_login_token(self):
        authorize_params = self._build_qq_connect_authorize_params()
        authorize_url = f"{QQ_CONNECT_AUTHORIZE_URL}?{urlencode(authorize_params)}"
        try:
            response, result = await self._request_text(
                "GET",
                QQ_CONNECT_AUTHORIZE_URL,
                params=authorize_params,
                headers=self._get_headers(),
                error_context="Failed to get QQ connect authorize page",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "获取登录token失败", "data": {}}
        if response["status"] != 200:
            return {"status": False, "message": "获取登录token失败", "data": {}}

        xlogin_url = self._extract_qq_connect_xlogin_url_from_authorize_page(result)
        if not xlogin_url:
            logger.warning("Failed to extract QQ xlogin URL from authorize page")
            return {"status": False, "message": "获取登录token失败", "data": {}}

        authorize_cookies = self._parse_cookies(response.get("cookies", {}))
        try:
            xlogin_response, xlogin_result = await self._request_text(
                "GET",
                xlogin_url,
                headers={
                    **self._get_headers(),
                    "Referer": response.get("url") or authorize_url,
                },
                cookies=authorize_cookies,
                error_context="Failed to get QQ login token",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "获取登录token失败", "data": {}}
        if xlogin_response["status"] != 200:
            return {"status": False, "message": "获取登录token失败", "data": {}}

        cookies = self._merge_cookies(authorize_cookies, xlogin_response.get("cookies", {}))
        login_config = self._extract_qq_login_config_from_xlogin_page(xlogin_result)
        login_config = self._merge_qq_login_config_from_url(login_config, xlogin_url)
        authorize_page_url = self._normalize_message_text(response.get("url"))
        parsed_authorize_page_url = urlparse(authorize_page_url)
        if (
            authorize_page_url
            and parsed_authorize_page_url.scheme == "https"
            and (parsed_authorize_page_url.hostname or "").lower() == "graph.qq.com"
            and parsed_authorize_page_url.path == "/oauth2.0/show"
        ):
            login_config["authorize_url"] = authorize_page_url

        authorize_need_login = self._extract_qq_connect_authorize_need_login(result)
        if authorize_need_login is not None:
            login_config["authorize_need_login"] = authorize_need_login
        login_sig = (
            self._normalize_message_text(login_config.get("login_sig"))
            or cookies.get("pt_login_sig", "")
        )
        return {
            "status": True,
            "message": "获取成功",
            "data": {
                "cookie": cookies,
                "loginSig": login_sig,
                "loginConfig": login_config,
            },
        }

    @staticmethod
    def _calc_qr_token(qrsig):
        e = 0
        for char in str(qrsig or ""):
            e += (e << 5) + ord(char)
        return e & 2147483647

    async def get_qq_login_qr(self):
        login_token = await self.get_login_token()
        if not login_token.get("status"):
            return {
                "status": False,
                "message": login_token.get("message", "获取登录token失败"),
                "data": {},
            }

        login_config = self._normalize_qq_login_config(
            login_token.get("data", {}).get("loginConfig", {})
        )
        params = {
            "appid": login_config.get("appid", str(LOGIN_APP_ID)),
            "e": 2,
            "l": "M",
            "s": 3,
            "d": 72,
            "v": 4,
            "t": time.time(),
            "u1": login_config.get("s_url", QQ_LOGIN_S_URL),
        }
        daid = self._normalize_message_text(login_config.get("daid"))
        if daid:
            params["daid"] = daid
        pt_3rd_aid = login_config.get("pt_3rd_aid")
        if pt_3rd_aid not in (None, ""):
            params["pt_3rd_aid"] = str(pt_3rd_aid)
        try:
            response, image_bytes = await self._request_bytes(
                "GET",
                QQ_QR_SHOW_URL,
                params=params,
                headers=self._build_qq_login_headers(login_config),
                cookies=login_token.get("data", {}).get("cookie", {}),
                error_context="Failed to get QQ login QR",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "获取二维码失败", "data": {}}

        if response["status"] != 200:
            return {"status": False, "message": "获取二维码失败", "data": {}}

        cookie_dict = self._merge_cookies(
            login_token.get("data", {}).get("cookie", {}),
            response["cookies"],
        )
        qr_sig_value = cookie_dict.get("qrsig", "")
        login_sig_value = (
            str(login_token.get("data", {}).get("loginSig", "")).strip()
            or cookie_dict.get("pt_login_sig", "")
        )
        if not qr_sig_value:
            return {"status": False, "message": "获取二维码失败，请重试", "data": {}}

        return {
            "status": True,
            "message": "获取成功",
            "data": {
                "image_base64": base64.b64encode(image_bytes).decode("utf-8"),
                "cookie": cookie_dict,
                "qrSig": qr_sig_value,
                "qrToken": self._calc_qr_token(qr_sig_value),
                "loginSig": login_sig_value,
                "loginConfig": login_config,
            },
        }

    async def get_login_status(self, cookie, qr_sig, qr_token, login_sig, login_config=None):
        cookies = self._parse_cookies(cookie)
        if not cookies:
            return {"code": -1, "message": "缺少cookie参数", "data": {}}

        normalized_login_config = self._normalize_qq_login_config(login_config)
        login_sig_value = (
            self._normalize_message_text(login_sig)
            or normalized_login_config.get("login_sig")
            or cookies.get("pt_login_sig", "")
        )
        cookies["qrsig"] = str(qr_sig)
        params = {
            "u1": normalized_login_config.get("s_url", QQ_LOGIN_S_URL),
            "ptqrtoken": qr_token,
            "ptredirect": normalized_login_config.get("target", "1"),
            "h": 1,
            "t": 1,
            "g": 1,
            "from_ui": 1,
            "ptlang": normalized_login_config.get("lang", "2052"),
            "action": f"0-0-{int(time.time() * 1000)}",
            "js_type": 1,
            "login_sig": login_sig_value,
            "pt_uistyle": normalized_login_config.get("style", "40"),
            "aid": normalized_login_config.get("appid", str(LOGIN_APP_ID)),
        }
        js_ver = self._normalize_message_text(normalized_login_config.get("ptui_version"))
        if js_ver:
            params["js_ver"] = js_ver
        daid = self._normalize_message_text(normalized_login_config.get("daid"))
        if daid:
            params["daid"] = daid
        pt_3rd_aid = self._normalize_message_text(
            normalized_login_config.get("pt_3rd_aid")
        )
        if pt_3rd_aid and pt_3rd_aid != "0":
            params["pt_3rd_aid"] = pt_3rd_aid
        ptdrvs = self._normalize_message_text(cookies.get("ptdrvs"))
        if ptdrvs:
            params["ptdrvs"] = ptdrvs

        try:
            response, result = await self._request_text(
                "GET",
                QQ_LOGIN_STATUS_URL,
                params=params,
                headers=self._build_qq_login_headers(normalized_login_config),
                cookies=cookies,
                error_context="Failed to get QQ login status",
            )
        except REQUEST_EXCEPTIONS as exc:
            logger.warning(
                f"Failed to get QQ login status: {type(exc).__name__}: {exc}"
            )
            return {
                "code": -4,
                "message": f"获取登录状态失败（网络异常：{type(exc).__name__}）",
                "data": {},
            }

        if response["status"] != 200:
            logger.warning(
                f"Unexpected QQ login status response code: {response['status']}"
            )
            return {"code": -5, "message": "响应错误", "data": {}}
        if not result:
            return {"code": -1, "message": "qrSig参数不正确", "data": {}}

        pattern = r"ptuiCB\s*\(\s*'(.*?)'\s*,\s*'(.*?)'\s*,\s*'(.*?)'\s*,\s*'(.*?)'\s*,\s*'(.*?)'\s*,\s*'(.*?)'\s*\)"
        matches = re.search(pattern, result)
        if not matches:
            logger.warning(
                f"Unexpected QQ login status payload: {result[:160]!r}"
            )
            return {"code": -4, "message": "获取登录状态失败（响应格式错误）", "data": {}}

        code = matches.group(1)
        message = matches.group(5)
        if code == "65":
            return {"code": -2, "message": message, "data": {}}
        if code == "66":
            return {"code": 1, "message": message, "data": {}}
        if code == "67":
            return {"code": 2, "message": message, "data": {}}
        if code == "86":
            return {"code": -3, "message": message, "data": {}}
        if code != "0":
            return {"code": -4, "message": message, "data": {}}

        merged_cookies = self._merge_cookies(cookies, response["cookies"])
        redirect_url = matches.group(3)
        if not self._is_allowed_redirect_target(redirect_url):
            logger.warning(
                "Rejected unexpected QQ login redirect target while finalizing login status: "
                f"{redirect_url}"
            )
            return {"code": -4, "message": "获取登录状态失败（回跳地址异常）", "data": {}}
        try:
            redirect_response, _ = await self._request_get_with_allowed_redirects(
                redirect_url,
                headers=self._build_qq_login_headers(normalized_login_config),
                cookies=merged_cookies,
                error_context="Failed to finalize QQ login status",
            )
        except REQUEST_EXCEPTIONS as exc:
            logger.warning(
                f"Failed to finalize QQ login status: {type(exc).__name__}: {exc}"
            )
            return {
                "code": -4,
                "message": f"获取登录状态失败（登录回跳失败：{type(exc).__name__}）",
                "data": {},
            }

        all_cookies = self._merge_cookies(merged_cookies, redirect_response["cookies"])
        return {"code": 0, "message": "登录成功", "data": {"cookie": all_cookies}}

    async def get_access_token_by_cookie(self, cookie, login_config=None):
        cookies = self._parse_cookies(cookie)
        if not cookies:
            return {"status": False, "message": "Cookie无效，请重新扫码登录", "data": {}}

        raw_login_config = login_config if isinstance(login_config, dict) else {}
        enable_authorize_show_flow = raw_login_config.get("enable_authorize_show_flow")
        if isinstance(enable_authorize_show_flow, str):
            lowered_enable_authorize_show_flow = enable_authorize_show_flow.strip().lower()
            if lowered_enable_authorize_show_flow in {"true", "1", "yes", "on"}:
                enable_authorize_show_flow = True
            elif lowered_enable_authorize_show_flow in {"false", "0", "no", "off"}:
                enable_authorize_show_flow = False
            else:
                enable_authorize_show_flow = None
        elif isinstance(enable_authorize_show_flow, (int, float)):
            enable_authorize_show_flow = bool(enable_authorize_show_flow)
        elif not isinstance(enable_authorize_show_flow, bool):
            enable_authorize_show_flow = None

        if enable_authorize_show_flow is None:
            enable_authorize_show_flow = QQ_AUTHORIZE_SHOW_FLOW_ENABLED

        if not enable_authorize_show_flow:
            logger.info(
                "Using legacy QQ authorize exchange flow by default; "
                "the oauth2.0/show-based flow is disabled."
            )
            legacy_headers = self._build_legacy_qq_authorize_headers(raw_login_config, cookies)
            legacy_form_data = self._build_legacy_qq_authorize_form_data(cookies)
            exchange_result = await self._perform_qq_authorize_exchange(
                cookies,
                legacy_headers,
                legacy_form_data,
            )
            if not exchange_result.get("status"):
                return exchange_result
            exchange_data = exchange_result.get("data", {})
            auth_code = self._normalize_message_text(exchange_data.get("auth_code"))
            merged_cookies = self._merge_cookies(cookies, exchange_data.get("cookies", {}))
            location = self._normalize_message_text(exchange_data.get("location"))
            response = exchange_data.get("response") or {}
            redirect_response = exchange_data.get("redirect_response") or {}
            final_url = self._normalize_message_text((redirect_response or {}).get("url"))
            if not auth_code:
                logger.warning(
                    "QQ authorize exchange did not return an auth code. "
                    f"status={response.get('status')} location={location!r} "
                    f"final_url={final_url!r}"
                )
                return {"status": False, "message": "未获取到授权码，请重新扫码登录", "data": {}}

            params = {
                "a": "qcCodeToOpenId",
                "qc_code": auth_code,
                "appid": APPID,
                "redirect_uri": "https://milo.qq.com/comm-htdocs/login/qc_redirect.html",
                "callback": "miloJsonpCb_86690",
                "_": self._get_micro_time(),
            }
            try:
                _, result = await self._request_text(
                    "GET",
                    "https://ams.game.qq.com/ams/userLoginSvr",
                    params=params,
                    headers={"referer": "https://df.qq.com/"},
                    cookies=merged_cookies,
                    error_context="Failed to exchange QQ code for access token",
                )
            except REQUEST_EXCEPTIONS:
                return {"status": False, "message": "获取access token失败", "data": {}}

            jsonp_match = re.search(r"try\{miloJsonpCb_86690\((\{.*?\})\);\}catch\(e\)\{\}", result)
            if not jsonp_match:
                jsonp_match = re.search(r"miloJsonpCb_86690\((\{.*?\})\)", result)
            if not jsonp_match:
                return {"status": False, "message": "AccessToken获取失败", "data": {}}

            try:
                json_data = json.loads(jsonp_match.group(1))
            except json.JSONDecodeError as exc:
                logger.warning(f"Failed to decode QQ access token response: {exc}")
                return {"status": False, "message": "AccessToken获取失败", "data": {}}

            if str(json_data.get("iRet")) != "0":
                return {"status": False, "message": "AccessToken获取失败", "data": {}}

            return {
                "status": True,
                "message": "获取成功",
                "data": {
                    "access_token": json_data.get("access_token", ""),
                    "expires_in": json_data.get("expires_in", ""),
                    "openid": json_data.get("openid", ""),
                },
            }

        authorize_page_url = self._normalize_message_text(
            raw_login_config.get("authorize_url") or raw_login_config.get("authorizeUrl")
        )
        parsed_authorize_page_url = urlparse(authorize_page_url)
        if not (
            authorize_page_url
            and parsed_authorize_page_url.scheme == "https"
            and (parsed_authorize_page_url.hostname or "").lower() == "graph.qq.com"
            and parsed_authorize_page_url.path == "/oauth2.0/show"
            and self._is_allowed_redirect_target(authorize_page_url)
        ):
            authorize_page_url = ""

        authorize_need_login = raw_login_config.get("authorize_need_login")
        if isinstance(authorize_need_login, str):
            lowered_authorize_need_login = authorize_need_login.strip().lower()
            if lowered_authorize_need_login in {"true", "1", "yes"}:
                authorize_need_login = True
            elif lowered_authorize_need_login in {"false", "0", "no"}:
                authorize_need_login = False
            else:
                authorize_need_login = None
        elif isinstance(authorize_need_login, (int, float)):
            authorize_need_login = bool(authorize_need_login)
        elif not isinstance(authorize_need_login, bool):
            authorize_need_login = None

        merged_cookies = dict(cookies)
        if not authorize_page_url:
            authorize_params = self._build_qq_connect_authorize_show_params()
            try:
                authorize_page_response, authorize_page_text = await self._request_get_with_allowed_redirects(
                    QQ_CONNECT_AUTHORIZE_URL,
                    params=authorize_params,
                    headers=self._get_headers(),
                    cookies=cookies,
                    error_context="Failed to prepare QQ authorize page",
                )
            except REQUEST_EXCEPTIONS:
                return {"status": False, "message": "获取access token失败", "data": {}}

            merged_cookies = self._merge_cookies(cookies, authorize_page_response["cookies"])
            authorize_page_url = (
                self._normalize_message_text(authorize_page_response.get("url"))
                or f"{QQ_CONNECT_AUTHORIZE_URL}?{urlencode(authorize_params)}"
            )
            if authorize_need_login is None:
                authorize_need_login = self._extract_qq_connect_authorize_need_login(
                    authorize_page_text
                )
        authorize_query = parse_qs(urlparse(authorize_page_url).query)

        def _query_value(name, default=""):
            return self._normalize_message_text((authorize_query.get(name) or [default])[0]) or default

        headers = {
            "Referer": authorize_page_url,
            "Origin": "https://graph.qq.com",
            "Content-Type": "application/x-www-form-urlencoded",
            "X-G-TK": str(self._get_gtk(cookies.get("p_skey", ""))),
        }
        form_data = {
            "response_type": _query_value("response_type", "code"),
            "client_id": _query_value("client_id", str(APPID)),
            "redirect_uri": _query_value(
                "redirect_uri",
                f"{QQ_CONNECT_REDIRECT_URI}?parent_domain=https://df.qq.com&isMiloSDK=1&isPc=1",
            ),
            "scope": _query_value("scope", QQ_CONNECT_SCOPE),
            "state": _query_value("state", QQ_CONNECT_STATE),
            "switch": _query_value("switch", ""),
            "from_ptlogin": 1,
            "src": int(_query_value("src", "1") or 1),
            "update_auth": 1 if authorize_need_login is not False else 0,
            "openapi": "1010",
            "g_tk": self._get_gtk(cookies.get("p_skey", "")),
            "auth_time": int(time.time() * 1000),
            "ui": str(uuid.uuid4()).upper(),
        }

        exchange_result = await self._perform_qq_authorize_exchange(
            merged_cookies,
            headers,
            form_data,
        )
        if not exchange_result.get("status"):
            return exchange_result

        exchange_data = exchange_result.get("data", {})
        auth_code = self._normalize_message_text(exchange_data.get("auth_code"))
        merged_cookies = self._merge_cookies(
            merged_cookies,
            exchange_data.get("cookies", {}),
        )
        location = self._normalize_message_text(exchange_data.get("location"))
        response = exchange_data.get("response") or {}
        redirect_response = exchange_data.get("redirect_response") or {}

        final_url = self._normalize_message_text((redirect_response or {}).get("url"))
        if (
            not auth_code
            and (
                self._is_qq_authorize_show_url(location)
                or self._is_qq_authorize_show_url(final_url)
            )
        ):
            logger.warning(
                "QQ authorize exchange bounced back to oauth2.0/show; "
                "falling back to legacy direct code exchange."
            )
            legacy_headers = self._build_legacy_qq_authorize_headers(raw_login_config, merged_cookies)
            legacy_form_data = self._build_legacy_qq_authorize_form_data(merged_cookies)
            legacy_result = await self._perform_qq_authorize_exchange(
                merged_cookies,
                legacy_headers,
                legacy_form_data,
            )
            if not legacy_result.get("status"):
                return legacy_result
            legacy_data = legacy_result.get("data", {})
            auth_code = self._normalize_message_text(legacy_data.get("auth_code"))
            merged_cookies = self._merge_cookies(
                merged_cookies,
                legacy_data.get("cookies", {}),
            )
            location = self._normalize_message_text(legacy_data.get("location"))
            response = legacy_data.get("response") or {}
            redirect_response = legacy_data.get("redirect_response") or {}
            final_url = self._normalize_message_text((redirect_response or {}).get("url"))

        if not auth_code:
            logger.warning(
                "QQ authorize exchange did not return an auth code. "
                f"status={response.get('status')} location={location!r} "
                f"final_url={final_url!r}"
            )
            return {"status": False, "message": "未获取到授权码，请重新扫码登录", "data": {}}

        params = {
            "a": "qcCodeToOpenId",
            "qc_code": auth_code,
            "appid": APPID,
            "redirect_uri": "https://milo.qq.com/comm-htdocs/login/qc_redirect.html",
            "callback": "miloJsonpCb_86690",
            "_": self._get_micro_time(),
        }
        try:
            _, result = await self._request_text(
                "GET",
                "https://ams.game.qq.com/ams/userLoginSvr",
                params=params,
                headers={"referer": "https://df.qq.com/"},
                cookies=merged_cookies,
                error_context="Failed to exchange QQ code for access token",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "获取access token失败", "data": {}}

        jsonp_match = re.search(r"try\{miloJsonpCb_86690\((\{.*?\})\);\}catch\(e\)\{\}", result)
        if not jsonp_match:
            jsonp_match = re.search(r"miloJsonpCb_86690\((\{.*?\})\)", result)
        if not jsonp_match:
            return {"status": False, "message": "AccessToken获取失败", "data": {}}

        try:
            json_data = json.loads(jsonp_match.group(1))
        except json.JSONDecodeError as exc:
            logger.warning(f"Failed to decode QQ access token response: {exc}")
            return {"status": False, "message": "AccessToken获取失败", "data": {}}

        if str(json_data.get("iRet")) != "0":
            return {"status": False, "message": "AccessToken获取失败", "data": {}}

        return {
            "status": True,
            "message": "获取成功",
            "data": {
                "access_token": json_data.get("access_token", ""),
                "expires_in": json_data.get("expires_in", ""),
                "openid": json_data.get("openid", ""),
            },
        }

    async def get_wechat_login_qr(self):
        params = {
            "appid": "wxfa0c35392d06b82f",
            "scope": "snsapi_login",
            "redirect_uri": "https://iu.qq.com/comm-htdocs/login/milosdk/wx_pc_redirect.html?appid=wxfa0c35392d06b82f&sServiceType=undefined&originalUrl=https%3A%2F%2Fdf.qq.com%2Fcp%2Frecord202410ver%2F&oriOrigin=https%3A%2F%2Fdf.qq.com",
            "state": 1,
            "login_type": "jssdk",
            "self_redirect": "true",
            "ts": self._get_micro_time(),
            "style": "black",
        }
        headers = {"referer": "https://df.qq.com/"}
        try:
            _, result = await self._request_text(
                "GET",
                WECHAT_QR_URL,
                params=params,
                headers=headers,
                error_context="Failed to get WeChat login QR",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "获取微信登录二维码失败", "data": {}}

        qrcode_match = re.search(r'/connect/qrcode/[^\s<>"]+', result)
        if not qrcode_match:
            return {"status": False, "message": "获取二维码失败", "data": {}}
        qrcode_path = qrcode_match.group(0)
        uuid = qrcode_path[16:]
        qrcode_url = f"https://open.weixin.qq.com{qrcode_path}"
        return {"status": True, "message": "获取成功", "data": {"qrCode": qrcode_url, "uuid": uuid}}

    async def check_wechat_login_status(self, uuid):
        if not uuid:
            return {"status": False, "message": "缺少参数", "code": -1, "data": {}}
        try:
            _, result = await self._request_text(
                "GET",
                WECHAT_QR_STATUS_URL,
                params={"uuid": uuid},
                error_context="Failed to get WeChat login status",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "获取微信登录状态失败", "code": -4, "data": {}}

        errcode_match = re.search(r"wx_errcode=(\d+);", result)
        code_match = re.search(r"wx_code='([^']*)';", result)
        if not errcode_match:
            return {"status": False, "message": "响应格式错误", "code": -4, "data": {}}

        wx_errcode = int(errcode_match.group(1))
        wx_code = code_match.group(1) if code_match else ""
        if wx_errcode == 408:
            return {"status": True, "message": "等待扫码", "code": 1, "data": {}}
        if wx_errcode == 404:
            return {"status": True, "message": "已扫码", "code": 2, "data": {}}
        if wx_errcode == 405:
            return {"status": True, "message": "扫码成功", "code": 3, "data": {"wx_code": wx_code}}
        if wx_errcode == 403:
            return {"status": False, "message": "扫码被拒绝", "code": -3, "data": {}}
        return {"status": False, "message": "其他错误代码", "code": -4, "data": {"wx_errcode": wx_errcode, "wx_code": wx_code}}

    async def get_wechat_access_token(self, code):
        if not code:
            return {"status": False, "message": "缺少参数", "data": {}}
        params = {
            "callback": "",
            "appid": "wxfa0c35392d06b82f",
            "wxcode": code,
            "originalUrl": "https://df.qq.com/cp/record202410ver/",
            "wxcodedomain": "iu.qq.com",
            "acctype": "wx",
            "sServiceType": "undefined",
            "_": self._get_micro_time(),
        }
        headers = {"referer": "https://df.qq.com/"}
        try:
            _, result = await self._request_text(
                "GET",
                "https://apps.game.qq.com/ams/ame/codeToOpenId.php",
                params=params,
                headers=headers,
                error_context="Failed to get WeChat access token",
            )
            data = json.loads(result)
            token_data = json.loads(data.get("sMsg", "{}"))
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "获取微信访问令牌失败", "data": {}}

        if data.get("iRet") != 0:
            return {"status": False, "message": "获取失败", "data": {}}
        return {
            "status": True,
            "message": "获取成功",
            "data": {
                "access_token": token_data.get("access_token", ""),
                "expires_in": token_data.get("expires_in", ""),
                "openid": token_data.get("openid", ""),
            },
        }
    async def bind_account(self, access_token, openid, platform=None):
        access_type = platform or self.platform
        if not openid or not access_token:
            return {"status": False, "message": "缺少参数", "error_kind": "invalid_request", "data": {}}

        cookies = self._get_cookies(openid, access_token, access_type)
        form_data = {
            "iChartId": 316964,
            "iSubChartId": 316964,
            "sIdeToken": "95ookO",
        }
        try:
            _, data = await self._post_base_json(
                form_data,
                cookies=cookies,
                error_context="Failed to fetch bind area",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "绑定失败", "error_kind": "upstream_error", "data": {}}

        if data.get("ret") != 0:
            response_message = self._extract_response_message(data)
            error_kind = "credential_expired" if self._is_credential_expired_message(response_message) else "upstream_error"
            return {
                "status": False,
                "message": response_message or f"绑定状态校验失败(ret={data.get('ret')})",
                "error_kind": error_kind,
                "data": {"ret": data.get("ret")},
            }

        bindarea = data.get("jData", {}).get("bindarea") or {}
        role_id = str(
            bindarea.get("role_id")
            or bindarea.get("roleId")
            or bindarea.get("sRoleId")
            or ""
        ).strip()
        role_data = {}
        if not bindarea or not role_id:
            role_data = await self._fetch_role_profile(access_token, openid, access_type)
            role_id = role_id or str(role_data.get("role_id", "")).strip()

        if bindarea:
            bindarea = dict(bindarea)
            if role_id:
                bindarea["role_id"] = role_id
            return {"status": True, "message": "获取成功", "data": bindarea}

        checkparam = role_data.get("checkparam", "")
        checkparam_parts = checkparam.split("|")
        if len(checkparam_parts) < 3:
            return {"status": False, "message": "角色信息解析失败", "error_kind": "unexpected_payload", "data": {}}

        role_id = role_id or checkparam_parts[2]
        bind_form = {
            "iChartId": 316965,
            "iSubChartId": 316965,
            "sIdeToken": "sTzZS2",
            "sArea": 36,
            "sPlatId": 1,
            "sPartition": 36,
            "sCheckparam": checkparam,
            "sRoleId": role_id,
            "md5str": role_data.get("md5str", ""),
        }
        try:
            _, bind_result = await self._post_base_json(
                bind_form,
                cookies=cookies,
                error_context="Failed to bind role",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "绑定失败", "error_kind": "upstream_error", "data": {}}

        if bind_result.get("ret") != 0:
            response_message = self._extract_response_message(bind_result)
            error_kind = "credential_expired" if self._is_credential_expired_message(response_message) else "upstream_error"
            return {
                "status": False,
                "message": response_message or f"绑定角色失败(ret={bind_result.get('ret')})",
                "error_kind": error_kind,
                "data": {"ret": bind_result.get("ret")},
            }

        bind_data = bind_result.get("jData", {}).get("bindarea", {}) or {}
        if role_id:
            bind_data = dict(bind_data)
            bind_data["role_id"] = role_id
        return {"status": True, "message": "获取成功", "data": bind_data}

    async def fetch_records(self, openid, access_token, type_id=4, page=1, platform="qq"):
        """
        获取战绩
        type_id: 4 烽火地带, 5 全面战场
        """
        cookies = self._get_cookies(openid, access_token, platform)
        data = {
            "iChartId": "319386",
            "iSubChartId": "319386",
            "sIdeToken": "zMemOt",
            "type": str(type_id),
            "page": str(page),
        }
        try:
            _, result = await self._post_base_json(
                data,
                cookies=cookies,
                error_context=f"Failed to fetch records (type={type_id}, page={page})",
            )
        except REQUEST_EXCEPTIONS:
            return []

        if result.get("ret") == 0:
            return result.get("jData", {}).get("data", [])
        return []

    async def fetch_records_v2(self, openid, access_token, type_id=4, page=1, platform="qq"):
        """
        获取新版战绩。
        type_id: 4 烽火地带, 5 全面战场
        """
        cookies = self._get_cookies(openid, access_token, platform)
        data = {
            "iChartId": "450526",
            "iSubChartId": "450526",
            "sIdeToken": "PHq59Y",
            "type": str(type_id),
            "page": str(page),
        }
        try:
            _, result = await self._post_base_json(
                data,
                cookies=cookies,
                error_context=f"Failed to fetch records_v2 (type={type_id}, page={page})",
            )
        except REQUEST_EXCEPTIONS:
            return []

        if result.get("ret") == 0:
            return result.get("jData", {}).get("data", [])
        return []

    async def fetch_room_info(self, openid, access_token, room_id, platform="qq"):
        """
        获取烽火地带房间信息。
        """
        cookies = self._get_cookies(openid, access_token, platform)
        data = {
            "iChartId": "450471",
            "iSubChartId": "450471",
            "sIdeToken": "ylP3eG",
            "roomId": str(room_id),
            "type": "2",
        }
        try:
            _, result = await self._post_base_json(
                data,
                cookies=cookies,
                error_context=f"Failed to fetch room_info (room_id={room_id})",
            )
        except REQUEST_EXCEPTIONS:
            return []

        if result.get("ret") == 0:
            return result.get("jData", {}).get("data", [])
        return []

    async def fetch_room_flow(self, openid, access_token, room_id, type_id=1, platform="qq"):
        """
        获取战绩流水补充信息。
        type_id=1: 烽火详情补充/昵称
        type_id=3: 烽火收益补充
        """
        cookies = self._get_cookies(openid, access_token, platform)
        data = {
            "iChartId": "450471",
            "iSubChartId": "450471",
            "sIdeToken": "ylP3eG",
            "roomId": str(room_id),
            "typeId": str(type_id),
        }
        try:
            _, result = await self._post_base_json(
                data,
                cookies=cookies,
                error_context=f"Failed to fetch room_flow (room_id={room_id}, type_id={type_id})",
            )
        except REQUEST_EXCEPTIONS:
            return None

        if result.get("ret") == 0:
            return result.get("jData", {}).get("data")
        return None

    async def fetch_item_flow(self, openid, access_token, page=1, platform="qq"):
        """
        获取单页道具流水，包含真实物品增减记录。
        type=2 为道具流水。
        """
        cookies = self._get_cookies(openid, access_token, platform)
        data = {
            "iChartId": "319386",
            "iSubChartId": "319386",
            "sIdeToken": "zMemOt",
            "type": "2",
            "page": str(page),
        }
        try:
            _, result = await self._post_base_json(
                data,
                cookies=cookies,
                error_context=f"Failed to fetch item_flow (page={page})",
            )
        except REQUEST_EXCEPTIONS:
            return []

        if result.get("ret") == 0:
            data_obj = result.get("jData", {}).get("data", {})
            item_arr = data_obj.get("itemArr", []) if isinstance(data_obj, dict) else []
            normalized = []
            for item in item_arr:
                if not isinstance(item, dict):
                    continue
                reason = unquote(str(item.get("Reason", "")))
                normalized.append({
                    "dtEventTime": item.get("dtEventTime", ""),
                    "iGoodsId": str(item.get("iGoodsId", "")),
                    "Name": item.get("Name", ""),
                    "AfterCount": item.get("AfterCount", 0),
                    "AddOrReduce": str(item.get("AddOrReduce", "0")),
                    "Reason": reason,
                })
            return normalized
        return []

    async def fetch_all_item_flows(self, openid, access_token, max_pages=10, platform="qq"):
        """
        分页获取全量道具流水。
        max_pages：最多拉取页数，防止无限循环。
        """
        all_flows = []
        for page in range(1, max_pages + 1):
            page_flows = await self.fetch_item_flow(
                openid,
                access_token,
                page=page,
                platform=platform,
            )
            if not page_flows:
                break
            all_flows.extend(page_flows)
        return all_flows

    async def fetch_items_info(self, item_ids):
        """
        获取物品详情，以匹配中文可读名字
        """
        if not isinstance(item_ids, list):
            item_ids = [item_ids]
        data = dict(OBJECT_LIST_PARAMS)
        data["param"] = json.dumps({"objectID": item_ids}, ensure_ascii=False)
        try:
            _, result = await self._post_base_json(
                data,
                error_context=f"Failed to fetch items_info (count={len(item_ids)})",
            )
        except REQUEST_EXCEPTIONS:
            return []

        if result.get("ret") == 0:
            return result.get("jData", {}).get("data", {}).get("data", {}).get("list", [])
        return []

    async def _fetch_item_catalog_from_remote(self, openid, access_token, platform=None):
        cookies = self.create_cookie(openid, access_token, platform=platform)
        data = dict(OBJECT_LIST_PARAMS)
        data["param"] = json.dumps({"primary": "props", "objectID": ""}, ensure_ascii=False)

        try:
            _, result = await self._post_base_json(
                data,
                cookies=cookies,
                error_context="Failed to fetch item catalog",
            )
        except REQUEST_EXCEPTIONS:
            return None

        if result.get("ret") != 0:
            return None
        items = result.get("jData", {}).get("data", {}).get("data", {}).get("list", [])
        if not isinstance(items, list):
            return None
        self._save_item_catalog_cache(items)
        return items

    async def refresh_item_catalog(self, openid, access_token, platform=None):
        items = await self._fetch_item_catalog_from_remote(
            openid,
            access_token,
            platform=platform,
        )
        if items is not None:
            return {
                "status": True,
                "items": items,
                "source": "network",
            }

        cached = self._load_item_catalog_cache()
        if cached:
            return {
                "status": False,
                "items": cached.get("items", []),
                "source": "cache",
            }
        return {
            "status": False,
            "items": [],
            "source": "none",
        }

    async def fetch_item_catalog(
        self,
        openid,
        access_token,
        force_refresh=False,
        platform=None,
        return_metadata=False,
    ):
        """
        获取并缓存物品列表。
        物品信息相对稳定，默认优先读取本地缓存。
        """
        cached = self._load_item_catalog_cache()
        cache_is_fresh = self._is_item_catalog_cache_fresh(cached)
        if not force_refresh and cache_is_fresh:
            items = cached.get("items", [])
            if return_metadata:
                return {
                    "items": items,
                    "source": "cache",
                    "cache_status": "fresh",
                    "used_stale_cache": False,
                }
            return items

        items = await self._fetch_item_catalog_from_remote(
            openid,
            access_token,
            platform=platform,
        )
        if items is not None:
            if return_metadata:
                return {
                    "items": items,
                    "source": "network",
                    "cache_status": "refreshed" if cached else "network",
                    "used_stale_cache": False,
                }
            return items

        if cached:
            cached_items = cached.get("items", [])
            used_stale_cache = not cache_is_fresh
            if not cache_is_fresh:
                logger.warning(
                    "Failed to refresh stale item catalog from remote; "
                    "falling back to cached data."
                )
            if return_metadata:
                return {
                    "items": cached_items,
                    "source": "cache",
                    "cache_status": "stale_fallback" if used_stale_cache else "cache_fallback",
                    "used_stale_cache": used_stale_cache,
                }
            return cached_items
        if return_metadata:
            return None
        return None
