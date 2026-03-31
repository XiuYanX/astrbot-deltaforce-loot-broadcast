import asyncio
import base64
import json
import os
import re
import tempfile
import time
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import aiohttp
from astrbot.api import logger

from ..data.runtime_paths import get_runtime_file_path

APPID = 101491592
BASE_URL = "https://comm.ams.game.qq.com/ide/"
GAME_API_URL = "https://comm.aci.game.qq.com/main"
LOGIN_APP_ID = 716027609
QQ_QR_SHOW_URL = "https://xui.ptlogin2.qq.com/ssl/ptqrshow"
QQ_LOGIN_TICKET_URL = "https://xui.ptlogin2.qq.com/cgi-bin/xlogin"
QQ_LOGIN_STATUS_URL = "https://ssl.ptlogin2.qq.com/ptqrlogin"
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
    ValueError,
)

class GameAPI:
    def __init__(self, platform="qq", timeout=REQUEST_TIMEOUT):
        self.platform = platform
        self.timeout = timeout
        self._session = None
        self._session_lock = asyncio.Lock()

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
                    cookie_jar=aiohttp.CookieJar(),
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
            parsed = GameAPI._safe_json_loads(cookie, {})
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items() if v not in ("", None)}
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
            if isinstance(source, dict):
                merged.update(cls._parse_cookies(source))
        return merged

    @classmethod
    def _snapshot_response(cls, response):
        snapshot = {
            "status": response.status,
            "headers": dict(response.headers),
            "cookies": cls._collect_response_cookies(response),
        }
        try:
            session_cookies = response._session.cookie_jar.filter_cookies(response.url)
            for key, morsel in session_cookies.items():
                snapshot["cookies"][str(key)] = str(morsel.value)
        except Exception:
            pass
        return snapshot

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
                return self._snapshot_response(response), await response.text()
        except REQUEST_EXCEPTIONS as exc:
            logger.warning(f"{error_context}: {type(exc).__name__}: {exc}")
            raise

    async def _request_json(self, method, url, *, error_context, **kwargs):
        session = await self._get_session()
        try:
            async with session.request(method, url, **kwargs) as response:
                return self._snapshot_response(response), await response.json(content_type=None)
        except REQUEST_EXCEPTIONS as exc:
            logger.warning(f"{error_context}: {type(exc).__name__}: {exc}")
            raise

    async def _request_bytes(self, method, url, *, error_context, **kwargs):
        session = await self._get_session()
        try:
            async with session.request(method, url, **kwargs) as response:
                return self._snapshot_response(response), await response.read()
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

    async def get_login_token(self):
        params = {
            "appid": LOGIN_APP_ID,
            "daid": 383,
            "style": 33,
            "login_text": "登录",
            "hide_title_bar": 1,
            "hide_border": 1,
            "target": "self",
            "s_url": "https://graph.qq.com/oauth2.0/login_jump",
            "pt_3rd_aid": APPID,
            "pt_feedback_link": f"https://support.qq.com/products/77942?customInfo=milo.qq.com.appid{APPID}",
            "theme": 2,
            "verify_theme": "",
        }
        try:
            response, _ = await self._request_text(
                "GET",
                QQ_LOGIN_TICKET_URL,
                params=params,
                headers=self._get_headers(),
                error_context="Failed to get login token",
            )
        except REQUEST_EXCEPTIONS:
            return False
        return response["status"] == 200

    @staticmethod
    def _calc_qr_token(qrsig):
        e = 0
        for char in str(qrsig or ""):
            e += (e << 5) + ord(char)
        return e & 2147483647

    async def get_qq_login_qr(self):
        if not await self.get_login_token():
            return {"status": False, "message": "获取登录token失败", "data": {}}

        params = {
            "appid": LOGIN_APP_ID,
            "e": 2,
            "l": "M",
            "s": 3,
            "d": 72,
            "v": 4,
            "t": 0.6142752744667854,
            "daid": 383,
            "pt_3rd_aid": APPID,
            "u1": "https://graph.qq.com/oauth2.0/login_jump",
        }
        try:
            response, image_bytes = await self._request_bytes(
                "GET",
                QQ_QR_SHOW_URL,
                params=params,
                headers=self._get_headers(),
                error_context="Failed to get QQ login QR",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "获取二维码失败", "data": {}}

        if response["status"] != 200:
            return {"status": False, "message": "获取二维码失败", "data": {}}

        cookie_dict = response["cookies"]
        qr_sig_value = cookie_dict.get("qrsig", "")
        login_sig_value = cookie_dict.get("pt_login_sig", "")
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
            },
        }

    async def get_login_status(self, cookie, qr_sig, qr_token, login_sig):
        cookies = self._parse_cookies(cookie)
        if not cookies:
            return {"code": -1, "message": "缺少cookie参数", "data": {}}

        cookies["qrsig"] = str(qr_sig)
        params = {
            "u1": "https://graph.qq.com/oauth2.0/login_jump",
            "ptqrtoken": qr_token,
            "ptredirect": 0,
            "h": 1,
            "t": 1,
            "g": 1,
            "from_ui": 1,
            "ptlang": 2052,
            "action": f"0-0-{int(time.time() * 1000)}",
            "js_ver": 25040111,
            "js_type": 1,
            "login_sig": login_sig,
            "pt_uistyle": 40,
            "aid": LOGIN_APP_ID,
            "daid": 383,
            "pt_3rd_aid": APPID,
            "o1vId": "378b06c889d9113b39e814ca627809e3",
            "pt_js_version": "530c3f68",
        }

        try:
            response, result = await self._request_text(
                "GET",
                QQ_LOGIN_STATUS_URL,
                params=params,
                headers=self._get_headers(),
                cookies=cookies,
                error_context="Failed to get QQ login status",
            )
        except REQUEST_EXCEPTIONS:
            return {"code": -4, "message": "获取登录状态失败", "data": {}}

        if response["status"] != 200:
            return {"code": -5, "message": "响应错误", "data": {}}
        if not result:
            return {"code": -1, "message": "qrSig参数不正确", "data": {}}

        pattern = r"ptuiCB\s*\(\s*'(.*?)'\s*,\s*'(.*?)'\s*,\s*'(.*?)'\s*,\s*'(.*?)'\s*,\s*'(.*?)'\s*,\s*'(.*?)'\s*\)"
        matches = re.search(pattern, result)
        if not matches:
            return {"code": -4, "message": "响应格式错误", "data": {}}

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
        try:
            redirect_response, _ = await self._request_text(
                "GET",
                matches.group(3),
                headers=self._get_headers(),
                cookies=merged_cookies,
                error_context="Failed to finalize QQ login status",
            )
        except REQUEST_EXCEPTIONS:
            return {"code": -4, "message": "获取登录状态失败", "data": {}}

        all_cookies = self._merge_cookies(merged_cookies, redirect_response["cookies"])
        return {"code": 0, "message": "登录成功", "data": {"cookie": all_cookies}}

    async def get_access_token_by_cookie(self, cookie):
        cookie_text = cookie if isinstance(cookie, str) else json.dumps(cookie)
        if "\\" in cookie_text:
            cookie_text = cookie_text.replace("\\", "")
        cookies = self._parse_cookies(cookie_text)
        if not cookies:
            return {"status": False, "message": "Cookie无效，请重新扫码登录", "data": {}}

        headers = {
            "referer": "https://xui.ptlogin2.qq.com/",
            "Content-Type": "application/x-www-form-urlencoded",
            "X-G-TK": str(self._get_gtk(cookies.get("p_skey", ""))),
        }
        form_data = {
            "response_type": "code",
            "client_id": str(APPID),
            "redirect_uri": "https://milo.qq.com/comm-htdocs/login/qc_redirect.html?parent_domain=https://df.qq.com&isMiloSDK=1&isPc=1",
            "scope": "",
            "state": "STATE",
            "switch": "",
            "form_plogin": 1,
            "src": 1,
            "update_auth": 1,
            "openapi": 1010,
            "g_tk": self._get_gtk(cookies.get("p_skey", "")),
            "auth_time": int(time.time()),
            "ui": "979D48F3-6CE2-4E95-A789-3BD3187648B6",
        }

        try:
            response, _ = await self._request_text(
                "POST",
                "https://graph.qq.com/oauth2.0/authorize",
                data=form_data,
                headers=headers,
                cookies=cookies,
                allow_redirects=False,
                error_context="Failed to start QQ access token exchange",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "获取access token失败", "data": {}}

        location = response["headers"].get("Location", "")
        auth_code = self._extract_query_param(location, "code")
        if not auth_code:
            return {"status": False, "message": "Cookie过期，请重新扫码登录", "data": {}}

        merged_cookies = self._merge_cookies(cookies, response["cookies"])
        try:
            redirect_response, _ = await self._request_text(
                "GET",
                location,
                headers=headers,
                cookies=merged_cookies,
                error_context="Failed to complete QQ authorize redirect",
            )
        except REQUEST_EXCEPTIONS:
            return {"status": False, "message": "获取access token失败", "data": {}}

        merged_cookies = self._merge_cookies(merged_cookies, redirect_response["cookies"])
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
            return {"status": False, "message": "缺少参数", "data": {}}

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
            return {"status": False, "message": "绑定失败", "data": {}}

        if data.get("ret") != 0:
            return {"status": False, "message": "获取失败,检查鉴权是否过期", "data": {}}

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
            return {"status": False, "message": "角色信息解析失败", "data": {}}

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
            return {"status": False, "message": "绑定失败", "data": {}}

        if bind_result.get("ret") != 0:
            return {"status": False, "message": "绑定失败", "data": {}}

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

    async def fetch_item_catalog(self, openid, access_token, force_refresh=False, platform=None):
        """
        获取并缓存物品列表。
        物品信息相对稳定，默认优先读取本地缓存。
        """
        if not force_refresh:
            cached = self._load_item_catalog_cache()
            if cached:
                return cached.get("items", [])

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
            result = {}

        if result.get("ret") == 0:
            items = result.get("jData", {}).get("data", {}).get("data", {}).get("list", [])
            if isinstance(items, list):
                self._save_item_catalog_cache(items)
                return items

        cached = self._load_item_catalog_cache()
        if cached:
            return cached.get("items", [])
        return []
