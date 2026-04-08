import asyncio
from contextlib import suppress

from astrbot.api import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.message_components import Image
from astrbot.api.star import Context, Star, register

from .api.game_api import GameAPI
from .data.runtime_paths import get_runtime_data_dir
from .data.secret_store import SecretProtectionError
from .data.storage import Storage
from .monitor.red_detector import RedDetector

MAX_LOGIN_ATTEMPTS = 120
LOGIN_ATTEMPT_INTERVAL = 0.5
MAX_TRANSIENT_LOGIN_STATUS_FAILURES = 12

@register(
    "astrbot_plugin_deltaforce_loot_broadcast",
    "XiuYan",
    "AstrBot 三角洲物资播报插件，支持 QQ 扫码绑定、撤离带出检测与高价值收集品播报。",
    "1.0.16",
)
class DeltaForceRedPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.polling_task = None
        self.command_api = GameAPI("qq")
        self.storage = Storage()
        self.detector = RedDetector(self.storage, context, api=self.command_api)

    def _log_command_exception(self, command_name: str, sender_id, exc: Exception):
        logger.error(
            f"Command {command_name} failed for sender {sender_id}: "
            f"{type(exc).__name__}: {exc}"
        )

    @staticmethod
    def _sanitize_notify_origin(origin: str, sender_id: str = ""):
        return Storage.sanitize_private_notify_origin(origin, sender_id=sender_id)

    @staticmethod
    def _normalize_interaction_origin(origin: str, sender_id: str = ""):
        return Storage.normalize_interaction_origin(origin, sender_id=sender_id)

    @staticmethod
    def _is_private_chat_event(event: AstrMessageEvent) -> bool:
        is_private_chat = getattr(event, "is_private_chat", None)
        if callable(is_private_chat):
            try:
                return bool(is_private_chat())
            except Exception as exc:
                logger.warning(
                    f"Failed to inspect event private-chat state: "
                    f"{type(exc).__name__}: {exc}"
                )
        raw_origin = str(getattr(event, "unified_msg_origin", "") or "")
        return bool(Storage.sanitize_private_notify_origin(raw_origin))

    @staticmethod
    def _get_secret_error_hint(user_data):
        if not isinstance(user_data, dict):
            return ""
        if not user_data.get("_secret_errors"):
            return ""
        return "已保存的账号凭证无法解密，请重新执行 df绑定 以覆盖旧绑定；若仍失败，再执行 df解绑 后重试。"

    @staticmethod
    def _get_binding_invalid_hint(user_data):
        if not isinstance(user_data, dict):
            return ""
        if str(user_data.get("binding_status", "")).strip().lower() != "invalid":
            return ""
        reason = str(user_data.get("binding_status_reason", "")).strip()
        if reason:
            return (
                "当前保存的绑定已失效，后台监测已暂停。\n"
                "请重新执行 df绑定 以覆盖旧绑定。\n"
                f"原因：{reason}"
            )
        return "当前保存的绑定已失效，后台监测已暂停，请重新执行 df绑定。"

    @staticmethod
    def _format_failed_group_lines(failed_groups):
        lines = []
        for item in failed_groups[:5]:
            origin = str(item.get("origin", "")).strip() or "未知目标"
            lines.append(f"- {origin} -> 发送失败，请查看日志。")
        return "\n".join(lines) or "- 未拿到具体错误信息"

    async def _finish_bind(
        self,
        sender_id: str,
        user_name: str,
        platform: str,
        openid: str,
        access_token: str,
        notify_origin: str = "",
    ):
        interaction_origin = self._normalize_interaction_origin(notify_origin, sender_id=sender_id)
        safe_notify_origin = self._sanitize_notify_origin(notify_origin, sender_id=sender_id)
        existing_user = None
        if not safe_notify_origin or not interaction_origin:
            existing_user = await self.storage.get_user(sender_id)
            if isinstance(existing_user, dict) and not safe_notify_origin:
                safe_notify_origin = self._sanitize_notify_origin(
                    existing_user.get("notify_origin", ""),
                    sender_id=sender_id,
                )
            if isinstance(existing_user, dict) and not interaction_origin:
                interaction_origin = self._normalize_interaction_origin(
                    existing_user.get("interaction_origin", ""),
                    sender_id=sender_id,
                )
        bind_res = await self.command_api.bind_account(access_token, openid, platform)
        if not bind_res.get("status"):
            return False, bind_res.get("message", "绑定失败")
        role_id = str(bind_res.get("data", {}).get("role_id", "")).strip()
        try:
            await self.storage.add_user(
                sender_id,
                openid,
                access_token,
                name=user_name,
                platform=platform,
                role_id=role_id,
                notify_origin=safe_notify_origin,
                interaction_origin=interaction_origin,
            )
        except SecretProtectionError as exc:
            logger.error(f"Failed to persist bound account for sender {sender_id}: {exc}")
            return False, "无法安全保存账号凭证，请先修复本地密钥保护后再试。"
        except OSError as exc:
            logger.error(f"Failed to persist bound account for sender {sender_id}: {exc}")
            return False, "绑定信息保存失败，请检查插件运行目录写入权限后重试。"
        self.detector.clear_user_runtime_state(sender_id)
        role_suffix = f" 角色ID：{role_id}" if role_id else ""
        return True, f"绑定成功！已为玩家 {user_name} 开启大红物品监测。平台：{platform.upper()}{role_suffix}"

    async def _remember_user_origin(self, sender_id: str, event: AstrMessageEvent, user_data=None):
        raw_origin = getattr(event, "unified_msg_origin", "")
        interaction_origin = self._normalize_interaction_origin(
            raw_origin,
            sender_id=sender_id,
        )
        if not interaction_origin:
            return
        notify_origin = ""
        if self._is_private_chat_event(event):
            notify_origin = interaction_origin
        updates = {}
        if (
            not isinstance(user_data, dict)
            or str(user_data.get("interaction_origin", "")).strip() != interaction_origin
        ):
            updates["interaction_origin"] = interaction_origin
        if notify_origin and (
            not isinstance(user_data, dict)
            or str(user_data.get("notify_origin", "")).strip() != notify_origin
        ):
            updates["notify_origin"] = notify_origin
        if not updates:
            return
        try:
            await self.storage.update_user_state(sender_id, **updates)
        except OSError as exc:
            logger.warning(f"Failed to persist user origins for sender {sender_id}: {exc}")
            return
        if isinstance(user_data, dict):
            user_data.update(updates)

    async def _bind_with_qq_qr(self, event: AstrMessageEvent):
        qr_res = await self.command_api.get_qq_login_qr()
        if not qr_res.get("status"):
            yield event.plain_result(f"❌ 获取QQ二维码失败：{qr_res.get('message', '未知错误')}")
            return

        data = qr_res.get("data", {})
        image_base64 = data.get("image_base64", "")
        if not image_base64:
            yield event.plain_result("❌ 获取QQ二维码失败：二维码数据为空")
            return

        yield event.chain_result([
            Image.fromBase64(image_base64),
        ])
        yield event.plain_result("请打开手机QQ使用摄像头扫码，等待自动绑定。")

        cookie = data.get("cookie", {})
        qr_sig = data.get("qrSig", "")
        qr_token = data.get("qrToken", "")
        login_sig = data.get("loginSig", "")
        login_config = data.get("loginConfig", {})
        consecutive_status_failures = 0
        last_status_error = ""

        for _ in range(MAX_LOGIN_ATTEMPTS):
            status_res = await self.command_api.get_login_status(
                cookie,
                qr_sig,
                qr_token,
                login_sig,
                login_config,
            )
            code = status_res.get("code")
            if code == 0:
                consecutive_status_failures = 0
                last_status_error = ""
                cookie = status_res.get("data", {}).get("cookie", cookie)
                token_res = await self.command_api.get_access_token_by_cookie(
                    cookie,
                    login_config,
                )
                if not token_res.get("status"):
                    yield event.plain_result(f"❌ QQ登录成功，但换取令牌失败：{token_res.get('message', '未知错误')}")
                    return
                token_data = token_res.get("data", {})
                success, message = await self._finish_bind(
                    event.get_sender_id(),
                    event.get_sender_name(),
                    "qq",
                    token_data.get("openid", ""),
                    token_data.get("access_token", ""),
                    notify_origin=str(getattr(event, "unified_msg_origin", "") or ""),
                )
                yield event.plain_result(message if success else f"❌ {message}")
                return
            if code in (1, 2):
                consecutive_status_failures = 0
                last_status_error = ""
            elif code in (-4, -5):
                consecutive_status_failures += 1
                last_status_error = status_res.get("message", "获取登录状态失败")
                logger.warning(
                    "QQ login status poll failed transiently "
                    f"({consecutive_status_failures}/{MAX_TRANSIENT_LOGIN_STATUS_FAILURES}): "
                    f"{last_status_error}"
                )
                if consecutive_status_failures >= MAX_TRANSIENT_LOGIN_STATUS_FAILURES:
                    yield event.plain_result(f"❌ QQ登录失败：{last_status_error}")
                    return
            elif code in (-1, -2, -3):
                yield event.plain_result(f"❌ QQ登录失败：{status_res.get('message', '未知错误')}")
                return
            elif isinstance(code, int) and code < 0:
                yield event.plain_result(f"❌ QQ登录失败：{status_res.get('message', '未知错误')}")
                return
            await asyncio.sleep(LOGIN_ATTEMPT_INTERVAL)

        if last_status_error:
            yield event.plain_result(f"❌ QQ登录超时，请重新尝试。最近一次状态查询错误：{last_status_error}")
            return
        yield event.plain_result("❌ QQ登录超时，请重新尝试。")

    async def initialize(self):
        """异步的插件初始化方法，当实例化该插件类之后会自动调用该方法。"""
        logger.info("AstrBot 三角洲物资播报插件初始化...")
        logger.info(f"AstrBot 三角洲物资播报运行数据目录: {get_runtime_data_dir()}")
        # AstrBot may re-enter initialize during hot reload or recovery. Keep a
        # single polling task so we do not duplicate requests and broadcasts.
        if self.polling_task and not self.polling_task.done():
            logger.warning("Polling task is already running; skipping duplicate initialization.")
            return
        self.polling_task = asyncio.create_task(self.start_polling())

    async def start_polling(self):
        """后台轮询任务"""
        logger.info("AstrBot 三角洲物资播报轮询任务已启动")
        while True:
            try:
                await self.detector.check_all_users()
                await asyncio.sleep(120) # 每 120 秒轮询一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                # Keep the long-running background task alive at the boundary.
                # Detailed failure context goes to logs; the next loop can retry.
                logger.error(f"轮询任务异常: {type(e).__name__}: {e}")
                await asyncio.sleep(60)

    @filter.command("df绑定")
    async def bind_account(self, event: AstrMessageEvent, platform: str = "qq"):
        """绑定三角洲游戏账号。当前仅支持 QQ 扫码。"""
        platform = (platform or "qq").strip().lower()
        if platform in ("qq", "q", "腾讯"):
            async for result in self._bind_with_qq_qr(event):
                yield result
            return
        yield event.plain_result("当前版本仅支持 QQ 扫码绑定，请直接使用：df绑定")

    @filter.command("df解绑")
    async def unbind_account(self, event: AstrMessageEvent):
        """解除绑定三角洲游戏账号"""
        sender_id = str(event.get_sender_id())
        try:
            removed = await self.storage.remove_user(sender_id)
        except OSError as exc:
            logger.error(f"Failed to remove bound account for sender {sender_id}: {exc}")
            yield event.plain_result("解绑失败，请检查插件运行目录写入权限后重试。")
            return

        if not removed:
            yield event.plain_result("当前尚未绑定账号，无需解绑。")
            return

        self.detector.clear_user_runtime_state(sender_id)
        yield event.plain_result("解绑成功！已停止您的大红物品监测。")

    @filter.command("df设置群")
    async def set_group(self, event: AstrMessageEvent):
        """设置当前群为大红播报群"""
        unified_msg_origin = event.unified_msg_origin
        if not unified_msg_origin:
            yield event.plain_result("当前会话暂不支持设置为播报群，请在目标群聊中使用该命令。")
            return

        try:
            added = await self.storage.add_group(unified_msg_origin)
        except OSError as exc:
            logger.error(f"Failed to persist broadcast group {unified_msg_origin}: {exc}")
            yield event.plain_result("设置播报群失败，请检查插件运行目录写入权限后重试。")
            return

        if added:
            yield event.plain_result("设置播报群成功！后续大红物品将播报至本群。")
            return
        yield event.plain_result("当前群已经设置为播报群，无需重复设置。")

    @filter.command("df取消群绑定")
    async def unset_group(self, event: AstrMessageEvent):
        """取消当前群的大红播报绑定"""
        unified_msg_origin = event.unified_msg_origin
        if not unified_msg_origin:
            yield event.plain_result("当前会话暂不支持取消播报群绑定，请在目标群聊中使用该命令。")
            return

        try:
            removed = await self.storage.remove_group(unified_msg_origin)
        except OSError as exc:
            logger.error(f"Failed to remove broadcast group {unified_msg_origin}: {exc}")
            yield event.plain_result("取消群绑定失败，请检查插件运行目录写入权限后重试。")
            return

        if removed:
            yield event.plain_result("取消群绑定成功！本群后续将不再接收大红物品播报。")
            return
        yield event.plain_result("当前群尚未设置为播报群，无需取消群绑定。")

    @filter.command("df状态")
    async def status(self, event: AstrMessageEvent):
        """查看当前绑定状态与播报群状态"""
        sender_id = str(event.get_sender_id())
        user_data = await self.storage.get_user(sender_id)
        if user_data:
            await self._remember_user_origin(sender_id, event, user_data=user_data)
        groups = await self.storage.get_groups()
        group_counts = len(groups)

        if not user_data:
            account_status = "未保存绑定记录"
            binding_status = "无"
            binding_reason = ""
        elif user_data.get("_secret_errors"):
            account_status = "已保存绑定记录"
            binding_status = "已失效"
            binding_reason = "已保存的账号凭证无法解密"
        elif str(user_data.get("binding_status", "")).strip().lower() == "invalid":
            account_status = "已保存绑定记录"
            binding_status = "已失效"
            binding_reason = str(user_data.get("binding_status_reason", "")).strip()
        else:
            account_status = "已保存绑定记录"
            binding_status = "有效"
            binding_reason = ""

        reply = (
            f"【账号记录】：{account_status}\n"
            f"【绑定状态】：{binding_status}\n"
            f"{f'【失效原因】：{binding_reason}\n' if binding_reason else ''}"
            f"【当前共有播报群数量】：{group_counts} 个"
        )
        yield event.plain_result(reply)

    @filter.command("df刷新物品缓存")
    async def refresh_item_catalog(self, event: AstrMessageEvent):
        """强制刷新本地物品缓存"""
        sender_id = str(event.get_sender_id())
        user_data = await self.storage.get_user(sender_id)
        if not user_data:
            yield event.plain_result("您还未绑定账号！无法刷新物品缓存。")
            return
        await self._remember_user_origin(sender_id, event, user_data=user_data)

        secret_error_hint = self._get_secret_error_hint(user_data)
        if secret_error_hint:
            yield event.plain_result(secret_error_hint)
            return
        binding_invalid_hint = self._get_binding_invalid_hint(user_data)
        if binding_invalid_hint:
            yield event.plain_result(binding_invalid_hint)
            return

        openid = user_data.get("openid")
        access_token = user_data.get("access_token")
        platform = (user_data.get("platform", "qq") or "qq").strip().lower()
        if platform != "qq":
            yield event.plain_result("当前版本仅支持 QQ 账号检测，请先解绑后重新使用 df绑定。")
            return
        refresh_result = await self.detector.api.refresh_item_catalog(
            openid,
            access_token,
            platform=platform,
        )
        items = refresh_result.get("items", [])
        if refresh_result.get("status"):
            yield event.plain_result(f"物品缓存刷新成功，共 {len(items)} 条。")
            return
        if not items:
            yield event.plain_result("❌ 刷新物品缓存失败。")
            return
        yield event.plain_result(
            f"❌ 远程刷新失败，当前仍在使用本地缓存，共 {len(items)} 条。"
        )

    @filter.command("df检查")
    async def check_now(self, event: AstrMessageEvent):
        """直接检查最近一局，并按正式播报逻辑返回结果"""
        sender_id = str(event.get_sender_id())
        user_data = await self.storage.get_user(sender_id)
        if not user_data:
            yield event.plain_result("您还未绑定账号！无法进行检测诊断。")
            return
        await self._remember_user_origin(sender_id, event, user_data=user_data)

        secret_error_hint = self._get_secret_error_hint(user_data)
        if secret_error_hint:
            yield event.plain_result(secret_error_hint)
            return
        binding_invalid_hint = self._get_binding_invalid_hint(user_data)
        if binding_invalid_hint:
            yield event.plain_result(binding_invalid_hint)
            return

        yield event.plain_result("正在检查最近一局，请稍候...")

        openid = user_data.get("openid")
        access_token = user_data.get("access_token")
        platform = (user_data.get("platform", "qq") or "qq").strip().lower()
        if platform != "qq":
            yield event.plain_result("当前版本仅支持 QQ 账号检测，请先解绑后重新使用 df绑定。")
            return
        user_name = user_data.get("name", sender_id)

        try:
            payload = await self.detector.build_latest_broadcast_payload(
                openid,
                access_token,
                platform=platform,
            )
            if payload.get("error"):
                yield event.plain_result(f"❌ {payload['error']}")
                return

            detected_items = payload.get("detected_items", [])
            match_info = payload.get("match_info")
            if not detected_items:
                dt = match_info.get("dtEventTime", "最近一局") if isinstance(match_info, dict) else "最近一局"
                yield event.plain_result(f"最近一局（{dt}）未检测到带出收集品。")
                return

            groups = await self.storage.get_groups()
            if groups:
                role_id = await self.detector.ensure_user_role_id(sender_id, user_data, match_info=match_info)
                broadcast_result = await self.detector.broadcast(
                    user_name,
                    detected_items,
                    match_info,
                    role_id=role_id,
                )
                success_groups = broadcast_result.get("success_groups", [])
                failed_groups = broadcast_result.get("failed_groups", [])
                total_groups = broadcast_result.get("total_groups", 0)
                if failed_groups:
                    try:
                        await self.detector.persist_failed_broadcasts(
                            sender_id,
                            user_data,
                            broadcast_result,
                            match_info=match_info,
                        )
                    except OSError as exc:
                        logger.error(
                            f"Failed to persist pending broadcast retries for sender {sender_id}: {exc}"
                        )

                if success_groups:
                    yield event.plain_result(
                        f"最近一局已成功播报到 {len(success_groups)}/{total_groups} 个播报群。"
                    )
                    if failed_groups:
                        failed_lines = self._format_failed_group_lines(failed_groups)
                        yield event.plain_result(
                            "以下播报群发送失败，请检查平台是否支持主动消息，或重新执行 df设置群：\n"
                            f"{failed_lines}"
                        )
                else:
                    failed_lines = self._format_failed_group_lines(failed_groups)
                    yield event.plain_result(
                        "检测到了收集品，但主动播报到已配置群全部失败。\n"
                        "这通常是平台不支持主动消息，或保存的播报群会话已失效。\n"
                        f"失败详情：\n{failed_lines}"
                    )
                    yield event.plain_result(
                        "以下是本次应播报的正文：\n"
                        f"{broadcast_result.get('message', '')}"
                    )
            else:
                items_str = "\n".join([f"🧰 {item.get('name')} / 变更{item.get('change')}" for item in detected_items[:12]])
                dt = match_info.get("dtEventTime", "刚刚") if isinstance(match_info, dict) else "刚刚"
                yield event.plain_result(f"最近一局（{dt}）检测到以下带出收集品：\n{items_str}")
            
        except Exception as e:
            self._log_command_exception("df检查", sender_id, e)
            yield event.plain_result("❌ 探测失败，请稍后重试。若问题持续，请查看日志。")

    @filter.command("df检查详细")
    async def check_debug(self, event: AstrMessageEvent):
        """输出本局带出/收集品判定的详细调试信息"""
        sender_id = str(event.get_sender_id())
        user_data = await self.storage.get_user(sender_id)
        if not user_data:
            yield event.plain_result("您还未绑定账号！无法进行详细诊断。")
            return
        await self._remember_user_origin(sender_id, event, user_data=user_data)

        secret_error_hint = self._get_secret_error_hint(user_data)
        if secret_error_hint:
            yield event.plain_result(secret_error_hint)
            return
        binding_invalid_hint = self._get_binding_invalid_hint(user_data)
        if binding_invalid_hint:
            yield event.plain_result(binding_invalid_hint)
            return

        yield event.plain_result("正在生成详细调试报告，请稍候...")

        openid = user_data.get("openid")
        access_token = user_data.get("access_token")
        platform = (user_data.get("platform", "qq") or "qq").strip().lower()
        if platform != "qq":
            yield event.plain_result("当前版本仅支持 QQ 账号检测，请先解绑后重新使用 df绑定。")
            return

        try:
            report = await self.detector.build_debug_report(
                openid,
                access_token,
                platform=platform,
            )
            if report.get("error"):
                yield event.plain_result(f"❌ {report['error']}")
                return

            match = report.get("match", {})
            flow_summary = report.get("flow_summary", {})
            total_item_flows = report.get("total_item_flows", 0)
            all_carry_out_items = report.get("all_carry_out_items", [])
            all_carry_in_items = report.get("all_carry_in_items", [])
            carry_out_items = report.get("carry_out_items", [])
            collection_candidates = report.get("collection_candidates", [])

            lines = [
                "====== 详细调试报告 ======",
                f"对局时间: {match.get('event_time', '')}",
                f"房间ID: {match.get('room_id', '')}",
                f"带出价值: {match.get('final_price', '0')}",
                f"撤离状态: {match.get('escape_reason', '')}",
                "-------------------",
                f"[流水总数] {total_item_flows}",
                f"撤离带出+: {flow_summary.get('撤离带出+', 0)} | 撤离带出-: {flow_summary.get('撤离带出-', 0)}",
                f"带入局内+: {flow_summary.get('带入局内+', 0)} | 带入局内-: {flow_summary.get('带入局内-', 0)}",
                f"其他+: {flow_summary.get('其他+', 0)} | 其他-: {flow_summary.get('其他-', 0)}",
                "-------------------",
                f"[全量撤离带出+] {len(all_carry_out_items)} 条",
                f"[本局撤离带出-时间窗420秒] {len(carry_out_items)} 条",
            ]

            for item in carry_out_items:
                lines.append(f"+OUT {item.get('dtEventTime')} | {item.get('iGoodsId')} | {item.get('Name')} | {item.get('AddOrReduce')} | {item.get('Reason')}")

            lines.extend([
                "-------------------",
                f"[收集品判定] {len(collection_candidates)} 条",
            ])
            for item in collection_candidates:
                lines.append(
                    f"COLL {item.get('id')} | {item.get('name')} | collection={item.get('is_collection')} | grade={item.get('grade')} | fields={item.get('category_fields')}"
                )

            collection_true = [item for item in collection_candidates if item.get("is_collection")]
            lines.extend([
                "-------------------",
                f"[最终可播报收集品] {len(collection_true)} 条",
            ])
            for item in collection_true:
                lines.append(
                    f"PUSH {item.get('id')} | {item.get('name')} | grade={item.get('grade')} | fields={item.get('category_fields')}"
                )

            message = "\n".join(lines)
            path = self.detector.write_debug_file("debug_last_report.txt", message)
            runtime_dir = self.detector.get_runtime_debug_dir()
            yield event.plain_result(
                "详细调试报告已写入本地文件：\n"
                f"运行目录：{runtime_dir}\n"
                f"报告路径：{path}"
            )
        except Exception as e:
            self._log_command_exception("df检查详细", sender_id, e)
            yield event.plain_result("❌ 详细调试失败，请稍后重试或查看日志。")

    async def terminate(self):
        """插件销毁方法，当插件被卸载/停用时会调用。"""
        if self.polling_task and not self.polling_task.done():
            self.polling_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.polling_task
        await self.detector.close()
        await self.command_api.close()
        logger.info("AstrBot 三角洲物资播报插件已卸载")
