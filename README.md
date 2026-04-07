# AstrBot 三角洲物资播报插件 (astrbot_plugin_deltaforce_loot_broadcast)

一个用于 AstrBot 的三角洲行动插件，用于监测玩家最近对局的撤离带出物品，并在命中高价值收集品时进行群播报。
插件运行数据会优先写入 AstrBot 的 `data/plugin_data/astrbot_plugin_deltaforce_loot_broadcast/`，便于直接 `git pull` 后重载插件，而不必卸载重装。

暂时有bug，等天才程序员GPT5.4修

## 版本信息

- 当前版本：`v1.0.12`
- 发布日期：`2026-04-01`
- 更新日志：见 [CHANGELOG.md](./CHANGELOG.md)

## 最近更新

- 默认屏蔽近几版持续失败的 `oauth2.0/show` 授权页链路，QQ 绑定改回优先走更短的 legacy `authorize -> code -> qcCodeToOpenId` 兑换流程
- 仅在显式开关或测试场景下才会进入 `oauth2.0/show` 授权页链路；若该路径再次被 QQ 打回展示页，会自动回退到 legacy 直连兑换流程
- QQ 换取令牌阶段仍保留官方授权页上下文代码，便于后续继续对照官网行为排查问题
- QQ 扫码绑定入口改为先走三角洲官网同款的 QQ Connect authorize 页面，再提取内层 `xlogin`，与官网当前登录层级保持一致
- QQ 扫码绑定链路改为动态解析腾讯当前 `xlogin` 页面配置，并将官方参数贯穿到二维码生成与登录状态轮询
- QQ 换取令牌阶段继续沿用官方登录上下文，并允许在官方域名内跟随中转跳转后再提取授权码
- 放行腾讯当前实际使用的 `ssl.ptlogin2.graph.qq.com` 登录回跳域名，修复扫码确认后仍被误判为登录状态失败的问题
- 文本接口请求增加多编码兜底解码，降低 `df检查` 在上游返回非 UTF-8 文本时出现 `UnicodeDecodeError` 的概率
- 通知路由继续遵循 AstrBot 官方会话规则，敏感提醒只发用户私聊，系统告警优先发给 `admins_id` 管理员私聊
- 物品目录缓存保持每 12 小时自动过期刷新，刷新失败时会沿用旧缓存继续检测并主动发出系统告警

## 功能特性

- 自动轮询最近对局
- QQ 扫码绑定账号
- 本地缓存物品列表
- 按 6 级收集品直接筛选
- 群内自动播报
- 支持取消当前群播报绑定
- 输出简化调试报告

## 核心命令

| 命令 | 参数 | 说明 |
|------|------|------|
| `df绑定` | - | 使用 QQ 扫码绑定账号 |
| `df解绑` | - | 解除绑定 |
| `df设置群` | - | 设置当前群为播报群 |
| `df取消群绑定` | - | 取消当前群的播报绑定 |
| `df状态` | - | 查看绑定状态与播报群数量 |
| `df刷新物品缓存` | - | 强制刷新本地物品缓存 |
| `df检查` | - | 手动检查最近一局 |
| `df检查详细` | - | 生成调试报告 |

## 工作流程

1. 执行 `df绑定`，QQ 扫码完成账号绑定
2. 执行 `df设置群` 设置播报群
3. 后台每 120 秒轮询一次最近战绩
4. 获取本局时间窗内的“撤离带出”物品
5. 与本地缓存目录中的 `item_catalog_cache.json` 对比
6. 命中 `primaryClass=props`、`secondClass=collection`、`grade = 6` 的物品后播报

## 物品缓存

插件会把物品列表缓存到：

- AstrBot 安装环境下：`data/plugin_data/astrbot_plugin_deltaforce_loot_broadcast/item_catalog_cache.json`
- 非 AstrBot 标准目录下：当前工作目录下的 `data/plugin_data/astrbot_plugin_deltaforce_loot_broadcast/item_catalog_cache.json`（旧版 `.runtime_data/` 数据会自动迁移）

说明：

- 首次检测时自动拉取一次完整物品列表
- 后续默认直接使用本地缓存比对，缓存每 12 小时自动尝试刷新一次
- 若自动刷新失败，会先沿用旧缓存继续检测，并向管理员私聊发送一次系统告警
- 如果需要手动更新，执行 `df刷新物品缓存`

## 通知与告警

- 用户私密提醒：绑定失效、凭证解密失败、需要重新绑定等，只发给用户自己的安全私聊目标
- 系统运行告警：连续上游异常、物品目录自动刷新失败等，优先发给 AstrBot 全局配置 `admins_id` 中的管理员私聊
- 群聊交互：`df状态`、`df检查` 等命令仍可在群里正常使用，但不会把敏感绑定状态主动广播到群里

## 运行数据目录

以下文件会优先写入 `data/plugin_data/astrbot_plugin_deltaforce_loot_broadcast/`：

- `df_red_data.json`
- `item_catalog_cache.json`
- `df_red_secret.key`（仅非 Windows 平台，用于本地加密凭证）
- `debug/debug_last_report.txt`
- `debug/debug_last_broadcast.txt`

如果插件从旧版本升级，首次启动时会自动从旧插件名 `astrbot_plugin_df_red` 对应的数据目录迁移绑定与缓存数据。
其中账号凭证不会再以明文写入 `df_red_data.json`：Windows 平台使用系统 DPAPI 保护，其他平台使用本地生成的对称密钥加密保存。

## 更新插件

推荐把插件仓库放到 AstrBot 的 `data/plugins/astrbot_plugin_deltaforce_loot_broadcast/` 下：

1. 在插件目录执行 `git pull`
2. 回到 AstrBot WebUI，执行“重载插件”

这样代码会更新，但绑定信息、缓存和调试文件会继续保留在 `data/plugin_data/astrbot_plugin_deltaforce_loot_broadcast/` 中。

## 收集品判定规则

当前只认定满足以下条件的物品：

- `primaryClass = props`
- `secondClass = collection`
- `grade = 6`

## 调试报告包含内容

- 对局时间 / 房间ID / 撤离状态
- 流水总数与分类统计
- 本局撤离带出物品
- 收集品判定结果
- 最终可播报收集品

## 依赖

```bash
pip install aiohttp cryptography
```

AstrBot 相关依赖由运行环境提供。

## 开发者

XiuYan

本项目基于 vibe coding 制作。
