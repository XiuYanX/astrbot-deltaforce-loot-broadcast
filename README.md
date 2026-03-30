# AstrBot 三角洲物资播报插件 (astrbot_plugin_deltaforce_loot_broadcast)

一个用于 AstrBot 的三角洲行动插件，用于监测玩家最近对局的撤离带出物品，并在命中高价值收集品时进行群播报。
插件运行数据会优先写入 AstrBot 的 `data/plugin_data/astrbot_plugin_deltaforce_loot_broadcast/`，便于直接 `git pull` 后重载插件，而不必卸载重装。

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
- 非 AstrBot 标准目录下：插件目录内的 `.runtime_data/item_catalog_cache.json`

说明：

- 首次检测时自动拉取一次完整物品列表
- 后续直接使用本地缓存比对
- 如果需要手动更新，执行 `df刷新物品缓存`

## 运行数据目录

以下文件会优先写入 `data/plugin_data/astrbot_plugin_deltaforce_loot_broadcast/`：

- `df_red_data.json`
- `item_catalog_cache.json`
- `debug/debug_last_report.txt`
- `debug/debug_last_broadcast.txt`

如果插件从旧版本升级，首次启动时会自动从旧插件名 `astrbot_plugin_df_red` 对应的数据目录迁移绑定与缓存数据。

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
pip install aiohttp
```

AstrBot 相关依赖由运行环境提供。

## 开发者

XiuYan

本项目基于 vibe coding 制作。
