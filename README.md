# maa-fleet-updater

![CI](https://github.com/ModerRAS/maa-fleet-updater/actions/workflows/ci.yml/badge.svg)

Multi-instance MaaAssistantArknights fleet updater and unified launcher.
Rust daemon with an embedded WebUI. Chinese docs below.

多开 MAA 的集中更新 + 统一拉起工具：Rust 常驻进程 + 内嵌 WebUI。
只做下载、分发、注册三件事，真正的安装仍然交给各实例自带的 MAA.Updater.exe。

## 为什么做这个

MAA 的每个实例目录都是独立的：各自检查更新、各自从网上下载、各自重启。
多开 N 个实例 = N 次下载 + N 次等待。 residential 网络下 GitHub 直链本来
就不稳，MirrorChyan 则按 CDK 计量，多开等于重复消耗。

本工具把流程拆成两段：白天 MAA 全开着的时候只做准备（查版本、下载包、
资源 git pull，一份下载全场共用）；每天机器重启的开机窗口做提交（纯本地
文件操作，零网络请求），然后逐个拉起 MAA，由它们自己的更新器完成安装。

## 工作原理

复用 MAA 原生更新机制，不复刻安装逻辑：

    准备阶段（可联网）
      版本元数据 api.maa.plus  ->  按通道分组（stable/beta/alpha 跟随各实例自身配置）
      应用包走 GitHub browser_download_url 直链（可配代理），OTA 优先、完整包兜底
      中央 MaaResource 浅克隆 git pull 一次
      全部落盘到 staging/，写入 plan.json

    提交阶段（零网络请求，开机窗口执行）
      未运行的实例：资源 merge（version.json 最后复制）
      更新包拷贝进实例目录，写 gui.new.json 的 Update.Name/UpdatePackage
      删除 pending-update-failure.txt，接管各实例开机自启
      逐个拉起 MAA.exe；拉起是最后一步，之前任何失败都不阻塞拉起

MAA 启动时发现 pending 包，走自己的 TryApplyPendingUpdatePackage +
MAA.Updater.exe 完成安装，和手动点更新是同一条代码路径。

## 功能

- 实例自动发现：在 search_roots 下递归找含 MAA.exe + resource/version.json 的目录
- 版本读取：解析各 MAA.exe 的 PE 版本资源（CI 盖戳的 InformationalVersion），运行中读取也安全
- OTA 默认：MAAComponent-OTA-当前-最新 精确匹配；版本掉队/无 OTA/上次失败时自动降级完整包
- 代理：同一份配置同时喂给 reqwest 和 git，支持 http/https/socks5
- 开机自启接管：删除 HKCU Run 下指向已发现实例的 MAA_* 项（幂等，只删自己人）
- 配置安全：gui.new.json 原子替换（tmp + 改名）+ JSON 回环校验 + .fleetbak 备份；运行中的实例绝不写入
- WebUI：实例表、准备/提交/拉起按钮、SSE 实时日志；只绑 127.0.0.1，可配 token

## 快速开始

1. 复制 config.example.json 为 config.json，按实际填写：

    search_roots  实例所在的大目录列表（自动发现）
    proxy         代理地址，空字符串 = 直连
    staging_dir   下载和计划落盘位置

2. 开机任务（二选一）：

    常驻模式：  schtasks /create /tn MAA-Fleet /sc onlogon /delay 0000:10
                /tr "maa-fleet-updater.exe --config config.json 路径"
    纯 stager： 同上，命令末尾再加 --commit-once（提交+拉起后退出，不开 WebUI）
    查日志（免开 WebUI）： maa-fleet-updater.exe --config config.json 路径 --logs [--logs-tail 200] [--logs-level error]

3. 首次运行建议先在 WebUI 点一次准备，确认计划无误；之后每天的自动准备
   由 auto_prepare_hours（默认 6 小时）完成，重启即自动提交+拉起。

## 配置项

见 config.example.json，全部字段都有默认值。常用：

    commit_on_startup   启动时自动提交+拉起（默认开）
    takeover_autostart  接管实例开机自启（默认开）
    launch_stagger_secs 拉起间隔秒数，错开 IO（默认 5）
    max_depth           搜索深度（默认 4）
    auth_token          非空时 API 需要 x-token 头，events 用 ?token=

## 项目结构

    src/main.rs       axum 服务、开机序列、后台自动准备
    src/discover.rs   实例发现（纯本地读）
    src/pe_version.rs PE 版本资源解析
    src/maa_api.rs    版本检查与包挑选（仅准备阶段联网）
    src/downloader.rs GitHub 直链下载（仅准备阶段）
    src/resource.rs   中央仓库 git pull + merge（提交阶段只做本地 merge）
    src/stage.rs      提交+拉起+自启接管（零网络）
    web/index.html    内嵌 WebUI（零构建）

## 当前状态

可用，但以下场景只在替身实例上验证过，真机首次使用建议手动确认：
live 版本检查与大包下载、中央仓库首次克隆、真机重启后的 Updater 接管。
Debug/本地构建版本（version 读不出）会被跳过应用更新，只同步资源。

## 本程序自更新

常驻进程每 `self_update_hours` 小时（默认 6，0=关）查一次版本：优先读
Release 附件里的静态 `latest.json`（走附件 CDN，不占 GitHub API 配额），
拿不到才回退 `api.github.com`（过渡期旧 release 没有 manifest 时靠它）。
`self_update_feed` 为空即官方源，可填自建同结构 JSON 做测试（自建源
只认自己，不会回退官方）。判定规则
跟 arknights-auto-update 一致：远端语义版本更高就更；版本相同但两边提
交号都已知且不同（tag 被移动指向）也更；从不降级；没有可对照的提交
号时不重复下载。

命中更新后只下载当平台那一个 `maa-fleet-updater-win-x64.zip` 及其 `.sha256`，
校验通过才解出其中的 `maa-fleet-updater.exe`（zip-slip/绝对路径拒绝，
MZ 魔数检查），暂存为 exe 旁的 `.new`，再起替换子进程：等原进程退出
→ Windows 下把运行中的 exe 改名 `.old` 让位 → `.new` 转正 → 按原 `--config`
重启。`--commit-once` 模式不检查；`target/` 下的开发构建只提示不自动换
（页面上仍可手动"更新并重启"）。配置页的三个新字段即时生效。

## Releases

Push a tag like v0.1.0 and the release workflow builds the Windows x64
binary and attaches maa-fleet-updater-win-x64.zip (+ .sha256) to the GitHub
release. The build bakes the commit into `MAA_FLEET_GIT_SHA` so moved tags
are still detected as updates.

## License

GPLv3, see LICENSE.