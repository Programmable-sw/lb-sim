# 仓库文件管理

唯一主实验入口为 `experiments/pretrim/`，GitHub 使用根目录 README。

| 类别 | 处理方式 |
|---|---|
| htsim 核心、交换机/NIC/LB/CC 源码、Makefile、测试 | 纳入 Git |
| pretrim 源码、常规 Victim/Hybrid 负载、runner、微型测试输入 | 纳入 Git |
| 旧阶段源码、脚本、报告及特殊实验 | 从主目录和提交范围移出，本地 `.local-archive/legacy-pretrim/` 可恢复归档 |
| 实验 results、references、baselines | 原地保留，本地忽略；已有跟踪文件不自动移除 |
| 导出包 V7_READBACK、output_metrics、构建产物、缓存 | 本地忽略 |
| 无法确认用途的临时文件 | 保留待检查，不自动删除 |

`.gitignore` 仅控制未跟踪文件，不会取消已有跟踪，也不会清除 Git 历史中的大对象。
忽略不表示数据无用。按用户要求清理退役模式时，删除范围和保留结果记录于
[本地清理记录](LOCAL_CLEANUP.md)。不重写 Git 历史，不自动提交或推送。

整理时将根目录误保存的终端输出 `tore --staged .` 移至
`.local-archive/terminal-captures/accidental-git-diff.txt`，可随时移回，未删除文件。
旧 `sim/datacenter` 的 pretrim 版本入口已归档，Makefile 的 `htsim_pfc_pretrim`
统一调用主入口构建脚本，不再编译旧版本。历史结果未删除，Git 提交历史未重写。
主程序为 `experiments/pretrim/src/main.cpp`，构建产物为
`experiments/pretrim/.build/htsim_pretrim`。旧版本标识仅在历史 provenance/事件身份中
保留，不作为构建入口；没有改动事件字段，方便与原有回归 fixture 比较。
迁移后编译成功、14 项测试通过，14 个共享内核文件经过重命名归一化后与原源码一致。
`.git` 当时约 53 GiB；该空间属于 Git 对象与历史，不能通过忽略规则回收。

后续提交前可执行：

```bash
git add --dry-run .
git diff --cached --stat
```

注意：`git add --dry-run` 只预览，不暂存；新目录里的未来文件仍需遵循此分类。
