# sensor-redundancy-go

冗余传感器信号的表决与故障隔离。同一物理量由 2/3/4 个独立通道同时测量，
系统对每一路采样做表决输出，按连续失配/匹配次数管理通道健康状态，
隔离故障通道并按窗口恢复，状态持久化以支持崩溃恢复，行为完全可复现。

## 核心概念

### 采样轮次（Round）

每个通道的采样携带逻辑序号 `Seq`。当所有已配置通道都交付了同一 `Seq`
的采样时，构成一个**轮次**并立即表决。读取方在任意时刻拿到的
`Snapshot` 都是某一个完整轮次的自洽状态，绝不跨轮次混合。
迟到且已无法对齐的采样被丢弃；轮次对齐只依赖 `Seq`，与到达顺序无关。

### 健康状态语义

每个通道处于四种状态之一，迁移只由**连续**失配/匹配次数驱动：

| 状态 | 含义 | 参与表决 |
| --- | --- | --- |
| `healthy` | 无连续失配 | 是 |
| `suspect` | 连续失配次数在 `[1, MismatchLimit)` | 是 |
| `faulty` | 连续失配达到 `MismatchLimit`，被隔离 | 否 |
| `recovering` | 已隔离通道重新连续匹配，累计中 | 否 |

迁移规则（每轮评估一次）：

- 匹配：`healthy` 保持；`suspect` → `healthy`；`faulty` → `recovering`；
  `recovering` 中连续匹配达到 `RecoverWindow` → `healthy`（重新纳入表决）。
- 失配：`healthy` → `suspect`；`suspect` 连续失配达到 `MismatchLimit`
  → `faulty`；`recovering` 中失配 → 立即回到 `faulty`（恢复窗口重新计）。

单次匹配即清零失配计数，因此瞬态抖动（单轮毛刺）不会触发隔离。

### 匹配判定与参考值

每轮先由参与通道（`healthy` + `suspect`）计算表决值，再以此判定各
通道匹配与否：`|sample - reference| <= Threshold` 为匹配。参考值
`reference` 的选取：

1. 本轮表决有效时，取本轮表决值；
2. 表决无效（参与方分歧无法裁决）时，回退到**最近一次有效表决值**，
   使系统仍能隔离偏离既有输出的通道（例如双余度下一路跳变），而不是
   永远停在失效输出；
3. 尚无任何有效表决时，本轮不做健康评估（计数保持不变）。

### 表决规则

对参与通道的采样，按确定性规则选取**最大一致集**：值域跨度不超过
`Threshold` 的最大通道集合；并列时取跨度更小者，再并列取序号较小者。
输出为该集合的均值，`Contributors` 列出集合成员（升序），输出始终
可解释为它恰好依赖的通道集合。

- 四余度：1 路故障时输出其余 3 路均值；2+2 分裂时降级为更紧的一对。
- 三余度：1 路故障时输出其余 2 路均值。
- 双余度：两路一致时输出均值；分歧时本轮输出失效，由健康评估依据
  最近有效输出隔离故障路，随后降级为单路输出。
- 降级：参与集合小于配置余度时 `Degraded = true`；仅剩 1 路且
  `MinVoters = 1` 时输出该路值并标记降级。
- 失效：无参与通道、或多个参与方无任何一致对、或可用数低于
  `MinVoters` 时，`Valid = false`，不输出任何数值。

### 崩溃恢复

每个轮次结束后，通道健康状态、连续失配/匹配计数、有界的近期采样
历史（`HistorySize`）、最近有效表决值与下一个待处理序号原子写入
`FileStore`（临时文件 + rename，崩溃不会留下半写文件）。重启后经
`NewMonitor` 自动恢复：已隔离通道不会被错误重新纳入，轮次序号与
崩溃前衔接，不会重复评估同一轮。

### 确定性复现

全部状态迁移只取决于采样序列与配置，不依赖时钟与调度。`Replay`
同步重放历史采样，返回每一轮的 `Snapshot`；相同输入必然产生逐条
一致的输出，可用于复现故障隔离过程。

### 运行期配置调整

`UpdateConfig` 可在线修改 `Threshold` 与 `RecoverWindow`。修改由采集
循环在轮次边界生效：每个轮次始终使用同一份完整配置评估，不会出现
评估到一半换配置的情况；在途的连续匹配计数保留，仅目标窗口变化。
非法值（非正阈值、窗口 < 1）被忽略。

## 配置项

| 字段 | 含义 | 约束 |
| --- | --- | --- |
| `Redundancy` | 通道数（余度） | 2、3 或 4 |
| `Threshold` | 一致集最大跨度 / 匹配判定阈值 | > 0，可在线改 |
| `MismatchLimit` | 隔离所需连续失配次数 | >= 1 |
| `RecoverWindow` | 恢复所需连续匹配次数 | >= 1，可在线改 |
| `MinVoters` | 输出有效所需最小表决集合 | [1, Redundancy]，缺省 1 |
| `HistorySize` | 每通道持久化的近期采样数上限 | >= 0（0 表示不保留） |

## 使用示例

```go
cfg := redundancy.Config{
    Redundancy:    3,
    Threshold:     1.0,
    MismatchLimit: 3,
    RecoverWindow: 2,
    HistorySize:   16,
}
m, err := redundancy.NewMonitor(cfg, redundancy.NewFileStore("state.json"))
if err != nil { /* 配置非法 */ }
defer m.Close()

// 每路传感器一个内存管道，可并发写入。
pipe := m.OpenPipe(0)
pipe.Write(redundancy.Sample{Seq: 42, Value: 10.1})

// 任意时刻读取同一采样时刻的自洽状态。
snap := m.Snapshot()
if snap.Vote.Valid {
    _ = snap.Vote.Value // 表决输出
}

// 在线调整阈值与恢复窗口。
threshold := 0.5
m.UpdateConfig(redundancy.ConfigUpdate{Threshold: &threshold})

// 确定性回放。
snaps := redundancy.Replay(cfg, recordedSamples)
```

## 测试

```sh
go test -race ./...
```

覆盖：表决规则（含并列裁决）、漂移/跳变/卡死隔离、瞬态抖动不隔离、
逐级降级与全失效、恢复窗口与恢复中复发、崩溃重启状态衔接、持久化
有界性、确定性回放与到达顺序无关性、并发写入下的一致读取、运行期
配置调整。
