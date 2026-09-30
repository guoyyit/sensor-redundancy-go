# sensor-redundancy-go

冗余传感器信号表决与故障隔离库。多个独立通道测量同一物理量，库逐采样拍
完成故障检测、健康状态迁移、多数表决/降级、持久化与确定性回放，向下游
输出一条可靠、可解释的信号；故障通道被隔离，恢复后按窗口重新纳入。

## 快速使用

```go
cfg := sensor.DefaultConfig([]sensor.ChannelID{"a", "b", "c"})

// 在线引擎：statePath 非空时每拍原子落盘，崩溃后自动恢复。
eng, err := sensor.NewEngine(cfg, "/var/lib/sensor/state.json")
if err != nil { /* 通道集合与磁盘快照不一致等 */ }
defer eng.Close()

// 多路采集协程可并发写入同一内存管道。
go func() {
    _ = eng.Submit(sensor.Sample{Channel: "a", Seq: 1, Value: 10.0})
}()

for r := range eng.Results() {
    if r.Status == sensor.StatusFailed {
        // 无可靠数值：r.Value 为 NaN，r.Reason 给出原因。
        continue
    }
    _ = r.Value // 表决输出；r.Contributors 为所依赖通道集合
}

// 任一时刻的完整自洽状态（同一采样拍，不会跨通道混合）。
view := eng.Snapshot()

// 阈值/恢复窗口可在线调整；通道集合不可变，计数与状态不重置。
cfg.DriftThreshold = 2.0
_ = eng.UpdateConfig(cfg)

// 离线确定性回放：相同配置+序列 => 逐条一致的输出。
results, err := sensor.Replay(cfg, samples)
```

## 健康状态语义

每条通道处于四种状态之一（`health.go`）：

| 状态 | 是否参与表决 | 语义 |
| --- | --- | --- |
| `healthy` | 是 | 健康 |
| `suspected` | 是 | 连续失配达到 `SuspectAfter`，仍参与表决但被观察；任一匹配立即回到 healthy |
| `faulty` | 否 | 连续失配达到 `FaultAfter`，已隔离 |
| `recovering` | 否 | 隔离后出现匹配，进入恢复观察；连续匹配达 `RecoverAfter` 才回到 healthy |

迁移遵循“连续失配下行、连续匹配上行”：

- 瞬态抖动不会累积到隔离——`healthy/suspected` 下任一匹配立即清零失配计数。
- 恢复窗口内任一失配立即退回 `faulty` 并清零恢复计数。
- 当一拍无法得出任何可靠参考时（全部分歧且无历史良好值），健康计数
  中性保持，不在“无真相”拍改变状态。

故障分类（`detect.go`，仅在与参考失配时才报故障，稳态真值不会被误判卡死）：

- `drift` 漂移：与参考偏差超 `DriftThreshold`，且非跳变、非卡死；
- `jump` 跳变：相对上一拍读数变化超 `JumpThreshold`；
- `stuck` 卡死：最近 `StuckWindow` 个历史读数之差均在 `StuckEpsilon` 内，
  但当前值与参考失配。

## 表决与降级规则

表决在“当前可用通道”（healthy + suspected）上进行，输出带
`Status`、`Contributors` 与 `Reason`，输出与其依赖通道集合始终一致：

- **四余度 / ≥3 通道**：取最紧团簇（簇内极差 ≤ `DriftThreshold`），
  成员均值为输出；优先更大团簇，其次更紧，平局由配置通道顺序打破，
  保证确定可复现。3:1 时多数团簇胜出，单故障无法拖偏输出；2:2 时
  确定性选择配置顺序更靠前的团簇。
- **三余度**：等价于团簇规则；两两一致时取一致对均值，第三个通道被排除。
- **双余度**：两通道一致取均值；分歧时无第三方可仲裁，直接判 `failed`
  （不会猜测哪一路正确，也不输出错误数值）。
- **降级**：可用通道少于配置余度时标 `degraded`；仅剩两路且一致时
  输出均值。可用数低于 `MinHealthy`（默认 2）或没有任何互相一致的
  通道对时输出 `failed`，值为 `NaN`（明确的失效状态，而非错误数值）。

失效拍内部仍尝试用“当前多数共识”以及“最近一次成功表决参考”驱动
故障隔离，使持续偏离已知良好值的通道被隔离、系统有机会降级恢复输出；
这些内部参考永不写入对外结果。

## 并发一致性

- 多个生产者可并发 `Submit`；内部仅一个后台 goroutine 串行推进状态。
- 帧按 `Seq` 组装：只有当每通道“已有该拍读数”或“已确认越过该序号
  （整拍缺数）”时才关闭，正在管道中并发投递的正常样本不会被误判缺失；
  迟到的低序号读数一律丢弃，杜绝跨通道、跨采样时刻混合。
- `Snapshot()` 读到的状态必然对应某个已完整处理完的采样拍。

## 崩溃恢复

每拍处理完成后原子写入快照（临时文件 + `rename`），内容含：采样序号、
配置、各通道状态与连续计数、每通道有界历史（上限 `HistorySize`）、
最近可靠参考。重启后故障通道仍为 `faulty`，必须重新走完恢复窗口才会
纳入；磁盘快照与运行配置的通道集合不一致时启动报错，绝不静默重纳。

## 配置项（`config.go`）

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `Channels` | 必填 | 参与表决的通道集合，顺序决定确定性平局选择 |
| `DriftThreshold` | 1.0 | 与参考的最大允许偏差，也是团簇一致性阈值 |
| `JumpThreshold` | 5.0 | 相邻读数允许的最大跳变 |
| `StuckEpsilon` | 1e-9 | 判定读数不变的容差 |
| `StuckWindow` | 5 | 判定卡死所需连续不变样本数 |
| `SuspectAfter` | 2 | 连续失配多少次进入疑似 |
| `FaultAfter` | 3 | 连续失配多少次隔离（须 ≥ SuspectAfter） |
| `RecoverAfter` | 3 | 隔离后连续匹配多少次重新纳入 |
| `MinHealthy` | 2 | 允许输出的最少可用通道数 |
| `HistorySize` | 16 | 每通道有界历史长度（须 ≥ StuckWindow+1） |

`UpdateConfig` 可在线修改阈值与窗口：参数下一拍生效，既有的健康状态
与连续计数原样保留，因此修改前后输出不会出现无解释跳变；通道集合不允许
运行期变更。

## 确定性复现

`Replay` 不接触墙钟、goroutine 与文件：帧按 `Seq` 分组升序处理，
帧内按配置通道顺序推进健康状态。相同采样序列与配置下，状态迁移与
表决输出逐条一致，可用于回放并复现故障隔离过程。
