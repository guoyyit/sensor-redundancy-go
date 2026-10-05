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

// 停机排空对账：接受数、丢弃数、处理数、产出数。
stats := eng.Stats()

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
- **同一采样时刻自洽**：采样序号、表决结果与各通道健康状态在同一把锁内
  更新，`Snapshot()` 读到的三元组必然完整属于同一个已完成的采样拍，
  持续写入与并发读取交错也不会出现相邻两拍的混合视图；读取不阻塞写入。
- 帧按 `Seq` 组装：只有当每通道“已有该拍读数”或“已确认越过该序号
  （整拍缺数）”时才关闭，正在管道中并发投递的正常样本不会被误判缺失。
- **封闭时刻凝固**：已产出表决结果的采样时刻视为封闭，之后到达的迟到、
  重复、乱序读数一律确定性丢弃，不会再次产出该时刻的结果，也不会改写
  已发布的输出；同一时刻同一通道重复上报时只有首次接收的读数生效。
  已隔离通道的恢复因此只依据新到达的连续采样时刻推进，重复投递旧读数
  不计入连续匹配。
- **有界等待**：待处理帧跨度（最新已见序号 − 最旧待处理序号）超过
  `MaxPendingFrames` 时，最旧帧按当时实际到位的通道集合确定性结算
  （输出为 `degraded`/`failed`，`Contributors` 即实际到位集合）。
  单通道长期缺报不会无限积压，推进规则只依赖采样序号，不依赖墙钟
  或到达时延，相同输入下可复现。

## 停机排空与对账

`Close()` 的语义：

1. 停止接受新样本：之后（以及停机过程中）的 `Submit` 要么已被完整接受
   并处理，要么收到明确的 `engine closed` 错误，绝不静默丢弃。
2. 所有已接受样本全部处理完毕：输入管道排空后，收集器中仍缓冲的帧
   逐帧结算（按实际到位通道输出），一个不漏。
3. 最终状态完整落盘后 `Close` 才返回；落盘错误会作为返回值上报。

`Stats()` 提供对账计数，不变式：

```
Accepted == Dropped + SamplesProcessed   // 接受的样本要么被丢弃（迟到/重复），要么参与表决
FramesProcessed == 产出的表决结果数
结果通道实际投递数 == FramesProcessed - ResultsDropped
```

结果通道缓冲有界（256）；消费者过慢时丢弃最旧结果并计入
`ResultsDropped`，写入侧永不阻塞，权威状态以 `Snapshot()`/落盘快照为准。

正常停机后重启：从最后对外可见的采样时刻继续衔接——已封闭时刻的重复
样本被确定性丢弃（不重复处理），未处理的样本不跳过，`Snapshot()` 与
停机前最后一次对外可见状态（序号、结果、健康状态）完全一致。

## 崩溃恢复

每拍处理完成后原子写入快照（临时文件 + `rename`），内容含：采样序号、
配置、各通道状态与连续计数、每通道有界历史（上限 `HistorySize`）、
最近可靠参考与最后一次表决结果。重启后故障通道仍为 `faulty`，必须重新
走完恢复窗口才会纳入；磁盘快照与运行配置的通道集合不一致时启动报错，
绝不静默重纳。

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
| `MaxPendingFrames` | 64 | 最大待处理帧跨度；0 表示取默认值，超限时最旧帧按实际到位通道确定性结算 |

`UpdateConfig` 可在线修改阈值与窗口：参数下一拍生效，既有的健康状态
与连续计数原样保留，因此修改前后输出不会出现无解释跳变；通道集合不允许
运行期变更。

## 确定性复现

`Replay` 不接触墙钟、goroutine 与文件：帧按 `Seq` 分组升序处理，
帧内按配置通道顺序推进健康状态；同一时刻同一通道的重复样本与在线
一样只取首次读数。相同采样序列与相同配置下，在线运行、停机排空与
离线回放给出逐条一致的表决结果与状态迁移；相同输入重复执行多次，
结果完全一致。

## 本地验证

```sh
# 全部单元测试与场景测试
go test ./...

# 并发读写一致性（数据竞争检测）与多次重复运行稳定性
go test -race -count=10 ./...
```

关键验收用例：

- `TestConcurrentReadWriteAtomicSnapshot`：并发读写下 Snapshot 同拍自洽；
- `TestLateDuplicateOutOfOrderDropped`：迟到/重复/乱序样本确定性丢弃；
- `TestIsolatedChannelDuplicateOldReadingsIgnored`：重复旧读数不推进恢复；
- `TestShutdownDrainsAllAccepted` / `TestSubmitDuringCloseIsDecisive`：
  停机排空、接受/拒绝明确、计数对账；
- `TestRestartContinuesFromLastVisible`：停机重启后与最后可见状态一致；
- `TestOnlineDrainReplayConsistency`：在线/排空/回放逐条一致；
- `TestSlowConsumerBounded` / `TestStalledChannelForcesDeterministicProgress`：
  慢消费者不阻塞写入、内存有界、单通道缺报确定性推进。
