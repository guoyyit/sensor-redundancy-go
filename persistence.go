package sensor

import (
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
)

// snapshot 是需要持久化的最小状态：健康状态、计数、有界历史与
// 最后一次对外可见的表决结果。
// 配置同时落盘：恢复时若与运行配置冲突会显式报错，而非静默重纳故障通道。
type snapshot struct {
	Seq         int64                       `json:"seq"`
	Config      Config                      `json:"config"`
	Statuses    map[ChannelID]ChannelStatus `json:"statuses"`
	History     map[ChannelID][]Sample      `json:"history"`
	LastGoodRef float64                     `json:"last_good_ref"`
	HasGoodRef  bool                        `json:"has_good_ref"`
	LastResult  voteResultSnapshot          `json:"last_result"`
}

// voteResultSnapshot 是 VoteResult 的 JSON 安全表示：
// 失效结果的 Value 为 NaN，无法直接编码为 JSON，用 HasValue 显式标记。
type voteResultSnapshot struct {
	Seq          int64       `json:"seq"`
	Value        float64     `json:"value"`
	HasValue     bool        `json:"has_value"`
	Status       VoteStatus  `json:"status"`
	Contributors []ChannelID `json:"contributors,omitempty"`
	Reason       string      `json:"reason,omitempty"`
}

func voteResultToSnapshot(r VoteResult) voteResultSnapshot {
	v := voteResultSnapshot{
		Seq:          r.Seq,
		Value:        r.Value,
		HasValue:     true,
		Status:       r.Status,
		Contributors: r.Contributors,
		Reason:       r.Reason,
	}
	if math.IsNaN(r.Value) {
		v.Value = 0
		v.HasValue = false
	}
	return v
}

func (v voteResultSnapshot) toVoteResult() VoteResult {
	if v.Status == "" {
		// 旧版本快照没有该字段：视为“尚无结果”，与全新引擎一致。
		return VoteResult{}
	}
	r := VoteResult{
		Seq:          v.Seq,
		Value:        v.Value,
		Status:       v.Status,
		Contributors: v.Contributors,
		Reason:       v.Reason,
	}
	if !v.HasValue {
		r.Value = math.NaN()
	}
	return r
}

// persist 将快照原子写入 path：先写临时文件再 rename，
// 崩溃发生在写入中途也不会留下半截状态文件。
func persist(path string, snap snapshot) error {
	if path == "" {
		return nil
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".sensor-state-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// loadSnapshot 读取快照；文件不存在时返回空快照与 ok=false。
func loadSnapshot(path string) (snapshot, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return snapshot{}, false, nil
		}
		return snapshot{}, false, err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return snapshot{}, false, err
	}
	return snap, true, nil
}
