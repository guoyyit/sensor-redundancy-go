package sensor

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// snapshot 是需要持久化的最小状态：健康状态、计数与有界历史。
// 配置同时落盘：恢复时若与运行配置冲突会显式报错，而非静默重纳故障通道。
type snapshot struct {
	Seq         int64                       `json:"seq"`
	Config      Config                      `json:"config"`
	Statuses    map[ChannelID]ChannelStatus `json:"statuses"`
	History     map[ChannelID][]Sample      `json:"history"`
	LastGoodRef float64                     `json:"last_good_ref"`
	HasGoodRef  bool                        `json:"has_good_ref"`
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
