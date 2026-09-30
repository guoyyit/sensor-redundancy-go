package sensor

import "time"

// ChannelID 标识一条独立传感器通道。
type ChannelID string

// Sample 是某条通道在某个采样序号下的一次读数。
// Seq 为采样时刻（拍），同一 Seq 的多通道读数构成一帧。
type Sample struct {
	Channel ChannelID
	Seq     int64
	At      time.Time
	Value   float64
}
