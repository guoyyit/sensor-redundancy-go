package sensor

import "testing"

func TestHealthTransientGlitchDoesNotIsolate(t *testing.T) {
	cfg := DefaultConfig([]ChannelID{"a"})
	st := newChannelStatus()

	// 默认 SuspectAfter=2, FaultAfter=3：单次失配只应累计，一次匹配即清零。
	st.observe(false, cfg)
	if st.State != StateHealthy || st.MismatchStreak != 1 {
		t.Fatalf("after 1 mismatch: %+v", st)
	}
	st.observe(true, cfg)
	if st.State != StateHealthy || st.MismatchStreak != 0 {
		t.Fatalf("after recovery match: %+v", st)
	}

	// 两个隔离阈值之间反复出现匹配：永不进入 faulty。
	for i := 0; i < 10; i++ {
		st.observe(false, cfg)
		st.observe(true, cfg)
	}
	if st.State != StateHealthy {
		t.Fatalf("transient glitches accumulated into %s", st.State)
	}
}

func TestHealthSustainedMismatchIsolates(t *testing.T) {
	cfg := DefaultConfig([]ChannelID{"a"})
	st := newChannelStatus()

	tr1 := st.observe(false, cfg)
	tr2 := st.observe(false, cfg)
	tr3 := st.observe(false, cfg)

	if tr1.To != StateHealthy {
		t.Fatalf("first mismatch changed state: %+v", tr1)
	}
	if tr2.To != StateSuspected {
		t.Fatalf("second mismatch: to=%s want suspected", tr2.To)
	}
	if tr3.To != StateFaulty {
		t.Fatalf("third mismatch: to=%s want faulty", tr3.To)
	}
}

func TestHealthRecoverWindow(t *testing.T) {
	cfg := DefaultConfig([]ChannelID{"a"}) // RecoverAfter=3
	st := ChannelStatus{State: StateFaulty}

	tr := st.observe(true, cfg)
	if tr.To != StateRecovering {
		t.Fatalf("first match: to=%s want recovering", tr.To)
	}
	st.observe(true, cfg)
	back := st.observe(true, cfg)
	if back.To != StateHealthy {
		t.Fatalf("third match: to=%s want healthy", back.To)
	}

	// 恢复窗口内一次失配应立即重新隔离。
	st = ChannelStatus{State: StateRecovering, MatchStreak: 2}
	re := st.observe(false, cfg)
	if re.To != StateFaulty {
		t.Fatalf("mismatch in recovery: to=%s want faulty", re.To)
	}
}

func TestDetectDriftJumpStuck(t *testing.T) {
	cfg := DefaultConfig([]ChannelID{"a"})
	d := newDetector(cfg)

	// 一致
	if det := d.check(Sample{Value: 10.5}, 10.0, nil); !det.Matched || det.Kind != FaultNone {
		t.Fatalf("within threshold detected as %v", det)
	}

	// 漂移（偏差大但相对上一拍变化平缓）
	prior := []Sample{{Value: 13.9}, {Value: 14.0}}
	if det := d.check(Sample{Value: 14.0}, 10.0, prior); det.Kind != FaultDrift {
		t.Fatalf("want drift, got %v", det)
	}

	// 跳变（相对上一拍变化超过 JumpThreshold）
	if det := d.check(Sample{Value: 30.0}, 10.0, []Sample{{Value: 10.0}}); det.Kind != FaultJump {
		t.Fatalf("want jump, got %v", det)
	}

	// 卡死（连续 StuckWindow 个历史读数等于当前值，但与参考失配）
	stuckPrior := []Sample{{Value: 5.0}, {Value: 5.0}, {Value: 5.0}, {Value: 5.0}, {Value: 5.0}}
	if det := d.check(Sample{Value: 5.0}, 10.0, stuckPrior); det.Kind != FaultStuck {
		t.Fatalf("want stuck, got %v", det)
	}

	// 稳态真实信号（与参考一致）不得误判卡死
	if det := d.check(Sample{Value: 10.0}, 10.0, stuckPrior); !det.Matched {
		t.Fatalf("steady in-range value misdetected: %v", det)
	}
}
