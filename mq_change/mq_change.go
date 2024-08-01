package mq_change

import (
	"context"
	"fmt"

	"code.byted.org/gopkg/jsonx"
)

type MqChange struct {
	Name            string
	StageOldManager *DiffMangager
	StageMixManager *DiffMangager
}

//diff的数量小于5，并且old阶段的请求数量大于10000，可以切换到mix阶段
func (m *MqChange) completeStageOld() bool {
	if m.StageOldManager.DiffNum <= 1 &&
		m.StageOldManager.OldNum >= 5 &&
		m.StageOldManager.NewNum >= 5 {
		return true
	}
	return false
}

//切换到mix阶段后，old阶段的所有旧请求都发送完了，此时可以切换到新链路
func (m *MqChange) completeStageMix() bool {
	return m.StageOldManager.OldNum >= m.StageOldManager.NewNum
}

func (m *MqChange) Handle(ctx context.Context, r Req) (Req, bool) {
	if !m.completeStageOld() {
		return m.stageOldLink(ctx, r)
	} else {
		if m.completeStageMix() { //人工确认，走新链路
			return m.stageNewLink(ctx, r)
		}
		return m.stageMixLink(ctx, r)
	}
}

//走老链路，新旧链路diff,只输出老链路
func (m *MqChange) stageOldLink(ctx context.Context, r Req) (Req, bool) {
	m.StageOldManager.diff(ctx, r)
	if r.IsOriginal() {
		m.StageOldManager.OldNum++
		return r, true
	}
	m.StageOldManager.NewNum++
	return nil, false
}

//新旧链路谁先到走哪个
func (m *MqChange) stageMixLink(ctx context.Context, r Req) (Req, bool) {
	if !r.IsOriginal() {
		if m.StageOldManager.isExist(ctx, r) { //new<old，新链路都干掉
			return nil, false
		}
		if m.StageMixManager.isExist(ctx, r) {
			return nil, false
		}
		return r, true
	}
	if m.StageOldManager.isExist(ctx, r) { //oldStage时候new>old，此时old正在追平new
		m.StageOldManager.OldNum++
	}
	//old<new的时候，旧链路需要追平第一阶段，之后第二阶段谁来的早消费谁，晚的就不消费了
	if m.StageMixManager.isExist(ctx, r) {
		return nil, false
	}
	return r, true
}

//走新链路
func (m *MqChange) stageNewLink(ctx context.Context, r Req) (Req, bool) {
	if !r.IsOriginal() {
		if m.StageMixManager.isExist(ctx, r) { //二阶段old链路已经发送过了，不需要重复发送
			return nil, false
		}
		return r, true
	}
	return nil, false
}

func (m *MqChange) Print() {
	fmt.Printf("mqchange :%v\n", jsonx.ToString(m))
}
