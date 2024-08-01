package mq_change

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type DiffMangager struct {
	Name    string
	Redis   *redis.Client
	OldNum  int //请求的数量
	NewNum  int //请求的数量
	DiffNum int //新旧请求diff的数量
}

//新旧请求diff
func (m *DiffMangager) diff(ctx context.Context, r Req) {
	key := r.Key() + m.Name
	v, err := m.Redis.Get(ctx, key).Result()
	if err == nil {
		if v != r.DiffVal() {
			m.DiffNum++
		}
		return
	}
	m.Redis.Set(ctx, key, r.DiffVal(), time.Hour*12)
}

//请求是否已经发送过了
func (m *DiffMangager) isExist(ctx context.Context, r Req) bool {
	key := r.Key() + m.Name
	v, err := m.Redis.Get(ctx, key).Result()
	if err == nil {
		if v == r.DiffVal() {
			return true
		}
	}
	m.Redis.Set(ctx, key, r.DiffVal(), time.Hour*12)
	return false
}
