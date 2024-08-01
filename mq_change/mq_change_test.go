package mq_change

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestMiniReids(t *testing.T) {
	s, _ := miniredis.Run()
	r := redis.NewClient(&redis.Options{
		Addr: s.Addr(), // mock redis server的地址
	})
	ctx := context.Background()
	err1 := r.SetNX(ctx, "test", "1", 12*time.Hour).Err()
	assert.Equal(t, err1, nil)

	v, _ := r.Get(ctx, "test").Result()
	assert.Equal(t, v, "1")

	err2 := r.SetNX(ctx, "test", "1", 12*time.Hour).Err()
	assert.Equal(t, err2, nil)
}

func TestIsExist(t *testing.T) {
	ctx := context.Background()
	s, _ := miniredis.Run()
	r := redis.NewClient(&redis.Options{
		Addr: s.Addr(), // mock redis server的地址
	})
	dm := &DiffMangager{
		Name:  "test",
		Redis: r,
	}
	dreq := DiffReq{
		v: 1,
	}
	f1 := dm.isExist(ctx, &dreq)
	assert.Equal(t, f1, false)
	f2 := dm.isExist(ctx, &dreq)
	assert.Equal(t, f2, true)
}

type DiffReq struct {
	v          int
	isOriginal bool
}

func (d *DiffReq) IsOriginal() bool {
	return d.isOriginal
}
func (d *DiffReq) Key() string {
	return fmt.Sprintf("%d", d.v)
}
func (d *DiffReq) DiffVal() string {
	return fmt.Sprintf("%d", d.v)
}
