package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"mq.change/mq_change"
)

var mqChange mq_change.MqChange
var mq = make(chan int, 10)
var newMq = make(chan int, 10)
var resCh = make(chan string, 20)

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

func producer() {
	for i := 0; i < 20; i++ {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)+50))
		mq <- i
	}
	close(mq)
}
func newProducer() {
	for i := 0; i < 20; i++ {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)+10))
		newMq <- i*i + 1
	}
	close(newMq)
}

func process() (int, bool) {
	v, ok := <-mq
	return v * v, ok
}

func newProcess() (int, bool) {
	v, ok := <-newMq
	return v - 1, ok
}

func handle(ctx context.Context) {
	go producer() //生产消息
	go func() {   //消费消息
		for {
			v, ok := process() //旧逻辑
			if !ok {
				break
			}
			// resCh <- v
			diffReq := &DiffReq{
				v:          v,
				isOriginal: true,
			}
			r, pass := mqChange.Handle(ctx, diffReq)
			if !pass {
				continue
			}
			diffReq = r.(*DiffReq)
			resCh <- fmt.Sprintf("old:%v", diffReq.v)
		}
	}()
}

func newHandle(ctx context.Context) {
	go newProducer() //生产消息
	go func() {      //消费消息
		for {
			v, ok := newProcess() //新逻辑
			if !ok {
				break
			}
			// resCh <- v
			diffReq := &DiffReq{
				v:          v,
				isOriginal: false,
			}
			r, pass := mqChange.Handle(ctx, diffReq)
			if !pass {
				continue
			}
			diffReq = r.(*DiffReq)
			resCh <- fmt.Sprintf("new:%v", diffReq.v)
		}
	}()
}

func output() {
	i := 0
	for {
		v, ok := <-resCh
		if !ok {
			break
		}
		fmt.Printf("i:%v, val:%v\n", i, v)
		i++
	}
}

func main() {
	ctx := context.Background()
	r := initRedis()
	mqChange = mq_change.MqChange{
		Name: "test",
		StageOldManager: &mq_change.DiffMangager{
			Name:  "old",
			Redis: r,
		},
		StageMixManager: &mq_change.DiffMangager{
			Name:  "mix",
			Redis: r,
		},
	}
	handle(ctx)    //旧链路
	newHandle(ctx) //新链路
	output()
}

func initRedis() *redis.Client {
	s, _ := miniredis.Run()
	r := redis.NewClient(&redis.Options{
		Addr: s.Addr(), // mock redis server的地址
	})
	fmt.Printf("%v\n", r)
	return r
}
