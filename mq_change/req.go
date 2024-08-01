package mq_change

type Req interface {
	Key() string
	DiffVal() string
	IsOriginal() bool
}
