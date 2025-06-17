package three_async

import "testing"

func TestFork(t *testing.T) {
	fork()
}

func TestMq(t *testing.T) {
	mq()
}

func TestNotify(t *testing.T) {
	notify()
}
