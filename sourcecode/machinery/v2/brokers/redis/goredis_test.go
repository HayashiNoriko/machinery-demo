// 测试发现 NewGR 会修改传进去的 addrs
package redis

import (
	"fmt"
	"testing"

	"demo/sourcecode/machinery/v2/config"
	// "github.com/redis/go-redis/v9"
)

func TestNewGR(t *testing.T) {
	cnf := &config.Config{
		Redis: &config.RedisConfig{
			MasterName:      "mymaster",
			DelayedTasksKey: "my_delayed_tasks",
		},
	}
	addrs := []string{"mypassword@127.0.0.1:6379", "mypassword2@127.0.0.1:6380"}

	_ = NewGR(cnf, addrs, 0)

	fmt.Println(addrs)

}
