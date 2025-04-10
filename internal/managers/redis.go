package managers

import (
	"binwatch/api/v1alpha1"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

type RedisManT struct {
	cli *redis.Client

	key string
}

func NewRedisMan(cfg *v1alpha1.ConfigSpec) (m *RedisManT, err error) {
	m = &RedisManT{
		key: fmt.Sprintf("%s-%s", cfg.Redis.KeyPrefix, cfg.Server.ID),
	}
	m.cli = redis.NewClient(&redis.Options{
		Addr:        cfg.Redis.Host + ":" + cfg.Redis.Port,
		Password:    cfg.Redis.Password,
		DB:          0,
		PoolSize:    10,
		PoolTimeout: 120,
	})

	return m, err
}

func (m *RedisManT) GetBinlogFilePos() (blfile string, blpos uint32, err error) {
	var result string
	result, err = m.cli.Get(context.Background(), m.key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
			blpos = 0
			return blfile, blpos, err
		}

		err = fmt.Errorf("error in get redis binlog file/position: %w", err)
		return blfile, blpos, err
	}

	// Split the result to get the file and position
	parts := strings.Split(result, "/")
	if len(parts) != 2 {
		err = fmt.Errorf("error parsing '%s' binlog file/position", result)
		return blfile, blpos, err
	}
	blfile = parts[0]

	var pos uint64
	pos, err = strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		err = fmt.Errorf("error parsing binlog position: %w", err)
		return blfile, blpos, err
	}
	blpos = uint32(pos)

	return blfile, blpos, err
}

func (m *RedisManT) SetBinlogFilePos(blfile string, blpos uint32) (err error) {
	filepos := fmt.Sprintf("%s/%d", blfile, blpos)
	err = m.cli.Set(context.Background(), m.key, filepos, 0).Err()
	return err
}
