package cache

import (
	"binwatch/api/v1alpha2"
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/redis/go-redis/v9"
)

type RedisCacheT struct {
	cli *redis.Client

	key string
}

func newRedisCache(cfg v1alpha2.ServerT) (c *RedisCacheT, err error) {
	c = &RedisCacheT{
		key: strings.Join([]string{"binwatch", cfg.ID, "cache"}, "."),
	}
	c.cli = redis.NewClient(&redis.Options{
		Addr:        fmt.Sprintf("%s:%d", cfg.Cache.Redis.Host, cfg.Cache.Redis.Port),
		Password:    cfg.Cache.Redis.Password,
		DB:          0,
		PoolSize:    10,
		PoolTimeout: 120,
	})

	return c, err
}

func (c *RedisCacheT) Load() (blLoc BinlogLocation, err error) {
	var locStr string
	locStr, err = c.cli.Get(context.Background(), c.key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
			return blLoc, err
		}

		err = fmt.Errorf("error in get '%s' redis cache: %w", c.key, err)
		return blLoc, err
	}

	if locStr == "" {
		return blLoc, err
	}

	if idx := strings.IndexFunc(locStr, func(r rune) bool { return unicode.IsSpace(r) }); idx != -1 {
		err = fmt.Errorf("error parsing location in '%s' redis cache, found spaces, must be <file>/<position> format", c.key)
		return blLoc, err
	}

	// Split the result to get the file and position
	locParts := strings.Split(locStr, "/")
	if len(locParts) != 2 {
		err = fmt.Errorf("error parsing location in '%s' cache file, number of inconsistent '/', must be <file>/<position> format", c.key)
		return blLoc, err
	}
	blLoc.File = locParts[0]

	var pos uint64
	pos, err = strconv.ParseUint(locParts[1], 10, 32)
	if err != nil {
		err = fmt.Errorf("error parsing location in '%s' redis cache, position format error: %w", c.key, err)
		return blLoc, err
	}
	blLoc.Position = uint32(pos)

	return blLoc, err
}

func (c *RedisCacheT) Store(blLoc BinlogLocation) (err error) {
	err = c.cli.Set(context.Background(), c.key, blLoc.String(), 0).Err()
	return err
}
