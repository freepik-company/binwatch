package cache

import (
	"fmt"

	"binwatch/api/v1alpha2"
)

const (
	TypeLocal = "local"
	TypeRedis = "redis"
)

type CacheI interface {
	Load() (BinlogLocation, error)
	Store(BinlogLocation) error
}

type BinlogLocation struct {
	File     string
	Position uint32
}

func (l *BinlogLocation) String() string {
	return fmt.Sprintf("%s/%d", l.File, l.Position)
}

func (l *BinlogLocation) Bytes() []byte {
	return []byte(l.String())
}

func NewCache(cfg v1alpha2.ServerT) (cache CacheI, err error) {
	switch cfg.Cache.Type {
	case TypeLocal:
		{
			cache, err = newLocalCache(cfg)
		}
	case TypeRedis:
		{
			cache, err = newRedisCache(cfg)
		}
	default:
		{
			err = fmt.Errorf("unsuported '%s' cache type", cfg.Cache.Type)
		}
	}
	return cache, err
}
