package cache

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"unicode"

	"binwatch/api/v1alpha2"
	"binwatch/internal/utils"
)

type LocalCacheT struct {
	file string
}

func newLocalCache(cfg v1alpha2.ServerT) (c *LocalCacheT, err error) {
	c = &LocalCacheT{
		file: path.Join(cfg.Cache.Local.Path, strings.Join([]string{"binwatch", cfg.ID, "cache"}, ".")),
	}

	var info os.FileInfo
	info, err = os.Stat(c.file)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(cfg.Cache.Local.Path, utils.DirMode)
			if err != nil {
				return c, err
			}

			if err = os.WriteFile(c.file, []byte{}, utils.FileMode); err != nil {
				return c, err
			}

			return c, err
		}
	}

	if info.IsDir() {
		err = fmt.Errorf("directory with same name as '%s' cache file", c.file)
	}

	return c, err
}

func (c *LocalCacheT) Load() (blLoc BinlogLocation, err error) {
	var locBytes []byte
	locBytes, err = os.ReadFile(c.file)
	if err != nil {
		return blLoc, err
	}

	if len(locBytes) == 0 {
		return blLoc, err
	}

	if idx := bytes.IndexFunc(locBytes, func(r rune) bool { return unicode.IsSpace(r) }); idx != -1 {
		err = fmt.Errorf("error parsing location in '%s' cache file, found spaces, must be <file>/<position> format", c.file)
		return blLoc, err
	}

	locParts := strings.Split(string(locBytes), "/")
	if len(locParts) != 2 {
		err = fmt.Errorf("error parsing location in '%s' cache file, number of inconsistent '/', must be <file>/<position> format", c.file)
		return blLoc, err
	}
	blLoc.File = locParts[0]

	var pos uint64
	pos, err = strconv.ParseUint(locParts[1], 10, 32)
	if len(locParts) != 2 {
		err = fmt.Errorf("error parsing location in '%s' cache file, position format error: %w", c.file, err)
		return blLoc, err
	}
	blLoc.Position = uint32(pos)

	return blLoc, err
}

func (c *LocalCacheT) Store(blLoc BinlogLocation) (err error) {
	err = os.WriteFile(c.file, blLoc.Bytes(), utils.FileMode)
	return err
}
