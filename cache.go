package cache

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strings"
	"time"
)

const DefaultSeparator = ":"

var (
	NotInitializedError = fmt.Errorf("cache was not initialized")
	Separator = DefaultSeparator
)

type Cache struct {
	pool *redis.Pool
}

func NewCache(host string, cred ...string) *Cache {
	return NewCacheFromPool(&redis.Pool{
		MaxActive: 10,
		MaxIdle:   5,
		Wait:      true,
		Dial: func() (redis.Conn, error) {
			if len(cred) >= 2 {
				return redis.Dial("tcp", host,
					redis.DialUsername(cred[0]),
					redis.DialPassword(cred[1]))
			} else if len(cred) == 1 {
				return redis.Dial("tcp", host,
					redis.DialPassword(cred[0]))
			} else {
				return redis.Dial("tcp", host)
			}
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	})
}

func NewCacheFromPool(pool *redis.Pool) *Cache {
	return &Cache{pool: pool}
}

func (c *Cache) Pool() *redis.Pool {
	return c.pool
}

func (c *Cache) Ping() error {
	if c == nil || c.pool == nil {
		return NotInitializedError
	}

	r := c.pool.Get()
	defer func() { _ = r.Close() }()

	if _, err := r.Do("PING"); err != nil {
		return fmt.Errorf("failed to 'PING' db, reason: %v", err)
	}

	return nil
}

func (c *Cache) Get(key string, namespace ...string) (interface{}, error) {
	if c == nil || c.pool == nil {
		return nil, NotInitializedError
	}

	r := c.pool.Get()
	defer func() { _ = r.Close() }()

	ns := key
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, Separator), ns)
	}

	if value, err := r.Do("GET", ns); err != nil {
		return nil, fmt.Errorf("failed to retrieve key '%s', reason %v", ns, err)
	} else {
		return value, nil

	}
}

func (c *Cache) GetBool(key string, namespace ...string) (bool, error) {
	return redis.Bool(c.Get(key, namespace...))
}

func (c *Cache) GetBytes(key string, namespace ...string) ([]byte, error) {
	return redis.Bytes(c.Get(key, namespace...))
}

func (c *Cache) GetInt(key string, namespace ...string) (int, error) {
	return redis.Int(c.Get(key, namespace...))
}

func (c *Cache) GetString(key string, namespace ...string) (string, error) {
	return redis.String(c.Get(key, namespace...))
}

func (c *Cache) Set(key string, value interface{}, namespace ...string) error {
	if c == nil || c.pool == nil {
		return NotInitializedError
	}

	r := c.pool.Get()
	defer func() { _ = r.Close() }()

	ns := key
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, Separator), ns)
	}

	if _, err := r.Do("SET", ns, value); err != nil {
		return fmt.Errorf("failed to set value to key '%s', reason: %v", ns, err)
	}

	return nil
}

func (c *Cache) SetEx(key string, value interface{}, duration time.Duration, namespace ...string) error {
	if c == nil || c.pool == nil {
		return NotInitializedError
	}

	r := c.pool.Get()
	defer func() { _ = r.Close() }()

	ns := key
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, Separator), ns)
	}

	if _, err := r.Do("SETEX", ns, int(duration/time.Second), value); err != nil {
		return fmt.Errorf("failed to set value to key '%s', reason: %v", ns, err)
	}

	return nil
}

func (c *Cache) Exists(key string, namespace ...string) (bool, error) {
	if c == nil || c.pool == nil {
		return false, NotInitializedError
	}

	r := c.pool.Get()
	defer func() { _ = r.Close() }()

	ns := key
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, Separator), ns)
	}

	if 	ok, err := redis.Bool(r.Do("EXISTS", ns)); err != nil {
		return false, fmt.Errorf("failed to check key '%s' existance, reason: %v", ns, err)
	} else {
		return ok, nil
	}
}

func (c *Cache) Delete(key string, namespace ...string) error {
	if c == nil || c.pool == nil {
		return NotInitializedError
	}

	r := c.pool.Get()
	defer func() { _ = r.Close() }()

	ns := key
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, Separator), ns)
	}

	if _, err := r.Do("DEL", ns); err != nil {
		return fmt.Errorf("failed to delete key '%s', reason: %v", ns, err)
	}

	return nil
}

func (c *Cache) Clear(pattern string, namespace ...string) error {
	if c == nil || c.pool == nil {
		return NotInitializedError
	}

	const msg = "failed to clear db, reason %v"
	var (
		cursor int64
		keys   []interface{}
	)

	r := c.pool.Get()
	defer func() { _ = r.Close() }()

	ns := pattern
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, Separator), ns)
	}

	for {
		values, err := redis.Values(r.Do("SCAN", cursor, "MATCH", fmt.Sprintf(ns)))
		if err != nil {
			return fmt.Errorf(msg, err)
		}

		values, err = redis.Scan(values, &cursor, &keys)
		if err != nil {
			return fmt.Errorf(msg, err)
		}

		if len(keys) > 0 {
			_, err = r.Do("UNLINK", keys...)
			if err != nil {
				return fmt.Errorf(msg, err)
			}
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}

func (c *Cache) ClearAll(namespace ...string) error {
	return c.Clear("*", namespace...)
}

func (c *Cache) GetKeys(pattern string, namespace ...string) ([]string, error) {
	if c == nil || c.pool == nil {
		return nil, NotInitializedError
	}

	const msg = "failed to retrieve keys, reason %v"
	var (
		cursor int64
		items  []string
	)

	r := c.pool.Get()
	defer func() { _ = r.Close() }()

	ns := pattern
	if len(namespace) > 0 {
		ns = fmt.Sprintf("%s:%s", strings.Join(namespace, Separator), ns)
	}

	var keys []string
	for {
		values, err := redis.Values(r.Do("SCAN", cursor, "MATCH", fmt.Sprintf(ns)))
		if err != nil {
			return nil, fmt.Errorf(msg, err)
		}

		values, err = redis.Scan(values, &cursor, &items)
		if err != nil {
			return nil, fmt.Errorf(msg, err)
		}

		keys = append(keys, items...)
		if cursor == 0 {
			break
		}
	}

	return keys, nil
}

func (c *Cache) GetAllKeys(namespace ...string) ([]string, error) {
	return c.GetKeys("*", namespace...)
}
