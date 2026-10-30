/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"os"
	"strings"
	"testing"
)

func TestRedisLoadIntoNonEmptyDatabase(t *testing.T) {
	m, err := newRedisMeta("redis", "127.0.0.1:6379/10", testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err = m.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}
	r := m.(*redisMeta)
	if err = r.checkEmpty(Background()); err != nil {
		t.Fatalf("empty database: %s", err)
	}
	r.rdb.Set(Background(), "foreign", "1", 0)
	defer r.rdb.Del(Background(), "foreign")
	if err = m.LoadMeta(strings.NewReader("{}")); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("load into a non-empty database: %v", err)
	}
	if err = r.prepareLoad(Background(), &LoadOption{}); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("binary load into a non-empty database: %v", err)
	}
}

func TestKvrocksDatabases(t *testing.T) {
	shared, isolated := os.Getenv("KVROCKS_SHARED_DB"), os.Getenv("KVROCKS_ISOLATED_DB")
	if shared == "" || isolated == "" {
		t.Skip("KVROCKS_SHARED_DB and KVROCKS_ISOLATED_DB are not set")
	}
	if _, err := newRedisMeta("redis", shared+"/5", testConfig()); err == nil || !strings.Contains(err.Error(), "ignores database 5") {
		t.Fatalf("database 5 on Kvrocks without redis-databases: %v", err)
	}
	if _, err := newRedisMeta("redis", shared+"/0", testConfig()); err != nil {
		t.Fatalf("database 0 on Kvrocks without redis-databases: %s", err)
	}
	m, err := newRedisMeta("redis", isolated+"/5", testConfig())
	if err != nil {
		t.Fatalf("database 5 on Kvrocks with redis-databases: %s", err)
	}
	if err = m.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}
	r := m.(*redisMeta)
	r.rdb.Set(Background(), "foreign", "1", 0)
	defer r.rdb.Del(Background(), "foreign")
	if n := r.rdb.DBSize(Background()).Val(); n != 0 {
		t.Logf("DBSIZE reports %d", n)
	}
	if err = r.checkEmpty(Background()); err == nil {
		t.Fatalf("non-empty Kvrocks database is reported empty")
	}
}

func TestKvrocksClient(t *testing.T) {
	addr := os.Getenv("KVROCKS_ISOLATED_DB")
	if addr == "" {
		t.Skip("KVROCKS_ISOLATED_DB is not set")
	}
	m, err := newRedisMeta("redis", addr+"/7", testConfig())
	if err != nil || m.Name() != "redis" {
		t.Fatalf("create meta: %s", err)
	}
	testMeta(t, m)
}
