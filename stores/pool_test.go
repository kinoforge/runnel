package stores

import (
	"testing"
	"time"

	"github.com/kinoforge/runnel/store/sqlstore"
)

func TestTakePool(t *testing.T) {
	dsn, p, err := takePool("postgres://u:p@h/db?sslmode=disable&max_conns=5&max_idle_conns=2&conn_max_lifetime=10m&conn_max_idle_time=1m")
	want := sqlstore.Pool{MaxOpen: 5, MaxIdle: 2, MaxLifetime: 10 * time.Minute, MaxIdleTime: time.Minute}
	if err != nil || p != want || dsn != "postgres://u:p@h/db?sslmode=disable" {
		t.Fatalf("%s %+v %v", dsn, p, err)
	}
	if dsn, p, err := takePool("host=h dbname=db"); err != nil || dsn != "host=h dbname=db" || p != (sqlstore.Pool{}) {
		t.Fatalf("key=value dsn: %s %+v %v", dsn, p, err)
	}
	for _, bad := range []string{"max_conns=0", "max_conns=x", "conn_max_lifetime=-1s", "conn_max_idle_time=soon"} {
		if _, _, err := takePool("postgres://h/db?" + bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}
