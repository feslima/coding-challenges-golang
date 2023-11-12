package redis

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"
)

type appTestCase struct {
	now   time.Time
	state mapState
	want  []byte
}

func setupApp(tC appTestCase) *Application {

	timer := TestClockTimer{mockNow: tC.now}
	logger := NewTestLogger()
	app := NewApplication(nil, timer, logger)
	app.state.keyspace.keys = tC.state.ks
	app.state.keyspace.stringMap = tC.state.sm
	app.state.keyspace.listMap = tC.state.lm

	return app
}

func TestStateSave(t *testing.T) {
	now := time.Now()
	tomorrow := now.Add(24 * time.Hour)
	tmwUnix := tomorrow.Unix()
	tc := appTestCase{
		now: now,
		state: mapState{
			ks: map[string]keyspaceEntry{
				"Name":      {group: "string", expires: nil},
				"Later":     {group: "string", expires: &tomorrow},
				"NameList":  {group: "list", expires: nil},
				"LaterList": {group: "list", expires: &tomorrow},
			},
			sm: map[string]string{
				"Name":  "John",
				"Later": "hello",
			},
			lm: map[string]list{
				"NameList":  NewListFromSlice([]string{"hi", "1"}),
				"LaterList": NewListFromSlice([]string{"hello", "2"}),
			},
		},
		want: []byte(
			"*3\r\n$3\r\nset\r\n$4\r\nName\r\n$4\r\nJohn\r\n" +
				"*3\r\n$3\r\nset\r\n$5\r\nLater\r\n$5\r\nhello\r\n" +
				fmt.Sprintf("*3\r\n$8\r\nexpireat\r\n$5\r\nLater\r\n$%d\r\n%d\r\n", len(fmt.Sprint(tmwUnix)), tmwUnix) +
				"*4\r\n$5\r\nrpush\r\n$8\r\nNameList\r\n$2\r\nhi\r\n$1\r\n1\r\n" +
				"*4\r\n$5\r\nrpush\r\n$9\r\nLaterList\r\n$5\r\nhello\r\n$1\r\n2\r\n" +
				fmt.Sprintf("*3\r\n$8\r\nexpireat\r\n$9\r\nLaterList\r\n$%d\r\n%d\r\n", len(fmt.Sprint(tmwUnix)), tmwUnix),
		),
	}
	app := setupApp(tc)
	buf := new(bytes.Buffer)

	err := app.state.Save(buf)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if app.state.keyspace.modifications != 0 {
		t.Fatal("modifications counter must be reset after calling save")
	}

	got := buf.String()
	if got != string(tc.want) {
		t.Errorf("\ngot:\n%s\n\nwant:\n%s", got, tc.want)
	}
}

func (e keyspaceEntry) IsEqual(o keyspaceEntry) bool {
	if e.group != o.group {
		return false
	}

	if e.expires == nil && o.expires != nil {
		return false
	}

	if e.expires != nil && o.expires == nil {
		return false
	}

	if e.expires == nil && o.expires == nil {
		return true
	}

	eexp := e.expires.Format(time.RFC3339)
	oexp := o.expires.Format(time.RFC3339)

	return oexp == eexp
}

func (ks keyspace) IsEqual(o keyspace) bool {
	for k, e := range ks.keys {
		oe, ok := o.keys[k]
		if !ok || !e.IsEqual(oe) {
			return false
		}

		if e.group == "string" {
			if !maps.Equal(ks.stringMap, o.stringMap) {
				return false
			}
		} else {
			ev, ok1 := ks.listMap[k]
			ov, ok2 := o.listMap[k]
			if !ok1 || !ok2 {
				return false
			}

			if !slices.Equal(ev.ToSlice(), ov.ToSlice()) {
				return false
			}
		}
	}

	return true
}

func TestStateLoad(t *testing.T) {
	now := time.Now()
	tomorrow := now.Add(24 * time.Hour)
	tmwUnix := tomorrow.Unix()
	want := keyspace{
		keys: map[string]keyspaceEntry{
			"Name":      {group: "string", expires: nil},
			"Later":     {group: "string", expires: &tomorrow},
			"NameList":  {group: "list", expires: nil},
			"LaterList": {group: "list", expires: &tomorrow},
		},
		stringMap: map[string]string{
			"Name":  "John",
			"Later": "hello",
		},
		listMap: map[string]list{
			"NameList":  NewListFromSlice([]string{"hi", "1"}),
			"LaterList": NewListFromSlice([]string{"hello", "2"}),
		},
	}

	data := []byte(
		"*3\r\n$3\r\nset\r\n$4\r\nName\r\n$4\r\nJohn\r\n" +
			"*3\r\n$3\r\nset\r\n$5\r\nLater\r\n$5\r\nhello\r\n" +
			fmt.Sprintf("*3\r\n$8\r\nexpireat\r\n$5\r\nLater\r\n$%d\r\n%d\r\n", len(fmt.Sprint(tmwUnix)), tmwUnix) +
			"*4\r\n$5\r\nrpush\r\n$8\r\nNameList\r\n$2\r\nhi\r\n$1\r\n1\r\n" +
			"*4\r\n$5\r\nrpush\r\n$9\r\nLaterList\r\n$5\r\nhello\r\n$1\r\n2\r\n" +
			fmt.Sprintf("*3\r\n$8\r\nexpireat\r\n$9\r\nLaterList\r\n$%d\r\n%d\r\n", len(fmt.Sprint(tmwUnix)), tmwUnix),
	)

	app := setupApp(
		appTestCase{
			now: now,
			state: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			}})

	r := bytes.NewReader(data)

	err := app.state.Load(r, app)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if app.state.keyspace.modifications != 0 {
		t.Fatal("modifications counter must be 0 after calling load")
	}

	gotState := app.state
	gotKs := gotState.keyspace

	if !gotKs.IsEqual(want) {
		t.Errorf("got: %#v. want: %#v", gotKs, want)
	}
}
