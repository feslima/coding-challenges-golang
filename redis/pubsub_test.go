package redis

import (
	"reflect"
	"testing"
	"time"
)

type pubsubTestCase struct {
	now              time.Time
	data             string
	want             []byte
	initialState     mapState
	expectedChannels []string
}

func (t pubsubTestCase) Now() time.Time {
	return t.now
}

func (t pubsubTestCase) InitialState() mapState {
	return t.initialState
}

func TestSubscribeCommandToSingleChannel(t *testing.T) {
	now := time.Now()
	tC := pubsubTestCase{
		now:  now,
		data: "*2\r\n$9\r\nsubscribe\r\n$4\r\ntest\r\n",
		want: []byte("*3\r\n$9\r\nsubscribe\r\n$4\r\ntest\r\n:1\r\n"),
		initialState: mapState{
			ks: map[string]keyspaceEntry{},
			sm: map[string]string{},
			lm: map[string]list{},
		},
		expectedChannels: []string{"test"},
	}

	app, srv, logger := setupApplication(tC, t)
	go func() { Listen(srv, app, logger) }()

	conn := makeRequestToServer(tC.data, srv, t)
	defer conn.Close()

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from connection: %s", err)
	}
	got := buf[:n]

	if !reflect.DeepEqual(got, tC.want) {
		t.Errorf("got: %#v. want: %#v", string(got), string(tC.want))
	}

	localaddr := conn.LocalAddr().String() // local addr to match with remote address indexing
	client, ok := app.clients[localaddr]
	if !ok || client == nil {
		t.Fatal("expected to have a client indexed")
	}

	if !client.isOnSubscribeMode {
		t.Error("client is expected to be on subscribe mode")
	}

	for _, ch := range tC.expectedChannels {
		_, ok = client.subscribedTo[ch]
		if !ok {
			t.Errorf("expected client to be subscribed to '%v' channel", ch)
		}
	}
}

func TestSubscribeCommandToMultipleChannels(t *testing.T) {
	now := time.Now()
	tC := pubsubTestCase{
		now:  now,
		data: "*3\r\n$9\r\nsubscribe\r\n$5\r\nfirst\r\n$6\r\nsecond\r\n",
		want: []byte("*3\r\n$9\r\nsubscribe\r\n$5\r\nfirst\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$6\r\nsecond\r\n:2\r\n"),
		initialState: mapState{
			ks: map[string]keyspaceEntry{},
			sm: map[string]string{},
			lm: map[string]list{},
		},
		expectedChannels: []string{"first", "second"},
	}

	app, srv, logger := setupApplication(tC, t)
	go func() { Listen(srv, app, logger) }()

	conn := makeRequestToServer(tC.data, srv, t)
	defer conn.Close()

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from connection: %s", err)
	}
	got := buf[:n]

	if !reflect.DeepEqual(got, tC.want) {
		t.Errorf("got: %#v. want: %#v", string(got), string(tC.want))
	}

	localaddr := conn.LocalAddr().String()
	client, ok := app.clients[localaddr]
	if !ok || client == nil {
		t.Fatal("expected to have a client indexed")
	}

	if !client.isOnSubscribeMode {
		t.Error("client is expected to be on subscribe mode")
	}

	for _, ch := range tC.expectedChannels {
		_, ok = client.subscribedTo[ch]
		if !ok {
			t.Errorf("expected client to be subscribed to '%v' channel", ch)
		}
	}
}

func TestPublishCommandToSingleSubscriber(t *testing.T) {
	now := time.Now()
	tC := pubsubTestCase{
		now:  now,
		data: "*3\r\n$7\r\npublish\r\n$4\r\ntest\r\n$5\r\nhello\r\n",
		want: []byte(":1\r\n"),
		initialState: mapState{
			ks: map[string]keyspaceEntry{},
			sm: map[string]string{},
			lm: map[string]list{},
		},
		expectedChannels: []string{"test"},
	}

	app, srv, logger := setupApplication(tC, t)
	go func() { Listen(srv, app, logger) }()

	// subscribe to channel
	conn := makeRequestToServer("*2\r\n$9\r\nsubscribe\r\n$4\r\ntest\r\n", srv, t)
	defer conn.Close()

	// read the response from subscribe command and assert for no errors
	buf := make([]byte, 4096)
	_, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from subscriber connection: %s", err)
	}

	// publish from another client
	pubConn := makeRequestToServer(tC.data, srv, t)
	defer pubConn.Close()

	// read the response from publish command
	buf = make([]byte, 4096)
	n, err := pubConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from publisher connection: %s", err)
	}
	got := buf[:n]

	if !reflect.DeepEqual(got, tC.want) {
		t.Fatalf("got from publisher connection: %#v. want: %#v", string(got), string(tC.want))
	}

	// read published message from subscriber connection
	buf = make([]byte, 4096)
	n, err = conn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read publication from subscriber connection: %s", err)
	}

	got = buf[:n]
	wantSub := []byte("*3\r\n$7\r\nmessage\r\n$4\r\ntest\r\n$5\r\nhello\r\n")
	if !reflect.DeepEqual(got, wantSub) {
		t.Errorf("got: %#v. want: %#v", string(got), string(wantSub))
	}

	localaddr := conn.LocalAddr().String() // local addr to match with remote address indexing
	client, ok := app.clients[localaddr]
	if !ok || client == nil {
		t.Fatal("expected to have a client indexed")
	}

	if !client.isOnSubscribeMode {
		t.Error("client is expected to be on subscribe mode")
	}

	for _, ch := range tC.expectedChannels {
		_, ok = client.subscribedTo[ch]
		if !ok {
			t.Errorf("expected client to be subscribed to '%v' channel", ch)
		}
	}
}

func TestPublishCommandToMultipleSubscriber(t *testing.T) {
	now := time.Now()
	tC := pubsubTestCase{
		now:  now,
		data: "*3\r\n$7\r\npublish\r\n$5\r\nfirst\r\n$5\r\nhello\r\n",
		want: []byte(":2\r\n"),
		initialState: mapState{
			ks: map[string]keyspaceEntry{},
			sm: map[string]string{},
			lm: map[string]list{},
		},
		expectedChannels: []string{""},
	}

	app, srv, logger := setupApplication(tC, t)
	go func() { Listen(srv, app, logger) }()

	// subscribe to channels
	firstSub := makeRequestToServer("*2\r\n$9\r\nsubscribe\r\n$5\r\nfirst\r\n", srv, t)
	defer firstSub.Close()

	secondSub := makeRequestToServer("*2\r\n$9\r\nsubscribe\r\n$6\r\nsecond\r\n", srv, t)
	defer secondSub.Close()

	thirdSub := makeRequestToServer("*4\r\n$9\r\nsubscribe\r\n$5\r\nfirst\r\n$6\r\nsecond\r\n$5\r\nthird\r\n", srv, t)
	defer thirdSub.Close()

	// read the response from subscribe command and assert for no errors
	_, err1 := firstSub.Read(make([]byte, 4096))
	_, err2 := secondSub.Read(make([]byte, 4096))
	_, err3 := thirdSub.Read(make([]byte, 4096))
	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatal("failed to read from subscribers connections")
	}

	// publish to first channel
	pubConn := makeRequestToServer("*3\r\n$7\r\npublish\r\n$5\r\nfirst\r\n$11\r\nhello first\r\n", srv, t)
	defer pubConn.Close()

	// read the response from publish command
	buf := make([]byte, 4096)
	n, err := pubConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from publisher connection: %s", err)
	}
	got := buf[:n]

	if !reflect.DeepEqual(got, tC.want) {
		t.Fatalf("got from publisher connection: %#v. want: %#v", string(got), string(tC.want))
	}

	// read published message from 1st and 3rd subscribers
	buf1 := make([]byte, 4096)
	n, err = firstSub.Read(buf1)
	if err != nil {
		t.Fatalf("failed to read publication from first subscriber connection: %s", err)
	}

	got1 := buf1[:n]
	wantSub1 := []byte("*3\r\n$7\r\nmessage\r\n$5\r\nfirst\r\n$11\r\nhello first\r\n")
	if !reflect.DeepEqual(got1, wantSub1) {
		t.Errorf("got: %#v. want: %#v", string(got1), string(wantSub1))
	}

	buf3 := make([]byte, 4096)
	n, err = thirdSub.Read(buf3)
	if err != nil {
		t.Fatalf("failed to read publication from third subscriber connection: %s", err)
	}

	got3 := buf3[:n]
	if !reflect.DeepEqual(got3, wantSub1) {
		t.Errorf("got: %#v. want: %#v", string(got3), string(wantSub1))
	}

	// publish to second channel
	_, err = pubConn.Write([]byte("*3\r\n$7\r\npublish\r\n$6\r\nsecond\r\n$12\r\nhello second\r\n"))
	if err != nil {
		t.Fatalf("failed to write second publish. err: %v", err)
	}

	// read the response from publish command
	buf = make([]byte, 4096)
	n, err = pubConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from publisher connection: %s", err)
	}
	got = buf[:n]

	if !reflect.DeepEqual(got, tC.want) {
		t.Fatalf("got from second publish connection: %#v. want: %#v", string(got), string(tC.want))
	}

	// read published message from 2nd and 3rd subscribers
	buf2 := make([]byte, 4096)
	n, err = secondSub.Read(buf2)
	if err != nil {
		t.Fatalf("failed to read publication from second subscriber connection: %s", err)
	}

	got2 := buf2[:n]
	wantSub2 := []byte("*3\r\n$7\r\nmessage\r\n$6\r\nsecond\r\n$12\r\nhello second\r\n")
	if !reflect.DeepEqual(got2, wantSub2) {
		t.Errorf("got: %#v. want: %#v", string(got2), string(wantSub2))
	}

	buf3 = make([]byte, 4096)
	n, err = thirdSub.Read(buf3)
	if err != nil {
		t.Fatalf("failed to read publication from third subscriber connection: %s", err)
	}

	got3 = buf3[:n]
	if !reflect.DeepEqual(got3, wantSub2) {
		t.Errorf("got: %#v. want: %#v", string(got3), string(wantSub2))
	}

	// publish to third channel
	_, err = pubConn.Write([]byte("*3\r\n$7\r\npublish\r\n$5\r\nthird\r\n$11\r\nhello third\r\n"))
	if err != nil {
		t.Fatalf("failed to write third publish. err: %v", err)
	}

	// read the response from publish command
	buf = make([]byte, 4096)
	n, err = pubConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from publisher connection: %s", err)
	}
	got = buf[:n]

	if !reflect.DeepEqual(got, []byte(":1\r\n")) {
		t.Fatalf("got from second publish connection: %#v. want: %#v", string(got), ":1\r\n")
	}

	// read published message from 3rd subscriber
	buf3 = make([]byte, 4096)
	n, err = thirdSub.Read(buf3)
	if err != nil {
		t.Fatalf("failed to read publication from third subscriber connection: %s", err)
	}

	got3 = buf3[:n]
	wantSub3 := []byte("*3\r\n$7\r\nmessage\r\n$5\r\nthird\r\n$11\r\nhello third\r\n")
	if !reflect.DeepEqual(got3, wantSub3) {
		t.Errorf("got: %#v. want: %#v", string(got3), string(wantSub3))
	}
}
