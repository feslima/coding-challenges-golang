package redis

import (
	"reflect"
	"testing"
)

func TestBulkStringsDeserialization(t *testing.T) {
	cases := []struct {
		desc      string
		raw       []byte
		want      *Cmd
		wantError bool
	}{
		{"should return nil if null bulk string is received", []byte("$-1\r\n"), &Cmd{parsed: nil}, false},
		{"should return error if null bulk has no number", []byte("$-\r\n"), nil, true},
		{"should return error if null bulk has number other than 1", []byte("$-0\r\n"), nil, true},
		{"should return error if null bulk has number other than 1", []byte("$-2\r\n"), nil, true},
		{"should return string slice with single empty string if empty bulk string is received", []byte("$0\r\n\r\n"), &Cmd{parsed: []string{""}}, false},
		{"should return string slice with single string if bulk string is received", []byte("$5\r\nhello\r\n"), &Cmd{parsed: []string{"hello"}}, false},
		{"should return error if bulk length does not match data (less)", []byte("$4\r\nhello\r\n"), nil, true},
		{"should return error if bulk length does not match data (greater)", []byte("$6\r\nhello\r\n"), nil, true},
		{"should return string slice with single string if bulk string is received with whitespace", []byte("$11\r\nhello world\r\n"), &Cmd{parsed: []string{"hello world"}}, false},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			got, err := DecodeMessage(c.raw)

			if c.wantError {
				if err == nil {
					t.Errorf("Should throw an error. got: %v", got)
				}
			} else {
				if err != nil {
					t.Fatalf("Should not throw an error. err: %v", err)
				}

				if got == nil {
					t.Fatal("Return value expected to not be nil.")
				}

				if !reflect.DeepEqual(got.parsed, c.want.parsed) {
					t.Fatalf("Expected parsed bulk string to be %v. Got %v", c.want.parsed, got.parsed)
				}
			}
		})
	}
}

func TestArrayDeserialization(t *testing.T) {
	testCases := []struct {
		desc      string
		raw       []byte
		want      *Cmd
		wantError bool
	}{
		{
			desc:      "should return empty array",
			raw:       []byte("*0\r\n"),
			want:      &Cmd{parsed: []string{}},
			wantError: false,
		},
		{
			desc:      "should return string array (hello)",
			raw:       []byte("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"),
			want:      &Cmd{parsed: []string{"hello", "world"}},
			wantError: false,
		},
		{
			desc:      "should return string array (echo)",
			raw:       []byte("*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n"),
			want:      &Cmd{parsed: []string{"echo", "hello world"}},
			wantError: false,
		},
		{
			desc:      "should return string array (ping)",
			raw:       []byte("*1\r\n$4\r\nping\r\n"),
			want:      &Cmd{parsed: []string{"ping"}},
			wantError: false,
		},
		{
			desc:      "should return string array (get)",
			raw:       []byte("*2\r\n$3\r\nget\r\n$3\r\nkey\r\n"),
			want:      &Cmd{parsed: []string{"get", "key"}},
			wantError: false,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			got, err := DecodeMessage(tC.raw)

			if tC.wantError {
				if err == nil {
					t.Errorf("Should throw an error. got: %v", got)
				}
			} else {
				if err != nil {
					t.Fatalf("Should not throw an error. err: %v", err)
				}

				if got == nil {
					t.Fatal("Return value expected to not be nil.")
				}

				if !reflect.DeepEqual(got.parsed, tC.want.parsed) {
					t.Fatalf("Expected parsed array to be %v. Got %v", tC.want.parsed, got.parsed)
				}
			}
		})
	}
}

func TestDecodeAndProcessCommands(t *testing.T) {
	testCases := []struct {
		desc      string
		raw       []byte
		want      []byte
		wantError bool
	}{
		{
			desc:      "PING",
			raw:       []byte("*1\r\n$4\r\nping\r\n"),
			want:      []byte("+PONG\r\n"),
			wantError: false,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			got, err := ProcessRequest(tC.raw)

			if tC.wantError {
				if err == nil {
					t.Errorf("Should throw an error. got: %v", got)
				}
			} else {
				if err != nil {
					t.Fatalf("Should not throw an error. err: %v", err)
				}

				if !reflect.DeepEqual(got, tC.want) {
					t.Fatalf("got: %s. want: %s", got, tC.want)
				}
			}
		})
	}
}