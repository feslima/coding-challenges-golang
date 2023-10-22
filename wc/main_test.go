package main

import (
	"flag"
	"os"
	"testing"
)

func TestOpenFile(t *testing.T) {
	filename := "test.txt"
	file, err := openFile(filename)
	if err != nil {
		t.Error("Expected to open file without errors.")
	}
	defer file.Close()

	fileContentsBuf := make([]byte, 329*1024)
	n, err := file.Read(fileContentsBuf)
	if err != nil {
		t.Fatal(err)
	}

	if n == 0 {
		t.Error("Expected to read more than zero bytes")
	}
}

func TestNumberOfBytesInFile(t *testing.T) {
	filename := "test.txt"
	file, _ := openFile(filename)

	result, err := DoWc(file)
	if err != nil {
		t.Fatal(err)
	}

	want := 342190
	got := result.byteCount
	if got != want {
		t.Errorf("got %d want %d", got, want)
	}
}

// See https://stackoverflow.com/a/57972717 on why this
func resetFlags() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
}

func TestConfigFlagsParser(t *testing.T) {
	t.Run("byte count should be true if not set", func(t *testing.T) {
		resetFlags()
		configs := WcConfigs{shouldCountBytes: false}

		configs.getFlags()

		if !configs.shouldCountBytes {
			t.Error("Count bytes flag expected to be true if not set")
		}
	})

	t.Run("byte count should be true if set", func(t *testing.T) {
		resetFlags()
		configs := WcConfigs{shouldCountBytes: false}

		os.Args = append(os.Args, "-c")

		configs.getFlags()

		if !configs.shouldCountBytes {
			t.Error("Count bytes flag expected to be true if not set")
		}
	})
}

func TestGetResultsReport(t *testing.T) {
	t.Run("byte count report should be printed if set", func(t *testing.T) {
		configs := WcConfigs{shouldCountBytes: true}
		results := WcResult{name: "test.txt", byteCount: 342190}

		want := "342190 test.txt"
		got := getResultsReport(configs, results)

		if want != got {
			t.Errorf("got '%s' want '%s'", got, want)
		}
	})
}
