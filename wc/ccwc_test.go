package main

import (
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

func TestNumberOfLinesInFile(t *testing.T) {
	filename := "test.txt"
	file, _ := openFile(filename)

	result, err := DoWc(file)
	if err != nil {
		t.Fatal(err)
	}

	want := 7145
	got := result.lineCount
	if got != want {
		t.Errorf("got %d want %d", got, want)
	}
}

func TestConfigFlagsParser(t *testing.T) {
	// byte count
	t.Run("byte count should be true if no flags are set", func(t *testing.T) {
		configs := WcConfigs{shouldCountBytes: false, shouldCountLines: false}

		_, err := configs.parseFlagsAndFileName("some-name", []string{})
		if err != nil {
			t.Error("Expected to parse flags without errors.")
		}

		if !configs.shouldCountBytes {
			t.Error("Count bytes flag expected to be true if not set")
		}
	})

	t.Run("byte count should be true if set", func(t *testing.T) {
		configs := WcConfigs{shouldCountBytes: false, shouldCountLines: false}

		_, err := configs.parseFlagsAndFileName("some-name", []string{"-c"})
		if err != nil {
			t.Error("Expected to parse flags without errors.")
		}

		if !configs.shouldCountBytes {
			t.Error("Count bytes flag expected to be true if not set")
		}
	})

	t.Run("byte count should be false if not set", func(t *testing.T) {
		configs := WcConfigs{shouldCountBytes: false, shouldCountLines: false}

		_, err := configs.parseFlagsAndFileName("some-name", []string{"-l"})
		if err != nil {
			t.Error("Expected to parse flags without errors.")
		}

		if configs.shouldCountBytes {
			t.Error("Count bytes flag expected to be false if not set")
		}
	})

	// line count
	t.Run("line count should be true if no flags are set", func(t *testing.T) {
		configs := WcConfigs{shouldCountBytes: false, shouldCountLines: false}

		_, err := configs.parseFlagsAndFileName("some-name", []string{})
		if err != nil {
			t.Error("Expected to parse flags without errors.")
		}

		if !configs.shouldCountLines {
			t.Error("Count lines flag expected to be true if not set")
		}
	})

	t.Run("line count should be true if set", func(t *testing.T) {
		configs := WcConfigs{shouldCountBytes: false, shouldCountLines: false}

		_, err := configs.parseFlagsAndFileName("some-name", []string{"-l"})
		if err != nil {
			t.Error("Expected to parse flags without errors.")
		}

		if !configs.shouldCountLines {
			t.Error("Count lines flag expected to be true if not set")
		}
	})

	t.Run("line count should be false if not set", func(t *testing.T) {
		configs := WcConfigs{shouldCountBytes: false, shouldCountLines: false}

		_, err := configs.parseFlagsAndFileName("some-name", []string{"-c"})
		if err != nil {
			t.Error("Expected to parse flags without errors.")
		}

		if configs.shouldCountLines {
			t.Error("Count line flag expected to be false if not set")
		}
	})
}

func TestGetResultsReport(t *testing.T) {
	t.Run("all stats count report should be printed if no flag is set", func(t *testing.T) {
		configs := WcConfigs{numberOfFlagsSet: 0, shouldCountBytes: false, shouldCountLines: false}
		results := WcResult{name: "test.txt", byteCount: 342190, lineCount: 7145}

		want := "342190 7145 test.txt"
		got := getResultsReport(configs, results)

		if want != got {
			t.Errorf("got '%s' want '%s'", got, want)
		}
	})

	t.Run("byte and line count report should be printed if set", func(t *testing.T) {
		configs := WcConfigs{numberOfFlagsSet: 2, shouldCountBytes: true, shouldCountLines: true}
		results := WcResult{name: "test.txt", byteCount: 342190, lineCount: 7145}

		want := "342190 7145 test.txt"
		got := getResultsReport(configs, results)

		if want != got {
			t.Errorf("got '%s' want '%s'", got, want)
		}
	})

	t.Run("byte count report should be printed if set in isolation", func(t *testing.T) {
		configs := WcConfigs{numberOfFlagsSet: 1, shouldCountBytes: true, shouldCountLines: false}
		results := WcResult{name: "test.txt", byteCount: 342190, lineCount: 7145}

		want := "342190 test.txt"
		got := getResultsReport(configs, results)

		if want != got {
			t.Errorf("got '%s' want '%s'", got, want)
		}
	})

	t.Run("line count report should be printed if set in isolation", func(t *testing.T) {
		configs := WcConfigs{numberOfFlagsSet: 1, shouldCountBytes: false, shouldCountLines: true}
		results := WcResult{name: "test.txt", byteCount: 342190, lineCount: 7145}

		want := "7145 test.txt"
		got := getResultsReport(configs, results)

		if want != got {
			t.Errorf("got '%s' want '%s'", got, want)
		}
	})
}
