package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
)

const MAX_FLAGS_NUMBER = 3

type WcConfigs struct {
	in               *os.File
	shouldCountBytes bool
	shouldCountLines bool
	shouldCountWords bool
	numberOfFlagsSet int
}

func (c *WcConfigs) parseFlagsAndFileName(programName string, args []string) (string, error) {
	flags := flag.NewFlagSet(programName, flag.ContinueOnError)
	flags.BoolVar(&c.shouldCountBytes, "c", false, "print the bytes count")
	flags.BoolVar(&c.shouldCountLines, "l", false, "print the line count")
	flags.BoolVar(&c.shouldCountWords, "w", false, "print the word count")

	err := flags.Parse(args)
	if err != nil {
		return "", err
	}

	c.numberOfFlagsSet = 0
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "c", "l", "w":
			c.numberOfFlagsSet += 1
		}
	})

	c.flipAllFlagsIfNoneSet()
	filename := flags.Arg(0)
	return filename, err
}

func (c *WcConfigs) checkIfFlagIsIsolated(flag string) bool {
	isIsolated := false

	switch flag {
	case "c":
		isIsolated = c.shouldCountBytes && c.numberOfFlagsSet == 1
	case "l":
		isIsolated = c.shouldCountLines && c.numberOfFlagsSet == 1
	case "w":
		isIsolated = c.shouldCountWords && c.numberOfFlagsSet == 1
	default:
		isIsolated = false
	}

	return isIsolated
}

func (c *WcConfigs) flipAllFlagsIfNoneSet() {

	if c.numberOfFlagsSet == 0 {
		c.shouldCountBytes = true
		c.shouldCountLines = true
		c.shouldCountWords = true
		c.numberOfFlagsSet = MAX_FLAGS_NUMBER
	}
}

type WcResult struct {
	name      string
	byteCount int
	lineCount int
	wordCount int
}

var defaultWcResult = WcResult{
	name:      "",
	byteCount: 0,
	lineCount: 0,
	wordCount: 0,
}

func openFile(filename string) (*os.File, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func getNumberOfLines(file *os.File) int {
	_, err := file.Seek(0, 0)
	if err != nil {
		return 0
	}
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	var lines int
	for scanner.Scan() {
		lines++
	}

	return lines
}

func getNumberOfWords(file *os.File) int {
	_, err := file.Seek(0, 0)
	if err != nil {
		return 0
	}
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)

	var words int
	for scanner.Scan() {
		words++
	}
	return words
}

func DoWc(file *os.File) (WcResult, error) {
	info, err := file.Stat()
	if err != nil {
		return defaultWcResult, err
	}
	lines := getNumberOfLines(file)
	words := getNumberOfWords(file)
	return WcResult{name: file.Name(), byteCount: int(info.Size()), lineCount: lines, wordCount: words}, nil
}

func getResultsReport(configs WcConfigs, results WcResult) string {
	report := results.name

	if configs.numberOfFlagsSet == 0 || configs.numberOfFlagsSet == MAX_FLAGS_NUMBER {
		report = fmt.Sprintf("%d %d %d %s", results.byteCount, results.lineCount, results.wordCount, report)
	} else if configs.numberOfFlagsSet == 1 {
		if configs.checkIfFlagIsIsolated("c") {
			report = fmt.Sprintf("%d %s", results.byteCount, report)
		} else if configs.checkIfFlagIsIsolated("l") {
			report = fmt.Sprintf("%d %s", results.lineCount, report)
		} else if configs.checkIfFlagIsIsolated("w") {
			report = fmt.Sprintf("%d %s", results.wordCount, report)
		}

	} else {
		if configs.shouldCountWords {
			report = fmt.Sprintf("%d %s", results.wordCount, report)
		}

		if configs.shouldCountLines {
			report = fmt.Sprintf("%d %s", results.lineCount, report)
		}

		if configs.shouldCountBytes {
			report = fmt.Sprintf("%d %s", results.byteCount, report)
		}

	}

	return report
}
