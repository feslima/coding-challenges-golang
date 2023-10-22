package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
)

type WcConfigs struct {
	in               *os.File
	shouldCountBytes bool
	shouldCountLines bool
	numberOfFlagsSet int
}

func (c *WcConfigs) parseFlagsAndFileName(programName string, args []string) (string, error) {
	flags := flag.NewFlagSet(programName, flag.ContinueOnError)
	flags.BoolVar(&c.shouldCountBytes, "c", false, "print the bytes count")
	flags.BoolVar(&c.shouldCountLines, "l", false, "print the line count")

	err := flags.Parse(args)
	if err != nil {
		return "", err
	}

	c.numberOfFlagsSet = 0
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "c", "l":
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
	default:
		isIsolated = false
	}

	return isIsolated
}

func (c *WcConfigs) flipAllFlagsIfNoneSet() {

	if c.numberOfFlagsSet == 0 {
		c.shouldCountBytes = true
		c.shouldCountLines = true
		c.numberOfFlagsSet = 2
	}
}

type WcResult struct {
	name      string
	byteCount int
	lineCount int
}

var defaultWcResult = WcResult{
	name:      "",
	byteCount: 0,
	lineCount: 0,
}

func hasAll(values []bool) bool {
	if len(values) == 0 {
		return false
	}

	result := true

	for _, v := range values {
		if !v {
			result = false
			break
		}
	}

	return result
}

func openFile(filename string) (*os.File, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func getNumberOfLines(file *os.File) int {
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	var lines int
	for scanner.Scan() {
		lines++
	}

	return lines
}

func DoWc(file *os.File) (WcResult, error) {
	info, err := file.Stat()
	if err != nil {
		return defaultWcResult, err
	}
	lines := getNumberOfLines(file)
	return WcResult{name: file.Name(), byteCount: int(info.Size()), lineCount: lines}, nil
}

func getResultsReport(configs WcConfigs, results WcResult) string {
	report := results.name

	if configs.numberOfFlagsSet == 0 || configs.numberOfFlagsSet == 2 {
		report = fmt.Sprintf("%d %d %s", results.byteCount, results.lineCount, report)
	} else if configs.numberOfFlagsSet == 1 {
		if configs.checkIfFlagIsIsolated("c") {
			report = fmt.Sprintf("%d %s", results.byteCount, report)
		} else if configs.checkIfFlagIsIsolated("l") {
			report = fmt.Sprintf("%d %s", results.lineCount, report)
		}
	} else {
		if configs.shouldCountLines {
			report = fmt.Sprintf("%d %s", results.lineCount, report)
		}

		if configs.shouldCountBytes {
			report = fmt.Sprintf("%d %s", results.byteCount, report)
		}

	}

	return report
}
