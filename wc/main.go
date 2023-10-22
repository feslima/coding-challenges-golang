package main

import (
	"flag"
	"fmt"
	"os"
)

type WcConfigs struct {
	in               *os.File
	shouldCountBytes bool
}

func (c *WcConfigs) getFlags() {
	flag.BoolVar(&c.shouldCountBytes, "c", true, "print the bytes count")
	flag.Parse()
}

type WcResult struct {
	name      string
	byteCount int
}

var defaultWcResult = WcResult{
	name:      "",
	byteCount: 0,
}

func openFile(filename string) (*os.File, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func DoWc(file *os.File) (WcResult, error) {
	info, err := file.Stat()
	if err != nil {
		return defaultWcResult, err
	}

	return WcResult{name: file.Name(), byteCount: int(info.Size())}, nil
}

func getResultsReport(configs WcConfigs, results WcResult) string {
	report := results.name

	report = fmt.Sprintf("%d %s", results.byteCount, report)

	return report
}

func main() {
	configs := WcConfigs{in: nil, shouldCountBytes: false}
	configs.getFlags()

	if filename := flag.Arg(0); filename != "" {
		file, err := openFile(filename)
		if err != nil {
			fmt.Println("Expected to open file without errors. err:", err)
			os.Exit(1)
		}
		defer file.Close()

		configs.in = file
	} else {
		configs.in = os.Stdin
	}

	results, err := DoWc(configs.in)
	if err != nil {
		fmt.Println("Failed to perform word count. err:", err)
		os.Exit(1)
	}

	fmt.Println(getResultsReport(configs, results))
}
