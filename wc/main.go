package main

import (
	"fmt"
	"os"
)

func main() {
	programName := os.Args[0]
	args := os.Args[1:]

	configs := WcConfigs{in: nil, shouldCountBytes: false, shouldCountLines: false}
	filename, err := configs.parseFlagsAndFileName(programName, args)
	if err != nil {
		fmt.Println("Failed to parse program flags. err: ", err)
		os.Exit(1)
	}

	if filename != "" {
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
