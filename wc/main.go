package main

import (
	"flag"
	"fmt"
	"os"
)

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
