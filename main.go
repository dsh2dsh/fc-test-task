package main

import (
	"encoding/csv"
	"flag"
	"log"
	"os"

	"dsh/fc/app"
)

const (
	defLowMem = false // work faster by default
	defOutDir = "."   // output dir is current one by default

	// Usage strings for CLI options
	inCSVUsage  = "name of input .csv file"
	lowMemUsage = "slower, but use less RAM"
	outDirUsage = "dir for output .csv files"
)

var (
	inCSV  string // name of input .csv file
	lowMem bool   // use less RAM
	outDir string // name of output dir
)

func init() {
	flag.StringVar(&inCSV, "i", "", inCSVUsage)
	flag.StringVar(&outDir, "o", defOutDir, outDirUsage)

	flag.StringVar(&inCSV, "input", "", inCSVUsage)
	flag.BoolVar(&lowMem, "lowmem", defLowMem, lowMemUsage)
	flag.StringVar(&outDir, "output", defOutDir, outDirUsage)

	flag.Parse()

	// input .csv file is mandatory
	if inCSV == "" {
		flag.Usage()
		os.Exit(2)
	}
}

func main() {
	log.SetFlags(0) // disable datetime

	file, err := os.Open(inCSV)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	// Create output dir if it isn't exist. If it already exist MkdirAll does
	// nothing.
	if err := os.MkdirAll(outDir, 0777); err != nil {
		log.Fatalln(err)
	}

	r := csv.NewReader(file)
	r.ReuseRecord = true // Reuse some memory for performance

	// Depending on existence of --lowmem option use one of algorithms
	if lowMem {
		processCSVLowMem(r, outDir)
	} else {
		processCSV(r, outDir)
	}
}

// processCSV reads input .csv file, parses it and aggregates by day-hour, dest
// IP and proto name. It keeps aggregated data in memory and works faster. It
// saves aggregated data into .csv files named by day-hour.csv in outPath dir.
func processCSV(r *csv.Reader, outPath string) {
	h, err := app.NewHeader(r)
	if err != nil {
		log.Fatalln(err)
	}

	// In allData we keep all our aggregated data indexed by day-hour string
	allData := make(map[string]app.HourData)
	for {
		netflow, err := app.NewRecord(h, r)
		if err != nil {
			log.Fatalln(err)
		} else if netflow == nil {
			break
		}

		// For unknown day-hour we need to create a new one
		if _, present := allData[netflow.TimeID]; !present {
			allData[netflow.TimeID] = make(app.HourData)
		}
		// Add new values into aggregated data
		allData[netflow.TimeID].Add(netflow)
	}

	// Save every day-hour data into its .csv file in outPath dir
	for timeID, data := range allData {
		if err := app.SaveHourToFile(timeID, data, outPath); err != nil {
			log.Fatalln(err)
		}
	}
}
