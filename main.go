package main

import (
	"encoding/csv"
	"flag"
	"log"
	"os"
	"path"
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
	h, err := newHeader(r)
	if err != nil {
		log.Fatalln(err)
	}

	// In allData we keep all our aggregated data indexed by day-hour string
	allData := make(map[string]hourData)
	for {
		netflow, err := newRecord(h, r)
		if err != nil {
			log.Fatalln(err)
		} else if netflow == nil {
			break
		}

		// For unknown day-hour we need to create a new one
		if _, present := allData[netflow.timeID]; !present {
			allData[netflow.timeID] = make(hourData)
		}
		// Add new values into aggregated data
		addHourData(allData[netflow.timeID], netflow)
	}

	// Save every day-hour data into its .csv file in outPath dir
	for timeID, data := range allData {
		if err := saveHourToFile(timeID, data, outPath); err != nil {
			log.Fatalln(err)
		}
	}
}

// addHourData inserts new data into data map or adds new bytes and packets to
// existing data
func addHourData(data hourData, netflow *csvRecord) {
	if nf, present := data[netflow.id]; present {
		nf.bytes += netflow.bytes
		nf.packets += netflow.packets
	} else {
		data[netflow.id] = netflow
	}
}

// saveHourToFile saves data into outPath/timeID.csv. If such file exists it
// overwrites it. First line is a header line with name of fields.
func saveHourToFile(timeID string, data hourData, outPath string) error {
	fname := path.Join(outPath, timeID+".csv")
	f, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := writeHourToFile(f, data, true); err != nil {
		return err
	}

	return nil
}

// writeHourToFile writes data into file f. header flag shows does it need
// header line or doesn't. If header == true first line of the file is a header
// line with name of fields.
func writeHourToFile(f *os.File, data hourData, header bool) error {
	w := csv.NewWriter(f)

	if header {
		if err := writeCSVHeader(w); err != nil {
			return err
		}
	}

	if err := writeHour(w, data); err != nil {
		return err
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}

	return nil
}

// writeHour writes aggregated data in CSV format to w
func writeHour(w *csv.Writer, data hourData) error {
	for _, v := range data {
		if err := v.writeCSV(w); err != nil {
			return err
		}
	}

	return nil
}
