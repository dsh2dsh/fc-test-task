package main

import (
	"encoding/csv"
	"flag"
	"log"
	"os"
	"path"
)

const (
	defLowMem   = false
	defOutDir   = "."
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

	// inCSV is mandatory
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

	if err := os.MkdirAll(outDir, 0777); err != nil {
		log.Fatalln(err)
	}

	r := csv.NewReader(file)
	if lowMem {
		processCSVLowMem(r, outDir)
	} else {
		processCSV(r, outDir)
	}
}

func processCSV(r *csv.Reader, outPath string) {
	h, err := newHeader(r)
	if err != nil {
		log.Fatalln(err)
	}

	allData := make(map[string]hourData)
	for {
		netflow, err := newRecord(h, r)
		if err != nil {
			log.Fatalln(err)
		} else if netflow == nil {
			break
		}

		if _, present := allData[netflow.timeID]; !present {
			allData[netflow.timeID] = make(hourData)
		}
		addHourData(allData[netflow.timeID], netflow)
	}

	for timeID, data := range allData {
		if err := saveHourToFile(timeID, data, outPath); err != nil {
			log.Fatalln(err)
		}
	}
}

func addHourData(data hourData, netflow *csvRecord) {
	if nf, present := data[netflow.id]; present {
		nf.bytes += netflow.bytes
		nf.packets += netflow.packets
	} else {
		data[netflow.id] = netflow
	}
}

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

func writeHour(w *csv.Writer, data hourData) error {
	for _, v := range data {
		if err := v.writeCSV(w); err != nil {
			return err
		}
	}

	return nil
}
