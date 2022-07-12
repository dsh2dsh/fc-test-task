package main

import (
	"encoding/csv"
	"io/fs"
	"log"
	"os"
	"path"
)

func processCSVLowMem(r *csv.Reader, outPath string) {
	h, err := newHeader(r)
	if err != nil {
		log.Fatalln(err)
	}

	seenTimeID := newSeenHourData()
	for {
		netflow, err := newRecord(h, r)
		if err != nil {
			log.Fatalln(err)
		} else if netflow == nil && seenTimeID.firstTime() {
			break
		}

		if seenTimeID.firstTime() {
			seenTimeID.rememberTimeID(netflow)
			seenTimeID.resetHourData()
		} else if netflow == nil || seenTimeID.anotherHour(netflow) {
			if err := seenTimeID.flushHourData(outPath); err != nil {
				log.Fatalln(err)
			}
			if netflow == nil {
				break
			}
			seenTimeID.rememberTimeID(netflow)
		}

		seenTimeID.addHourData(netflow)
	}

	commitCSVLowMem(outPath)
}

func commitCSVLowMem(outPath string) {
	outFS := os.DirFS(outPath)
	files, err := fs.Glob(outFS, "*.csv")
	if err != nil {
		log.Fatalln(err)
	}

	for _, fname := range files {
		fname := path.Join(outPath, fname)
		file, err := os.Open(fname)
		if err != nil {
			log.Fatalln(err)
		}
		defer file.Close()
		r := csv.NewReader(file)
		if err := processSubCSV(r, outPath); err != nil {
			log.Fatalln(fname, err)
		}
	}
}

func processSubCSV(r *csv.Reader, outPath string) error {
	h, err := newHeader(r)
	if err != nil {
		return err
	}

	var curTimeID string
	data := make(hourData)

	for {
		netflow, err := newRecordCompact(h, r)
		if err != nil {
			return err
		} else if netflow == nil {
			break
		}
		if curTimeID == "" {
			curTimeID = netflow.timeID
		}
		if nf, present := data[netflow.id]; present {
			nf.bytes += netflow.bytes
			nf.packets += netflow.packets
		} else {
			data[netflow.id] = netflow
		}
	}

	if err := saveHourToFile(curTimeID, data, outPath); err != nil {
		return err
	}

	return nil
}
