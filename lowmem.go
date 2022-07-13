package main

import (
	"encoding/csv"
	"io/fs"
	"log"
	"os"
	"path"

	"dsh/fc/app"
)

// processCSVLowMem like [processCSV] reads input .csv file, parses it and
// aggregates by day-hour, dest IP and proto name. But it doesn't keep
// aggregated data in memory, so it uses less RAM and works a little slower.
//
// On the first step it divides input .csv file into many more-or-less
// aggregated day-hour.csv files. How much they'll be aggregated depends how the
// input file sorted. It reads the input file line by line, aggregates them in
// memory and flushes it into day-hour.csv, when new line is for another
// day-hour. It means it will work faster when the input file is sorted by date
// and it'll work slower when it interleaved.
//
// On the second step it reads every previous preprocessed file, aggregates it
// and writes aggregated data back into the same file, overwriting it.
func processCSVLowMem(r *csv.Reader, outPath string) {
	h, err := app.NewHeader(r)
	if err != nil {
		log.Fatalln(err)
	}

	// In seenTimeID we'll keep every day-hour string we already created .csv file
	// for. So when we'll meet same day-hour we'll know should we overwrite its
	// .csv file (which left from prev exec) or append into it if we flushed data
	// into it before.
	seenTimeID := app.NewSeenHourData()

	// Let's preprocess the input file into many intermediate files, aggregated
	// as much as possible.
	for {
		netflow, err := app.NewRecord(h, r)
		if err != nil {
			log.Fatalln(err)
		} else if netflow == nil && seenTimeID.FirstTime() {
			// We got EOF right after header line
			break
		}

		if seenTimeID.FirstTime() {
			// First data line after header line
			seenTimeID.RememberTimeID(netflow)
			seenTimeID.ResetHourData()
		} else if netflow == nil || seenTimeID.AnotherHour(netflow) {
			// End of input file or we got another day-hour line. In both cases we
			// need to flush current data into .csv file.
			if err := seenTimeID.FlushHourData(outPath); err != nil {
				log.Fatalln(err)
			}
			if netflow == nil {
				// End of input file. We finished preprocessing.
				break
			}
			// Begin another .csv file
			seenTimeID.RememberTimeID(netflow)
		}

		// Add new data into aggregated one
		seenTimeID.AddHourData(netflow)
	}

	// Now let's aggregate intermediate files
	commitCSVLowMem(outPath)
}

// commitCSVLowMem aggregates every .csv file in outPath dir and overwrites it
// with aggregated data for that day-hour. We can use log.Fatal here, because
// it's actually continuation of processCSVLowMem, it's a top level function.
func commitCSVLowMem(outPath string) {
	outFS := os.DirFS(outPath)
	files, err := fs.Glob(outFS, "*.csv")
	if err != nil {
		log.Fatalln(err)
	}

	// For every .csv
	for _, fname := range files {
		fname := path.Join(outPath, fname)
		file, err := os.Open(fname)
		if err != nil {
			log.Fatalln(err)
		}
		defer file.Close()
		r := csv.NewReader(file)
		r.ReuseRecord = true // Reuse some memory for performance
		// Aggregate it
		if err := processSubCSV(r, outPath); err != nil {
			log.Fatalln(fname, err)
		}
	}
}

// processSubCSV aggregates one intermediate .csv and writes it back into the
// same .csv file. It's a light version of [processCSV] designed to process just
// one .csv, which contains data for one day-hour only.
func processSubCSV(r *csv.Reader, outPath string) error {
	h, err := app.NewHeader(r)
	if err != nil {
		return err
	}

	var curTimeID string
	data := make(app.HourData)

	for {
		netflow, err := app.NewRecordCompact(h, r)
		if err != nil {
			return err
		} else if netflow == nil {
			break
		}
		if curTimeID == "" {
			// First line after header. We need to remember day-hour ID, because we'll
			// use it as name of the .csv file.
			curTimeID = netflow.TimeID
		}
		// Add new data into aggregated data or insert new data into the map if it's
		// new
		if nf, present := data[netflow.ID]; present {
			nf.Add(netflow)
		} else {
			data[netflow.ID] = netflow
		}
	}

	// End of intermediate file, let's overwrite it with aggregated data.
	if err := app.SaveHourToFile(curTimeID, data, outPath); err != nil {
		return err
	}

	return nil
}
