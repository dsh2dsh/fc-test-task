package app

import (
	"encoding/csv"
	"os"
	"path"
)

// HourData keeps aggregated data, indexed by day-hour.
type HourData map[string]*CSVRecord

// Add inserts new data into the map or adds new bytes and packets to existing
// data
func (self HourData) Add(netflow *CSVRecord) {
	if nf, present := self[netflow.ID]; present {
		nf.Add(netflow)
	} else {
		self[netflow.ID] = netflow
	}
}

// NewSeenHourData returns initialized [*seenHourData]
func NewSeenHourData() *SeenHourData {
	return &SeenHourData{
		seen: make(map[string]bool),
	}
}

// SeenHourData collects aggregated data for one day-hour and keeps a map of
// previously flushed day-hours.
type SeenHourData struct {
	timeID string          // current day-hour ID
	data   HourData        // aggregated data for this day-hour
	seen   map[string]bool // map of known day-hours
}

// FirstTime return true if this seenHourData just created and empty
func (self *SeenHourData) FirstTime() bool {
	return self.timeID == ""
}

// AnotherHour return true if netflow contains data for another day-hour
func (self *SeenHourData) AnotherHour(netflow *CSVRecord) bool {
	return self.timeID != netflow.TimeID
}

// RememberTimeID remembers day-hour from netflow, so we can use anotherHour to
// recognize netflow from next day-hour.
func (self *SeenHourData) RememberTimeID(netflow *CSVRecord) {
	self.timeID = netflow.TimeID
}

// hourData returns map of aggregated hourData
func (self *SeenHourData) hourData() HourData {
	return self.data
}

// ResetHourData resets internal storage for aggregated hourData. After that
// it's ready for collecting new data. Also should be called before first use of
// this seenHourData.
func (self *SeenHourData) ResetHourData() {
	self.data = make(HourData)
}

// FlushHourData saves aggregated data into outPath/timeID.csv file and resets
// internal storage. If we already saved data into this .csv file, it appends
// data into the file. If such file exist and we haven't yet saved any data into
// it, FlushHourData overwrites this file, because it means, this file left here
// from prev exec.
func (self *SeenHourData) FlushHourData(outPath string) error {
	curTimeID := self.timeID
	if _, present := self.seen[curTimeID]; present {
		if err := appendHourToFile(curTimeID, self.data, outPath); err != nil {
			return err
		}
	} else {
		if err := SaveHourToFile(curTimeID, self.data, outPath); err != nil {
			return err
		}
		self.seen[curTimeID] = true
	}
	self.ResetHourData()

	return nil
}

// AddHourData inserts new netflow into data map or adds new bytes and packets
// to existing data
func (self *SeenHourData) AddHourData(netflow *CSVRecord) {
	self.hourData().Add(netflow)
}

// SaveHourToFile saves data into outPath/timeID.csv. If such file exists it
// overwrites it. First line is a header line with name of fields.
func SaveHourToFile(timeID string, data HourData, outPath string) error {
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
func writeHourToFile(f *os.File, data HourData, header bool) error {
	w := csv.NewWriter(f)

	if header {
		if err := WriteCSVHeader(w); err != nil {
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
func writeHour(w *csv.Writer, data HourData) error {
	for _, v := range data {
		if err := v.WriteCSV(w); err != nil {
			return err
		}
	}

	return nil
}

// appendHourToFile appends data to outPath/timeID.csv file. We assume this file
// already has header line, because [saveHourToFile] added it, when created this
// file.
func appendHourToFile(timeID string, data HourData, outPath string) error {
	fname := path.Join(outPath, timeID+".csv")
	f, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := writeHourToFile(f, data, false); err != nil {
		return err
	}

	return nil
}
