package main

import (
	"os"
	"path"
)

// hourData keeps aggregated data, indexed by day-hour.
type hourData map[string]*csvRecord

// newSeenHourData returns initialized [*seenHourData]
func newSeenHourData() *seenHourData {
	return &seenHourData{
		seen: make(map[string]bool),
	}
}

// seenHourData collects aggregated data for one day-hour and keeps a map of
// previously flushed day-hours.
type seenHourData struct {
	timeID string          // current day-hour ID
	data   hourData        // aggregated data for this day-hour
	seen   map[string]bool // map of known day-hours
}

// firstTime return true if this seenHourData just created and empty
func (self *seenHourData) firstTime() bool {
	return self.timeID == ""
}

// anotherHour return true if netflow contains data for another day-hour
func (self *seenHourData) anotherHour(netflow *csvRecord) bool {
	return self.timeID != netflow.timeID
}

// rememberTimeID remembers day-hour from netflow, so we can use anotherHour to
// recognize netflow from next day-hour.
func (self *seenHourData) rememberTimeID(netflow *csvRecord) {
	self.timeID = netflow.timeID
}

// hourData returns map of aggregated hourData
func (self *seenHourData) hourData() hourData {
	return self.data
}

// resetHourData resets internal storage for aggregated hourData. After that
// it's ready for collecting new data. Also should be called before first use of
// this seenHourData.
func (self *seenHourData) resetHourData() {
	self.data = make(hourData)
}

// flushHourData saves aggregated data into outPath/timeID.csv file and resets
// internal storage. If we already saved data into this .csv file, it appends
// data into the file. If such file exist and we haven't yet saved any data into
// it, flushHourData overwrites this file, because it means, this file left here
// from prev exec.
func (self *seenHourData) flushHourData(outPath string) error {
	curTimeID := self.timeID
	if _, present := self.seen[curTimeID]; present {
		if err := appendHourToFile(curTimeID, self.data, outPath); err != nil {
			return err
		}
	} else {
		if err := saveHourToFile(curTimeID, self.data, outPath); err != nil {
			return err
		}
		self.seen[curTimeID] = true
	}
	self.resetHourData()

	return nil
}

// addHourData inserts new netflow into data map or adds new bytes and packets
// to existing data
func (self *seenHourData) addHourData(netflow *csvRecord) {
	addHourData(self.hourData(), netflow)
}

// appendHourToFile appends data to outPath/timeID.csv file. We assume this file
// already has header line, because [saveHourToFile] added it, when created this
// file.
func appendHourToFile(timeID string, data hourData, outPath string) error {
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
