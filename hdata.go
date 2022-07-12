package main

import (
	"os"
	"path"
)

type hourData map[string]*csvRecord

func newSeenHourData() *seenHourData {
	return &seenHourData{
		seen: make(map[string]bool),
	}
}

type seenHourData struct {
	timeID string
	data   hourData
	seen   map[string]bool
}

func (self *seenHourData) firstTime() bool {
	return self.timeID == ""
}

func (self *seenHourData) anotherHour(netflow *csvRecord) bool {
	return self.timeID != netflow.timeID
}

func (self *seenHourData) rememberTimeID(netflow *csvRecord) {
	self.timeID = netflow.timeID
}

func (self *seenHourData) hourData() hourData {
	return self.data
}

func (self *seenHourData) resetHourData() {
	self.data = make(hourData)
}

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

func (self *seenHourData) addHourData(netflow *csvRecord) {
	addHourData(self.hourData(), netflow)
}

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
