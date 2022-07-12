package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"time"
)

var outHeaderRecord = []string{
	"Timestamp",
	"Destination.IP",
	"ProtocolName",
	"Packets",
	"Bytes",
}

func newHeader(r *csv.Reader) (csvHeader, error) {
	record, err := r.Read()
	if err != nil {
		return nil, err
	}

	header := make(csvHeader, len(record))
	for i := 0; i < len(record); i++ {
		header[record[i]] = i
	}

	return header, nil
}

type csvHeader map[string]int

func (self csvHeader) extractField(field string, record []string) string {
	idx := self[field]
	return record[idx]
}

func newRecord(h csvHeader, r *csv.Reader) (*csvRecord, error) {
	record, err := r.Read()
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	rec := &csvRecord{
		h:         h,
		dstIP:     h.extractField("Destination.IP", record),
		protoName: h.extractField("ProtocolName", record),
	}
	rec.fillID(record)

	packets, err := rec.extractCounters(
		record, "Total.Fwd.Packets", "Total.Backward.Packets")
	if err != nil {
		return nil, err
	}
	rec.packets = packets

	bytes, err := rec.extractCounters(
		record, "Total.Length.of.Fwd.Packets", "Total.Length.of.Bwd.Packets")
	if err != nil {
		return nil, err
	}
	rec.bytes = bytes

	return rec, nil
}

type csvRecord struct {
	h         csvHeader
	timeID    string
	id        string
	dstIP     string
	protoName string
	packets   uint64
	bytes     uint64
}

func (self *csvRecord) fillID(record []string) error {
	t, err := time.Parse("2/01/200615:04:05", self.h.extractField("Timestamp", record))
	if err != nil {
		return err
	}

	self.timeID = t.Format("2006-01-02-15")
	self.dstIP = self.h.extractField("Destination.IP", record)
	self.protoName = self.h.extractField("ProtocolName", record)
	self.id = self.genUniqID()

	return nil
}

func (self *csvRecord) genUniqID() string {
	return fmt.Sprintf("%s-%s-%s", self.timeID, self.dstIP, self.protoName)
}

func (self *csvRecord) extractCounters(
	record []string, f1 string, f2 string,
) (uint64, error) {
	// strconv.ParseUint() can't parse "3e+05", use big.ParseFloat() instead
	fwdF, _, err := big.ParseFloat(
		self.h.extractField(f1, record), 10, 0, big.ToNearestEven)
	if err != nil {
		return 0, err
	}
	fwd, _ := fwdF.Uint64()

	backF, _, err := big.ParseFloat(
		self.h.extractField(f2, record), 10, 0, big.ToNearestEven)
	if err != nil {
		return 0, err
	}
	back, _ := backF.Uint64()

	return fwd + back, nil
}

func (self *csvRecord) writeCSV(w *csv.Writer) error {
	record := []string{
		self.timeID,
		self.dstIP,
		self.protoName,
		strconv.FormatUint(self.packets, 10),
		strconv.FormatUint(self.bytes, 10),
	}
	if err := w.Write(record); err != nil {
		return err
	}

	return nil
}

func writeCSVHeader(w *csv.Writer) error {
	if err := w.Write(outHeaderRecord); err != nil {
		return err
	}
	return nil
}

func newRecordCompact(h csvHeader, r *csv.Reader) (*csvRecord, error) {
	record, err := r.Read()
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	rec := &csvRecord{
		h:         h,
		timeID:    h.extractField("Timestamp", record),
		dstIP:     h.extractField("Destination.IP", record),
		protoName: h.extractField("ProtocolName", record),
	}
	rec.id = rec.genUniqID()

	packets, err := strconv.ParseUint(h.extractField("Packets", record), 10, 64)
	if err != nil {
		return nil, err
	}
	rec.packets = packets

	bytes, err := strconv.ParseUint(h.extractField("Bytes", record), 10, 64)
	if err != nil {
		return nil, err
	}
	rec.bytes = bytes

	return rec, nil
}
