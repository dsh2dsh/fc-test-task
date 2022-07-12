package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"time"
)

// The header line our output .csv files
var outHeaderRecord = []string{
	"Timestamp",
	"Destination.IP",
	"ProtocolName",
	"Packets",
	"Bytes",
}

// newHeader reads CSV header line from r and returns initialized [csvHeader]
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

// csvHeader keeps column number (or field index) for every field of .csv
// file. Using it we can map field name to index of value of that field.
type csvHeader map[string]int

// extractField returns value for field from record
func (self csvHeader) extractField(field string, record []string) string {
	idx := self[field]
	return record[idx]
}

// newRecord parses line from csv file r according to its header h and returns
// it as [*csvRecord]. It extracts values for
//
//   * Timestamp
//   * Destination.IP
//   * ProtocolName
//   * Total.Fwd.Packets + Total.Backward.Packets
//   * Total.Length.of.Fwd.Packets + Total.Length.of.Bwd.Packets
//
// I suppose Total.Fwd.Packets + Total.Backward.Packets is num of packets in
// this line and Total.Length.of.Fwd.Packets + Total.Length.of.Bwd.Packets is
// num of bytes.
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

// csvRecord keeps data for one flow aggregated by day-hour, dst IP and proto
// name. It keeps num of packets and bytes.
type csvRecord struct {
	h         csvHeader
	timeID    string // day-hour ID
	id        string // uniq ID for this aggregation
	dstIP     string // destination IP
	protoName string // high level protocol name
	packets   uint64 // num of packets
	bytes     uint64 // num of bytes
}

// fillID extracts and assigns timeID, dstIP amd protoName. Using them is
// generates uniq id for this flow.
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

// genUniqID generates uniq ID based on values of timeID, dstIP and protoName
func (self *csvRecord) genUniqID() string {
	return fmt.Sprintf("%s-%s-%s", self.timeID, self.dstIP, self.protoName)
}

// extractCounters returns sum of values of fields f1 and f2 from record. It
// converts them from text to uint64 before adding.
func (self *csvRecord) extractCounters(
	record []string, f1 string, f2 string,
) (uint64, error) {
	// strconv.ParseUint() can't parse "3e+05", use big.ParseFloat() instead.
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

// writeCSV writes internal data as line of CSV into w
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

// writeCSVHeader writes [outHeaderRecord] as header line of CSV into w
func writeCSVHeader(w *csv.Writer) error {
	if err := w.Write(outHeaderRecord); err != nil {
		return err
	}
	return nil
}

// newRecordCompact is a light verion of [newRecord]. It parses line from our
// intemediate csv file r according to its header h and returns it as
// [*csvRecord]. It's simplier, because it actualy reads back output of
// [writeCSV].
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

	// We can use ParseUint here, because we can be sure, we never meet "3e+05"
	// here, or something else, what ParseUint can't handle, because we wrote it
	// using FormatUint.
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
