package app

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

// NewHeader reads CSV header line from r and returns initialized [csvHeader]
func NewHeader(r *csv.Reader) (CSVHeader, error) {
	record, err := r.Read()
	if err != nil {
		return nil, err
	}

	header := make(CSVHeader, len(record))
	for i := 0; i < len(record); i++ {
		header[record[i]] = i
	}

	return header, nil
}

// CSVHeader keeps column number (or field index) for every field of .csv
// file. Using it we can map field name to index of value of that field.
type CSVHeader map[string]int

// extractField returns value for field from record
func (self CSVHeader) extractField(field string, record []string) string {
	idx := self[field]
	return record[idx]
}

// NewRecord parses line from csv file r according to its header h and returns
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
func NewRecord(h CSVHeader, r *csv.Reader) (*CSVRecord, error) {
	record, err := r.Read()
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	rec := &CSVRecord{
		h:         h,
		DstIP:     h.extractField("Destination.IP", record),
		ProtoName: h.extractField("ProtocolName", record),
	}
	rec.fillID(record)

	packets, err := rec.extractCounters(
		record, "Total.Fwd.Packets", "Total.Backward.Packets")
	if err != nil {
		return nil, err
	}
	rec.Packets = packets

	bytes, err := rec.extractCounters(
		record, "Total.Length.of.Fwd.Packets", "Total.Length.of.Bwd.Packets")
	if err != nil {
		return nil, err
	}
	rec.Bytes = bytes

	return rec, nil
}

// CSVRecord keeps data for one flow aggregated by day-hour, dst IP and proto
// name. It keeps num of packets and bytes.
type CSVRecord struct {
	h         CSVHeader
	TimeID    string // day-hour ID
	ID        string // uniq ID for this aggregation
	DstIP     string // destination IP
	ProtoName string // high level protocol name
	Packets   uint64 // num of packets
	Bytes     uint64 // num of bytes
}

// fillID extracts and assigns timeID, dstIP amd protoName. Using them is
// generates uniq id for this flow.
func (self *CSVRecord) fillID(record []string) error {
	t, err := time.Parse("2/01/200615:04:05", self.h.extractField("Timestamp", record))
	if err != nil {
		return err
	}

	self.TimeID = t.Format("2006-01-02-15")
	self.DstIP = self.h.extractField("Destination.IP", record)
	self.ProtoName = self.h.extractField("ProtocolName", record)
	self.ID = self.genUniqID()

	return nil
}

// genUniqID generates uniq ID based on values of timeID, dstIP and protoName
func (self *CSVRecord) genUniqID() string {
	return fmt.Sprintf("%s-%s-%s", self.TimeID, self.DstIP, self.ProtoName)
}

// extractCounters returns sum of values of fields f1 and f2 from record. It
// converts them from text to uint64 before adding.
func (self *CSVRecord) extractCounters(
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

func (self *CSVRecord) Add(netflow *CSVRecord) {
	self.Bytes += netflow.Bytes
	self.Packets += netflow.Packets
}

// writeCSV writes internal data as line of CSV into w
func (self *CSVRecord) WriteCSV(w *csv.Writer) error {
	record := []string{
		self.TimeID,
		self.DstIP,
		self.ProtoName,
		strconv.FormatUint(self.Packets, 10),
		strconv.FormatUint(self.Bytes, 10),
	}
	if err := w.Write(record); err != nil {
		return err
	}

	return nil
}

// WriteCSVHeader writes [outHeaderRecord] as header line of CSV into w
func WriteCSVHeader(w *csv.Writer) error {
	if err := w.Write(outHeaderRecord); err != nil {
		return err
	}
	return nil
}

// NewRecordCompact is a light verion of [newRecord]. It parses line from our
// intemediate csv file r according to its header h and returns it as
// [*csvRecord]. It's simplier, because it actualy reads back output of
// [writeCSV].
func NewRecordCompact(h CSVHeader, r *csv.Reader) (*CSVRecord, error) {
	record, err := r.Read()
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	rec := &CSVRecord{
		h:         h,
		TimeID:    h.extractField("Timestamp", record),
		DstIP:     h.extractField("Destination.IP", record),
		ProtoName: h.extractField("ProtocolName", record),
	}
	rec.ID = rec.genUniqID()

	// We can use ParseUint here, because we can be sure, we never meet "3e+05"
	// here, or something else, what ParseUint can't handle, because we wrote it
	// using FormatUint.
	packets, err := strconv.ParseUint(h.extractField("Packets", record), 10, 64)
	if err != nil {
		return nil, err
	}
	rec.Packets = packets

	bytes, err := strconv.ParseUint(h.extractField("Bytes", record), 10, 64)
	if err != nil {
		return nil, err
	}
	rec.Bytes = bytes

	return rec, nil
}
