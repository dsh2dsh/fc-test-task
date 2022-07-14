package app

import (
	"bytes"
	"encoding/csv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testRecord1 = `A,B,C
1,2,3`

	testRecord2 = `Destination.IP,Timestamp,Total.Fwd.Packets,Total.Backward.Packets,Total.Length.of.Fwd.Packets,Total.Length.of.Bwd.Packets,ProtocolName
172.19.1.46,26/04/201711:11:17,22,55,132,110414,HTTP_PROXY`

	testRecord3 = `Timestamp,Destination.IP,ProtocolName,Packets,Bytes
2017-04-26-11,172.19.1.46,HTTP_PROXY,77,110546`
)

func TestNewHeader(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	r := csv.NewReader(strings.NewReader(testRecord1))
	h, err := NewHeader(r)
	require.NoError(err)

	want := CSVHeader{
		"A": 0,
		"B": 1,
		"C": 2,
	}
	assert.Equal(h, want)
}

func TestExtractField(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	r := csv.NewReader(strings.NewReader(testRecord1))
	h, err := NewHeader(r)
	require.NoError(err)

	record, err := r.Read()
	require.NoError(err)

	tests := map[string]string{
		"A": "1",
		"B": "2",
		"C": "3",
	}
	for k, want := range tests {
		assert.Equal(h.extractField(k, record), want)
	}
}

func TestGenUniqID(t *testing.T) {
	rec := &CSVRecord{
		TimeID:    "A",
		DstIP:     "1.1.1.1",
		ProtoName: "GOOGLE",
	}
	assert.Equal(t, rec.genUniqID(), "A-1.1.1.1-GOOGLE")
}

func makeTestRecord() (*CSVRecord, error) {
	r := csv.NewReader(strings.NewReader(testRecord2))
	h, err := NewHeader(r)
	if err != nil {
		return nil, err
	}

	rec, err := NewRecord(h, r)
	if err != nil {
		return nil, err
	}

	return rec, nil
}

func TestNewRecord(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	rec, err := makeTestRecord()
	require.NoError(err)

	want := &CSVRecord{
		h:         rec.h,
		TimeID:    "2017-04-26-11",
		DstIP:     "172.19.1.46",
		ProtoName: "HTTP_PROXY",
		ID:        rec.genUniqID(),
		Packets:   uint64(77),
		Bytes:     uint64(110546),
	}
	assert.Equal(rec, want)
}

func TestExtractCounters(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	rec, err := makeTestRecord()
	require.NoError(err)

	testRecord := []string{"", "", "3e+05", "1"}
	got, err := rec.extractCounters(
		testRecord, "Total.Fwd.Packets", "Total.Backward.Packets")
	require.NoError(err)
	assert.Equal(got, uint64(300001))
}

func TestAdd(t *testing.T) {
	assert := assert.New(t)

	rec := &CSVRecord{Packets: 1, Bytes: 2}
	rec.Add(&CSVRecord{Packets: 3, Bytes: 4})

	assert.Equal(rec.Packets, uint64(4))
	assert.Equal(rec.Bytes, uint64(6))
}

func TestWriteCSV(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	rec, err := makeTestRecord()
	require.NoError(err)

	b := new(bytes.Buffer)
	w := csv.NewWriter(b)
	rec.WriteCSV(w)
	w.Flush()

	assert.Equal(b.String(), "2017-04-26-11,172.19.1.46,HTTP_PROXY,77,110546\n")
}

func TestWriteCSVHeader(t *testing.T) {
	b := new(bytes.Buffer)
	w := csv.NewWriter(b)
	WriteCSVHeader(w)
	w.Flush()

	assert.Equal(t, b.String(), strings.Join(outHeaderRecord, ",")+"\n")
}

func makeTestRecordCompact() (*CSVRecord, error) {
	r := csv.NewReader(strings.NewReader(testRecord3))
	h, err := NewHeader(r)
	if err != nil {
		return nil, err
	}

	rec, err := NewRecordCompact(h, r)
	if err != nil {
		return nil, err
	}

	return rec, nil
}

func TestNewRecordCompact(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	rec, err := makeTestRecordCompact()
	require.NoError(err)

	want := &CSVRecord{
		h:         rec.h,
		TimeID:    "2017-04-26-11",
		DstIP:     "172.19.1.46",
		ProtoName: "HTTP_PROXY",
		ID:        rec.genUniqID(),
		Packets:   uint64(77),
		Bytes:     uint64(110546),
	}
	assert.Equal(rec, want)
}
