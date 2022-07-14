package app

import (
	"encoding/csv"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHourDataAdd(t *testing.T) {
	assert := assert.New(t)

	data := make(HourData)
	data.Add(&CSVRecord{ID: "A", Packets: 1, Bytes: 2})
	assert.Contains(data, "A")

	data.Add(&CSVRecord{ID: "A", Packets: 3, Bytes: 4})
	rec := data["A"]
	assert.Equal(rec.Packets, uint64(4))
	assert.Equal(rec.Bytes, uint64(6))
}

func TestNewSeenHourData(t *testing.T) {
	assert := assert.New(t)

	seenHD := NewSeenHourData()
	assert.True(seenHD.FirstTime())

	rec := &CSVRecord{TimeID: "123", ID: "A", Packets: 1, Bytes: 2}
	assert.True(seenHD.AnotherHour(rec))

	seenHD.RememberTimeID(rec)
	assert.False(seenHD.AnotherHour(rec))
	assert.Equal(seenHD.timeID, "123")
	assert.Nil(seenHD.hourData())

	seenHD.ResetHourData()
	assert.NotNil(seenHD.hourData())

	seenHD.AddHourData(rec)
	assert.Contains(seenHD.hourData(), "A")

	seenHD.AddHourData(&CSVRecord{TimeID: "123", ID: "A", Packets: 3, Bytes: 4})
	hd := seenHD.hourData()["A"]
	assert.Equal(hd.Packets, uint64(4))
	assert.Equal(hd.Bytes, uint64(6))
}

func TestFlushHourData(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	netflow, err := makeTestRecordCompact()
	require.NoError(err)

	seenTimeID := NewSeenHourData()
	seenTimeID.RememberTimeID(netflow)
	seenTimeID.ResetHourData()
	seenTimeID.AddHourData(netflow)

	outPath := t.TempDir()
	require.NoError(seenTimeID.FlushHourData(outPath))

	fname := path.Join(outPath, netflow.TimeID+".csv")
	netflows, err := loadRecordsCompact(fname)
	require.NoError(err)
	assert.Len(netflows, 1)
	assert.Equal(netflow, netflows[0])

	seenTimeID.AddHourData(netflow)
	require.NoError(seenTimeID.FlushHourData(outPath))

	netflows, err = loadRecordsCompact(fname)
	require.NoError(err)
	assert.Len(netflows, 2)
	for i := 0; i < len(netflows); i++ {
		require.Equal(netflow, netflows[i])
	}
}

func loadRecordsCompact(fname string) ([]*CSVRecord, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	r := csv.NewReader(file)

	h, err := NewHeader(r)
	if err != nil {
		return nil, err
	}

	records := make([]*CSVRecord, 0)
	for {
		if rec, err := NewRecordCompact(h, r); err == nil {
			if rec == nil {
				break
			}
			records = append(records, rec)
		} else {
			return nil, err
		}
	}

	return records, nil
}
