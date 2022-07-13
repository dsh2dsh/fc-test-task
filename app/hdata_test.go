package app

import (
	"encoding/csv"
	"os"
	"path"
	"reflect"
	"testing"
)

func TestHourDataAdd(t *testing.T) {
	data := make(HourData)

	data.Add(&CSVRecord{ID: "A", Packets: 1, Bytes: 2})
	if _, present := data["A"]; !present {
		t.Fatalf("want 'A' in data; got false")
	}

	data.Add(&CSVRecord{ID: "A", Packets: 3, Bytes: 4})
	rec := data["A"]

	t.Run("Packets", func(t *testing.T) {
		want := uint64(4)
		if rec.Packets != want {
			t.Fatalf("got %d; want %d", rec.Packets, want)
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		want := uint64(6)
		if rec.Bytes != want {
			t.Fatalf("got %d; want %d", rec.Bytes, want)
		}
	})
}

func TestNewSeenHourData(t *testing.T) {
	seenHD := NewSeenHourData()

	if !seenHD.FirstTime() {
		t.Fatalf("got FirstTime() == false; want true")
	}

	rec := &CSVRecord{TimeID: "123", ID: "A", Packets: 1, Bytes: 2}
	if !seenHD.AnotherHour(rec) {
		t.Fatalf("got FirstTime() == false; want true")
	}

	seenHD.RememberTimeID(rec)
	if seenHD.AnotherHour(rec) {
		t.Fatalf("got FirstTime() == true; want false")
	}
	if seenHD.timeID != "123" {
		t.Fatalf("got TimeID == %q; want '123'", seenHD.timeID)
	}

	if seenHD.hourData() != nil {
		t.Fatalf("got hourData() != nil; want nil")
	}

	seenHD.ResetHourData()
	if seenHD.hourData() == nil {
		t.Fatalf("got hourData() == nil; want not nil")
	}

	seenHD.AddHourData(rec)
	if _, present := seenHD.hourData()["A"]; !present {
		t.Fatalf("want 'A' in data; got false")
	}

	seenHD.AddHourData(&CSVRecord{TimeID: "123", ID: "A", Packets: 3, Bytes: 4})
	hd := seenHD.hourData()["A"]

	t.Run("Packets", func(t *testing.T) {
		want := uint64(4)
		if hd.Packets != want {
			t.Fatalf("got %d; want %d", hd.Packets, want)
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		want := uint64(6)
		if hd.Bytes != want {
			t.Fatalf("got %d; want %d", hd.Bytes, want)
		}
	})
}

func TestFlushHourData(t *testing.T) {
	netflow, err := makeTestRecordCompact()
	if err != nil {
		t.Fatal(err)
	}

	seenTimeID := NewSeenHourData()
	seenTimeID.RememberTimeID(netflow)
	seenTimeID.ResetHourData()
	seenTimeID.AddHourData(netflow)

	outPath := t.TempDir()
	if err := seenTimeID.FlushHourData(outPath); err != nil {
		t.Fatal(err)
	}

	fname := path.Join(outPath, netflow.TimeID+".csv")
	netflows, err := loadRecordsCompact(fname)
	if err != nil {
		t.Fatal(err)
	}
	if len(netflows) != 1 {
		t.Fatalf("got %d netflows; want 1", len(netflows))
	}
	if !reflect.DeepEqual(netflow, netflows[0]) {
		t.Fatalf("got %q; want %q", netflows[0], netflow)
	}

	seenTimeID.AddHourData(netflow)
	if err := seenTimeID.FlushHourData(outPath); err != nil {
		t.Fatal(err)
	}

	netflows, err = loadRecordsCompact(fname)
	if err != nil {
		t.Fatal(err)
	}
	if len(netflows) != 2 {
		t.Fatalf("got %d netflows; want 2", len(netflows))
	}
	for i := 0; i < len(netflows); i++ {
		if !reflect.DeepEqual(netflow, netflows[i]) {
			t.Fatalf("%d: got %q; want %q", i, netflows[i], netflow)
		}
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
