package app

import (
	"bytes"
	"encoding/csv"
	"reflect"
	"strings"
	"testing"
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
	r := csv.NewReader(strings.NewReader(testRecord1))
	h, err := NewHeader(r)
	if err != nil {
		t.Fatal(err)
	}

	want := CSVHeader{
		"A": 0,
		"B": 1,
		"C": 2,
	}

	if !reflect.DeepEqual(h, want) {
		t.Fatalf("got %q; want %q", h, want)
	}
}

func TestExtractField(t *testing.T) {
	r := csv.NewReader(strings.NewReader(testRecord1))
	h, err := NewHeader(r)
	if err != nil {
		t.Fatal(err)
	}

	record, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]string{
		"A": "1",
		"B": "2",
		"C": "3",
	}

	for k, want := range tests {
		got := h.extractField(k, record)
		if got != want {
			t.Fatalf("got %q; want %q", got, want)
		}
	}
}

func TestGenUniqID(t *testing.T) {
	rec := &CSVRecord{
		TimeID:    "A",
		DstIP:     "1.1.1.1",
		ProtoName: "GOOGLE",
	}
	id := rec.genUniqID()
	want := "A-1.1.1.1-GOOGLE"
	if id != want {
		t.Fatalf("got %q; want %q", id, want)
	}
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
	rec, err := makeTestRecord()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("TimeID", func(t *testing.T) {
		want := "2017-04-26-11"
		if rec.TimeID != want {
			t.Fatalf("got %q; want %q", rec.TimeID, want)
		}
	})

	t.Run("DstIP", func(t *testing.T) {
		want := "172.19.1.46"
		if rec.DstIP != want {
			t.Fatalf("got %q; want %q", rec.DstIP, want)
		}
	})

	t.Run("ProtoName", func(t *testing.T) {
		want := "HTTP_PROXY"
		if rec.ProtoName != want {
			t.Fatalf("got %q; want %q", rec.ProtoName, want)
		}
	})

	t.Run("ID", func(t *testing.T) {
		want := rec.genUniqID()
		if rec.ID != want {
			t.Fatalf("got %q; want %q", rec.ID, want)
		}
	})

	t.Run("Packets", func(t *testing.T) {
		want := uint64(77)
		if rec.Packets != want {
			t.Fatalf("got %d; want %d", rec.Packets, want)
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		want := uint64(110546)
		if rec.Bytes != want {
			t.Fatalf("got %d; want %d", rec.Bytes, want)
		}
	})
}

func TestExtractCounters(t *testing.T) {
	rec, err := makeTestRecord()
	if err != nil {
		t.Fatal(err)
	}

	testRecord := []string{"", "", "3e+05", "1"}
	got, err := rec.extractCounters(
		testRecord, "Total.Fwd.Packets", "Total.Backward.Packets")
	if err != nil {
		t.Fatal(err)
	}

	want := uint64(300001)
	if got != want {
		t.Fatalf("extractCounters can't parse 3e+05: got %d; want %d", got, want)
	}
}

func TestAdd(t *testing.T) {
	rec := &CSVRecord{Packets: 1, Bytes: 2}
	rec.Add(&CSVRecord{Packets: 3, Bytes: 4})

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

func TestWriteCSV(t *testing.T) {
	rec, err := makeTestRecord()
	if err != nil {
		t.Fatal(err)
	}

	b := new(bytes.Buffer)
	w := csv.NewWriter(b)
	rec.WriteCSV(w)
	w.Flush()

	want := "2017-04-26-11,172.19.1.46,HTTP_PROXY,77,110546\n"
	if b.String() != want {
		t.Fatalf("got %q; want %q", b.String(), want)
	}
}

func TestWriteCSVHeader(t *testing.T) {
	b := new(bytes.Buffer)
	w := csv.NewWriter(b)
	WriteCSVHeader(w)
	w.Flush()

	want := strings.Join(outHeaderRecord, ",") + "\n"
	if b.String() != want {
		t.Fatalf("got %q; want %q", b.String(), want)
	}
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
	rec, err := makeTestRecordCompact()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("TimeID", func(t *testing.T) {
		want := "2017-04-26-11"
		if rec.TimeID != want {
			t.Fatalf("got %q; want %q", rec.TimeID, want)
		}
	})

	t.Run("DstIP", func(t *testing.T) {
		want := "172.19.1.46"
		if rec.DstIP != want {
			t.Fatalf("got %q; want %q", rec.DstIP, want)
		}
	})

	t.Run("ProtoName", func(t *testing.T) {
		want := "HTTP_PROXY"
		if rec.ProtoName != want {
			t.Fatalf("got %q; want %q", rec.ProtoName, want)
		}
	})

	t.Run("ID", func(t *testing.T) {
		want := rec.genUniqID()
		if rec.ID != want {
			t.Fatalf("got %q; want %q", rec.ID, want)
		}
	})

	t.Run("Packets", func(t *testing.T) {
		want := uint64(77)
		if rec.Packets != want {
			t.Fatalf("got %d; want %d", rec.Packets, want)
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		want := uint64(110546)
		if rec.Bytes != want {
			t.Fatalf("got %d; want %d", rec.Bytes, want)
		}
	})
}
