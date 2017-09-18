package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/tsunami42/influxdb/models"
	"github.com/tsunami42/influxdb/pkg/escape"
	"github.com/tsunami42/influxdb/tsdb/engine/tsm1"
)

var (
	tsmPath  string
	compress bool
	outPath  string
	db       string
	rp       string
	limit    int
)

func init() {
	flag.StringVar(&tsmPath, "p", "a.tsm", "path for tsm file")
	flag.BoolVar(&compress, "c", false, "Compress the output")
	flag.StringVar(&db, "db", "telegraf", "Database of tsm file")
	flag.StringVar(&rp, "rp", "default", "RP of tsm file")
	flag.IntVar(&limit, "l", 100, "read key limit")
}

func main() {
	flag.Parse()

	log.Println(tsmPath)
	if err := write(); err != nil {
		log.Fatal(err)
	}
}

func write() error {
	// Because calling (*os.File).Write is relatively expensive,
	// and we don't *need* to sync to disk on every written line of export,
	// use a sized buffered writer so that we only sync the file every megabyte.
	bw := bufio.NewWriterSize(os.Stdout, 1024*1024)
	defer bw.Flush()

	var w io.Writer = bw

	if compress {
		gzw := gzip.NewWriter(w)
		defer gzw.Close()
		w = gzw
	}

	if err := writeTsmFile(w, tsmPath); err != nil {
		return err
	}

	return nil

}

func writeTsmFile(w io.Writer, tsmFilePath string) error {
	f, err := os.Open(tsmFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		log.Printf("unable to read %s, skipping: %s\n", tsmFilePath, err.Error())
		return nil
	}
	defer r.Close()

	fmt.Fprintln(w, "# DML")
	fmt.Fprintf(w, "# CONTEXT-DATABASE:%s\n", db)
	fmt.Fprintf(w, "# CONTEXT-RETENTION-POLICY:%s\n", rp)

	for i := 0; i < r.KeyCount(); i++ {
		if i > limit {
			break
		}
		key, _ := r.KeyAt(i)
		values, err := r.ReadAll(string(key))
		if err != nil {
			log.Printf("unable to read key %q in %s, skipping: %s\n", string(key), tsmFilePath, err.Error())
			continue
		}
		measurement, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		field = escape.Bytes(field)

		if err := writeValues(w, measurement, string(field), values); err != nil {
			// An error from writeValues indicates an IO error, which should be returned.
			return err
		}
	}
	return nil
}

func writeValues(w io.Writer, seriesKey []byte, field string, values []tsm1.Value) error {
	buf := []byte(string(seriesKey) + " " + field + "=")
	prefixLen := len(buf)

	for _, value := range values {
		ts := value.UnixNano()

		// Re-slice buf to be "<series_key> <field>=".
		buf = buf[:prefixLen]

		// Append the correct representation of the value.
		switch v := value.Value().(type) {
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
			buf = append(buf, 'i')
		case bool:
			buf = strconv.AppendBool(buf, v)
		case string:
			buf = append(buf, '"')
			buf = append(buf, models.EscapeStringField(v)...)
			buf = append(buf, '"')
		default:
			// This shouldn't be possible, but we'll format it anyway.
			buf = append(buf, fmt.Sprintf("%v", v)...)
		}

		// Now buf has "<series_key> <field>=<value>".
		// Append the timestamp and a newline, then write it.
		buf = append(buf, ' ')
		buf = strconv.AppendInt(buf, ts, 10)
		buf = append(buf, '\n')
		if _, err := w.Write(buf); err != nil {
			// Underlying IO error needs to be returned.
			return err
		}
	}

	return nil
}
