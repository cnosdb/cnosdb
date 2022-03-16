package server

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/tinylib/msgp/msgp"
)

// ResponseWriter is an interface for writing a response.
type ResponseWriter interface {
	// WriteResponse writes a response.
	WriteResponse(resp Response) (int, error)

	http.ResponseWriter
}

// NewResponseWriter creates a new ResponseWriter based on the Accept header
// in the request that wraps the ResponseWriter.
func NewResponseWriter(w http.ResponseWriter, r *http.Request) ResponseWriter {
	pretty := r.URL.Query().Get("pretty") == "true"
	rw := &responseWriter{ResponseWriter: w}
	switch r.Header.Get("Accept") {
	case "application/csv", "text/csv":
		w.Header().Add("Content-Type", "text/csv")
		rw.formatter = &csvFormatter{statementID: -1}
	case "application/x-msgpack":
		w.Header().Add("Content-Type", "application/x-msgpack")
		rw.formatter = &msgpackFormatter{}
	case "application/json":
		fallthrough
	default:
		w.Header().Add("Content-Type", "application/json")
		rw.formatter = &jsonFormatter{Pretty: pretty}
	}
	return rw
}

type bytesCountWriter struct {
	w io.Writer
	n int
}

func (w *bytesCountWriter) Write(data []byte) (int, error) {
	n, err := w.w.Write(data)
	w.n += n
	return n, err
}

// responseWriter is an implementation of ResponseWriter.
type responseWriter struct {
	formatter interface {
		WriteResponse(w io.Writer, resp Response) error
	}
	http.ResponseWriter
}

// WriteResponse writes the response using the formatter.
func (w *responseWriter) WriteResponse(resp Response) (int, error) {
	writer := bytesCountWriter{w: w.ResponseWriter}
	err := w.formatter.WriteResponse(&writer, resp)
	return writer.n, err
}

// Flush flushes the ResponseWriter if it has a Flush() method.
func (w *responseWriter) Flush() {
	if w, ok := w.ResponseWriter.(http.Flusher); ok {
		w.Flush()
	}
}

// CloseNotify calls CloseNotify on the underlying http.ResponseWriter if it
// exists. Otherwise, it returns a nil channel that will never notify.
func (w *responseWriter) CloseNotify() <-chan bool {
	if notifier, ok := w.ResponseWriter.(http.CloseNotifier); ok {
		return notifier.CloseNotify()
	}
	return nil
}

type jsonFormatter struct {
	Pretty bool
}

func (f *jsonFormatter) WriteResponse(w io.Writer, resp Response) (err error) {
	var b []byte
	if f.Pretty {
		b, err = json.MarshalIndent(resp, "", "    ")
	} else {
		b, err = json.Marshal(resp)
	}

	if err != nil {
		_, err = io.WriteString(w, err.Error())
	} else {
		_, err = w.Write(b)
	}

	_, _ = w.Write([]byte("\n"))
	return err
}

type csvFormatter struct {
	statementID int
	columns     []string
}

func (f *csvFormatter) WriteResponse(w io.Writer, resp Response) (err error) {
	cw := csv.NewWriter(w)
	if resp.Err != nil {
		_ = cw.Write([]string{"error"})
		_ = cw.Write([]string{resp.Err.Error()})
		cw.Flush()
		return cw.Error()
	}

	for _, result := range resp.Results {
		if result.StatementID != f.statementID {
			// If there are no series in the result, skip past this result.
			if len(result.Series) == 0 {
				continue
			}

			// Set the statement id and print out a newline if this is not the first statement.
			if f.statementID >= 0 {
				// Flush the csv writer and write a newline.
				cw.Flush()
				if err := cw.Error(); err != nil {
					return err
				}

				if _, err := io.WriteString(w, "\n"); err != nil {
					return err
				}
			}
			f.statementID = result.StatementID

			// Print out the column headers from the first series.
			f.columns = make([]string, 2+len(result.Series[0].Columns))
			f.columns[0] = "name"
			f.columns[1] = "tags"
			copy(f.columns[2:], result.Series[0].Columns)
			if err := cw.Write(f.columns); err != nil {
				return err
			}
		}

		for i, row := range result.Series {
			if i > 0 && !stringsEqual(result.Series[i-1].Columns, row.Columns) {
				// The columns have changed. Print a newline and reprint the header.
				cw.Flush()
				if err := cw.Error(); err != nil {
					return err
				}

				if _, err := io.WriteString(w, "\n"); err != nil {
					return err
				}

				f.columns = make([]string, 2+len(row.Columns))
				f.columns[0] = "name"
				f.columns[1] = "tags"
				copy(f.columns[2:], row.Columns)
				if err := cw.Write(f.columns); err != nil {
					return err
				}
			}

			f.columns[0] = row.Name
			if len(row.Tags) > 0 {
				f.columns[1] = string(models.NewTags(row.Tags).HashKey()[1:])
			} else {
				f.columns[1] = ""
			}
			for _, values := range row.Values {
				for i, value := range values {
					if value == nil {
						f.columns[i+2] = ""
						continue
					}

					switch v := value.(type) {
					case float64:
						f.columns[i+2] = strconv.FormatFloat(v, 'f', -1, 64)
					case int64:
						f.columns[i+2] = strconv.FormatInt(v, 10)
					case uint64:
						f.columns[i+2] = strconv.FormatUint(v, 10)
					case string:
						f.columns[i+2] = v
					case bool:
						if v {
							f.columns[i+2] = "true"
						} else {
							f.columns[i+2] = "false"
						}
					case time.Time:
						f.columns[i+2] = strconv.FormatInt(v.UnixNano(), 10)
					case *float64, *int64, *string, *bool:
						f.columns[i+2] = ""
					}
				}
				_ = cw.Write(f.columns)
			}
		}
	}
	cw.Flush()
	return cw.Error()
}

type msgpackFormatter struct{}

func (f *msgpackFormatter) ContentType() string {
	return "application/x-msgpack"
}

func (f *msgpackFormatter) WriteResponse(w io.Writer, resp Response) (err error) {
	enc := msgp.NewWriter(w)
	defer enc.Flush()

	_ = enc.WriteMapHeader(1)
	if resp.Err != nil {
		_ = enc.WriteString("error")
		_ = enc.WriteString(resp.Err.Error())
		return nil
	} else {
		_ = enc.WriteString("results")
		_ = enc.WriteArrayHeader(uint32(len(resp.Results)))
		for _, result := range resp.Results {
			if result.Err != nil {
				_ = enc.WriteMapHeader(1)
				_ = enc.WriteString("error")
				_ = enc.WriteString(result.Err.Error())
				continue
			}

			sz := 2
			if len(result.Messages) > 0 {
				sz++
			}
			if result.Partial {
				sz++
			}
			_ = enc.WriteMapHeader(uint32(sz))
			_ = enc.WriteString("statement_id")
			_ = enc.WriteInt(result.StatementID)
			if len(result.Messages) > 0 {
				_ = enc.WriteString("messages")
				_ = enc.WriteArrayHeader(uint32(len(result.Messages)))
				for _, msg := range result.Messages {
					_ = enc.WriteMapHeader(2)
					_ = enc.WriteString("level")
					_ = enc.WriteString(msg.Level)
					_ = enc.WriteString("text")
					_ = enc.WriteString(msg.Text)
				}
			}
			_ = enc.WriteString("series")
			_ = enc.WriteArrayHeader(uint32(len(result.Series)))
			for _, series := range result.Series {
				sz := 2
				if series.Name != "" {
					sz++
				}
				if len(series.Tags) > 0 {
					sz++
				}
				if series.Partial {
					sz++
				}
				_ = enc.WriteMapHeader(uint32(sz))
				if series.Name != "" {
					_ = enc.WriteString("name")
					_ = enc.WriteString(series.Name)
				}
				if len(series.Tags) > 0 {
					_ = enc.WriteString("tags")
					_ = enc.WriteMapHeader(uint32(len(series.Tags)))
					for k, v := range series.Tags {
						_ = enc.WriteString(k)
						_ = enc.WriteString(v)
					}
				}
				_ = enc.WriteString("columns")
				_ = enc.WriteArrayHeader(uint32(len(series.Columns)))
				for _, col := range series.Columns {
					_ = enc.WriteString(col)
				}
				_ = enc.WriteString("values")
				_ = enc.WriteArrayHeader(uint32(len(series.Values)))
				for _, values := range series.Values {
					_ = enc.WriteArrayHeader(uint32(len(values)))
					for _, v := range values {
						_ = enc.WriteIntf(v)
					}
				}
				if series.Partial {
					_ = enc.WriteString("partial")
					_ = enc.WriteBool(series.Partial)
				}
			}
			if result.Partial {
				_ = enc.WriteString("partial")
				_ = enc.WriteBool(true)
			}
		}
	}
	return nil
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
