package trdsql

import (
	"encoding/hex"
	"strings"
	"unicode/utf8"

	"github.com/goccy/go-yaml"
)

// YAMLWriter provides methods of the Writer interface.
type YAMLWriter struct {
	writer     *yaml.Encoder
	outNULL    string
	results    []yaml.MapSlice
	needNULL   bool
	jsonToYAML bool
}

// NewYAMLWriter returns YAMLWriter.
func NewYAMLWriter(writeOpts *WriteOpts) *YAMLWriter {
	w := &YAMLWriter{}
	w.writer = yaml.NewEncoder(writeOpts.OutStream)
	w.needNULL = writeOpts.OutNeedNULL
	w.outNULL = writeOpts.OutNULL
	w.jsonToYAML = writeOpts.OutJSONToYAML
	return w
}

// PreWrite is area preparation.
func (w *YAMLWriter) PreWrite(columns []string, types []string) error {
	w.results = make([]yaml.MapSlice, 0)
	return nil
}

// WriteRow is Addition to array.
func (w *YAMLWriter) WriteRow(values []interface{}, columns []string) error {
	m := make(yaml.MapSlice, len(values))
	for i, col := range values {
		m[i].Key = columns[i]
		m[i].Value = compatibleYAML(col, w.jsonToYAML, w.needNULL, w.outNULL)
	}
	w.results = append(w.results, m)
	return nil
}

func tryJSONToYAML(v []byte) []byte {
	y, err := yaml.JSONToYAML(v)
	if err != nil {
		return v
	}
	return y
}

// CompatibleYAML converts the value to a YAML-compatible value.
func compatibleYAML(v interface{}, jsonToYAML bool, needNULL bool, outNULL string) interface{} {
	var yl interface{}
	switch t := v.(type) {
	case []byte:
		if jsonToYAML {
			t = tryJSONToYAML(t)
		}
		if err := yaml.Unmarshal(t, &yl); err == nil {
			return yl
		}
		if ok := utf8.Valid(t); ok {
			return string(t)
		}
		return `\x` + hex.EncodeToString(t)
	case string:
		var y []byte
		if jsonToYAML {
			y = tryJSONToYAML([]byte(t))
		} else {
			if !strings.Contains(t, "\n") {
				return t
			}
			y = []byte(t)
		}
		if err := yaml.Unmarshal(y, &yl); err == nil {
			return yl
		}
		return v
	default:
		if needNULL {
			return outNULL
		}
		return v
	}
}

// PostWrite is Actual output.
func (w *YAMLWriter) PostWrite() error {
	return w.writer.Encode(w.results)
}
