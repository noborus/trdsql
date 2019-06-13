package trdsql

// Writer is file format writer.
// Writer is a group of methods called from Export.
type Writer interface {
	PreWrite([]string, []string) error
	WriteRow([]interface{}, []string) error
	PostWrite() error
}

// NewWriter returns a Writer interface.
func NewWriter(writeOpts WriteOpts) Writer {
	switch writeOpts.OutFormat {
	case LTSV:
		return NewLTSVWriter(writeOpts)
	case JSON:
		return NewJSONWriter(writeOpts)
	case RAW:
		return NewRAWWriter(writeOpts)
	case MD:
		return NewTWWriter(writeOpts, true)
	case AT:
		return NewTWWriter(writeOpts, false)
	case VF:
		return NewVFWriter(writeOpts)
	case TBLN:
		return NewTBLNWriter(writeOpts)
	case CSV:
		return NewCSVWriter(writeOpts)
	default:
		return NewCSVWriter(writeOpts)
	}
}
