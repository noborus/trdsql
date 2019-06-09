package trdsql

// Writer is file format writer
type Writer interface {
	PreWrite([]string, []string) error
	WriteRow([]interface{}, []string) error
	PostWrite() error
}

func NewWriter(writeOpts WriteOpts) Writer {
	switch writeOpts.OutFormat {
	case LTSV:
		return NewLTSVWrite(writeOpts)
	case JSON:
		return NewJSONWrite(writeOpts)
	case RAW:
		return NewRAWWrite(writeOpts)
	case MD:
		return NewTWWrite(writeOpts, true)
	case AT:
		return NewTWWrite(writeOpts, false)
	case VF:
		return NewVFWrite(writeOpts)
	case TBLN:
		return NewTBLNWrite(writeOpts)
	case CSV:
		return NewCSVWrite(writeOpts)
	default:
		return NewCSVWrite(writeOpts)
	}
}
