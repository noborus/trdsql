package trdsql

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"
)

// ValString converts database value to string.
func ValString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		if ok := utf8.Valid(t); ok {
			return string(t)
		}
		return `\x` + hex.EncodeToString(t)
	case int:
		return strconv.Itoa(t)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case time.Time:
		return t.Format(time.RFC3339)
	default:
		return fmt.Sprint(v)
	}
}

func colValue(value any, replace bool, nullString string) any {
	if !replace {
		return value
	}
	switch t := value.(type) {
	case nil:
		return nil
	case string:
		if t == nullString {
			return nil
		}
	case []byte:
		if string(t) == nullString {
			return nil
		}
	}
	return value
}
