package trdsql

import (
	"bufio"
	"fmt"
	"strings"

	runewidth "github.com/mattn/go-runewidth"
	"golang.org/x/crypto/ssh/terminal"
)

// VFWrite is Vertical Format output
type VFWrite struct {
	writer    *bufio.Writer
	termWidth int
	hsize     int
	header    []string
	count     int
}

func (trdsql *TRDSQL) NewVFWrite() *VFWrite {
	var err error
	vf := &VFWrite{}
	vf.writer = bufio.NewWriter(trdsql.OutStream)
	vf.termWidth, _, err = terminal.GetSize(0)
	if err != nil {
		vf.termWidth = 40
	}
	return vf
}

// First is preparation
func (vf *VFWrite) First(columns []string, types []string) error {
	vf.header = make([]string, len(columns))
	vf.hsize = 0
	for i, col := range columns {
		if vf.hsize < runewidth.StringWidth(col) {
			vf.hsize = runewidth.StringWidth(col)
		}
		vf.header[i] = col
	}
	return nil
}

// WriteRow is Actual output
func (vf *VFWrite) WriteRow(values []interface{}, columns []string) error {
	vf.count++
	_, err := fmt.Fprintf(vf.writer,
		"---[ %d]%s\n", vf.count, strings.Repeat("-", (vf.termWidth-16)))
	if err != nil {
		debug.Printf("%s\n", err)
	}
	for i, col := range vf.header {
		v := vf.hsize - runewidth.StringWidth(col)
		_, err := fmt.Fprintf(vf.writer,
			"%s%s | %-s\n",
			strings.Repeat(" ", v+2),
			col,
			ValString(values[i]))
		if err != nil {
			debug.Printf("%s\n", err)
		}
	}
	return nil
}

// Last is flush
func (vf *VFWrite) Last() error {
	return vf.writer.Flush()
}
