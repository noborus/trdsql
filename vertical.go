package main

import (
	"bufio"
	"fmt"
	"strings"

	runewidth "github.com/mattn/go-runewidth"
	"golang.org/x/crypto/ssh/terminal"
)

// VfOut is Vertical Format output
type VfOut struct {
	writer    *bufio.Writer
	termWidth int
	hsize     int
	header    []string
	count     int
}

func (trdsql *TRDSQL) vfOutNew() Output {
	var err error
	vf := &VfOut{}
	vf.writer = bufio.NewWriter(trdsql.outStream)
	vf.termWidth, _, err = terminal.GetSize(0)
	if err != nil {
		vf.termWidth = 40
	}
	return vf
}

// First is preparation
func (vf *VfOut) First(columns []string) error {
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

// RowWrite is Actual output
func (vf *VfOut) RowWrite(values []interface{}, columns []string) error {
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
			valString(values[i]))
		if err != nil {
			debug.Printf("%s\n", err)
		}
	}
	return nil
}

// Last is flush
func (vf *VfOut) Last() error {
	return vf.writer.Flush()
}
