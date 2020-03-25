package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

var (
	buffSize = 1 << 20
)

// ReadLineFromEnd --
type ReadLineFromEnd struct {
	f *os.File

	fileSize int
	bwr      *bytes.Buffer
	swapBuff []byte
}

// NewReadLineFromEnd --
func NewReadLineFromEnd(name string) (rd *ReadLineFromEnd, err error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if info.IsDir() {
		return nil, fmt.Errorf("not file")
	}
	fileSize := int(info.Size())
	rd = &ReadLineFromEnd{
		f:        f,
		fileSize: fileSize,
		bwr:      bytes.NewBuffer([]byte{}),
		swapBuff: make([]byte, buffSize),
	}
	return rd, nil
}

// Read --
func (c *ReadLineFromEnd) Read(p []byte) (n int, err error) {
	err = c.buff()
	if err != nil {
		return n, err
	}
	return c.bwr.Read(p)
}

// ReadLine 结尾包含'\n'
func (c *ReadLineFromEnd) ReadLine() (line []byte, err error) {
	err = c.buff()
	if err != nil {
		return nil, err
	}
	return c.bwr.ReadBytes('\n')
}

// Close --
func (c *ReadLineFromEnd) Close() (err error) {
	return c.f.Close()
}

func (c *ReadLineFromEnd) buff() (err error) {
	if c.fileSize == 0 {
		return nil
	}

	if c.bwr.Len() >= buffSize {
		return nil
	}

	offset := 0
	if c.fileSize > buffSize {
		offset = c.fileSize - buffSize
	}
	_, err = c.f.Seek(int64(offset), 0)
	if err != nil {
		return err
	}

	n, err := c.f.Read(c.swapBuff)
	if err != nil && err != io.EOF {
		return err
	}
	if n == 0 {
		return nil
	}
	for {
		m := bytes.LastIndex(c.swapBuff[:n], []byte{'\n'})
		if m == -1 {
			break
		}
		if m < n-1 {
			_, err = c.bwr.Write(c.swapBuff[m+1 : n])
			if err != nil {
				return err
			}
			_, err = c.bwr.Write([]byte{'\n'})
			if err != nil {
				return err
			}
		}
		n = m
		if n == 0 {
			break
		}
	}
	if n > 0 {
		_, err := c.bwr.Write(c.swapBuff[:n])
		if err != nil {
			return err
		}
	}
	c.fileSize = offset
	return nil
}

//func main() {
//	rd, err := NewReadLineFromEnd("./10.16.4.125.3307")
//	if err != nil {
//
//	}
//	defer rd.Close()
//	for {
//		data, err := rd.ReadLine()
//		if err != nil {
//			if err != io.EOF {
//
//			}
//			break
//		}
//		fmt.Println("-------")
//		fmt.Print(string(data))
//		fmt.Println("-------")
//
//	}
//}
