// correct-platinum-fastq-sequence-identifier.
// Copyright (c) 2018, 2019 imec vzw.

// The fastq files at https://www.ebi.ac.uk/ena/data/view/PRJEB3381
// provide the Illumina sequence identifiers only as comments.
// However, for optical duplicate marking to work correctly
// in elPrep, GATK, and Picard, they need to be the actual
// sequence identifiers in the fastq files before they are
// aligned with bwa mem. This script ensures that this is the
// case.

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/exascience/pargo/pipeline"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func assert(b bool) {
	if !b {
		panic("assertion failed")
	}
}

func correctPlatinumFastqSequenceIdentifierSequential(infastq, outfastq string) {
	fmt.Println("Correcting platinum fastq sequence identifiers sequentially:", infastq, "to", outfastq)

	ingz, err := os.Open(infastq)
	check(err)
	defer func() { check(ingz.Close()) }()

	outgz, err := os.Create(outfastq)
	check(err)
	defer func() { check(outgz.Close()) }()

	input, err := gzip.NewReader(ingz)
	check(err)
	defer func() { check(input.Close()) }()
	output := gzip.NewWriter(outgz)
	defer func() { check(output.Close()) }()

	in := bufio.NewScanner(input)
	out := bufio.NewWriter(output)

	for in.Scan() {
		line := in.Bytes()
		assert(line[0] == '@')
		assert(bytes.HasSuffix(line, []byte("/1")) || bytes.HasSuffix(line, []byte("/2")))
		check(out.WriteByte('@'))
		_, err := out.Write(line[bytes.IndexByte(line, ' ')+1 : len(line)-2])
		check(err)
		check(out.WriteByte('\n'))

		assert(in.Scan())
		_, err = out.Write(in.Bytes())
		check(err)
		check(out.WriteByte('\n'))

		assert(in.Scan())
		assert(in.Bytes()[0] == '+')
		_, err = out.Write([]byte("+\n"))
		check(err)

		assert(in.Scan())
		_, err = out.Write(in.Bytes())
		check(err)
		check(out.WriteByte('\n'))
	}
}

// an entry in a fastq file
type record struct {
	identifier, sequence, qualities string
}

// source, newSource, Close, Err, Fetch, and Data are
// defined for constructing a parallel pargo pipeline.

type source struct {
	gz      *os.File
	reader  *gzip.Reader
	scanner *bufio.Scanner
	data    interface{}
	err     error
}

func newSource(name string) (*source, error) {
	gz, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	reader, err := gzip.NewReader(gz)
	if err != nil {
		_ = gz.Close()
		return nil, err
	}
	scanner := bufio.NewScanner(reader)
	return &source{
		gz:      gz,
		reader:  reader,
		scanner: scanner,
	}, nil
}

func (s *source) Close() error {
	rerr := s.reader.Close()
	gerr := s.gz.Close()
	if rerr != nil {
		return rerr
	}
	if gerr != nil {
		return gerr
	}
	return nil
}

func (s *source) Err() error {
	return s.err
}

func (s *source) Prepare(_ context.Context) int {
	return -1
}

func (s *source) Fetch(n int) (fetched int) {
	var data []record
	s.data = nil
	for fetched = 0; fetched < n; fetched++ {
		if !s.scanner.Scan() {
			s.err = s.scanner.Err()
			if s.err == nil {
				s.data = data
				return
			}
			return 0
		}
		var r record
		r.identifier = s.scanner.Text()
		if r.identifier[0] != '@' {
			s.err = errors.New("malformed identifier line, missing initial @ sign")
			return 0
		}
		if !(strings.HasSuffix(r.identifier, "/1") || strings.HasSuffix(r.identifier, "/2")) {
			s.err = errors.New("malformed identifier line, missing suffix")
			return 0
		}
		if !s.scanner.Scan() {
			s.err = errors.New("missing sequence line")
			return 0
		}
		r.sequence = s.scanner.Text()
		if !s.scanner.Scan() {
			s.err = errors.New("missing intermediate line")
			return 0
		}
		if s.scanner.Text()[0] != '+' {
			s.err = errors.New("malformed intermediate line, missing initial + sign")
			return 0
		}
		if !s.scanner.Scan() {
			s.err = errors.New("missing qualities line")
			return 0
		}
		r.qualities = s.scanner.Text()
		data = append(data, r)
	}
	s.data = data
	return
}

func (s *source) Data() interface{} {
	return s.data
}

func correctPlatinumFastqSequenceIdentifierParallel(infastq, outfastq string) {
	fmt.Println("Correcting platinum fastq sequence identifiers in parallel:", infastq, "to", outfastq)

	src, err := newSource(infastq)
	check(err)
	defer func() { check(src.Close()) }()

	outgz, err := os.Create(outfastq)
	check(err)
	defer func() { check(outgz.Close()) }()

	output := gzip.NewWriter(outgz)
	defer func() { check(output.Close()) }()

	out := bufio.NewWriter(output)

	var p pipeline.Pipeline
	p.Source(src)
	p.Add(
		pipeline.LimitedPar(runtime.GOMAXPROCS(0), pipeline.Receive(func(_ int, data interface{}) interface{} {
			records := data.([]record)
			for i, r := range records {
				records[i].identifier = r.identifier[strings.IndexByte(r.identifier, ' ')+1 : len(r.identifier)-2]
			}
			return records
		})),
		pipeline.StrictOrd(pipeline.Receive(func(_ int, data interface{}) interface{} {
			records := data.([]record)
			for _, r := range records {
				check(out.WriteByte('@'))
				_, err := out.WriteString(r.identifier)
				check(err)
				check(out.WriteByte('\n'))
				_, err = out.WriteString(r.sequence)
				check(err)
				_, err = out.WriteString("\n+\n")
				check(err)
				_, err = out.WriteString(r.qualities)
				check(err)
				check(out.WriteByte('\n'))
			}
			return nil
		})),
	)
	p.Run()
	check(p.Err())
}

func main() {
	switch os.Args[1] {
	case "seq":
		correctPlatinumFastqSequenceIdentifierSequential(os.Args[2], os.Args[3])
	case "par":
		correctPlatinumFastqSequenceIdentifierParallel(os.Args[2], os.Args[3])
	default:
		fmt.Println("correct-platinum-fastq-sequence-identifier [seq|par] in.fastq.gz out.fastq.gz")
	}
}
