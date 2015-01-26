package main

import (
	"bufio"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type Contact struct {
	id         int
	email_hash uint64
}

func read(filename string, channels []chan Contact, done chan bool) {
	file, _ := os.Open(filename)
	defer file.Close()

	r := bufio.NewReader(file)

	relpacer := strings.NewReplacer(".", "")
	crcTable := crc64.MakeTable(crc64.ECMA)

	for {
		record, err := r.ReadString('\n')
		if err == io.EOF {
			break
		}

		splitted := strings.Split(record, "\t")

		if len(splitted) != 2 || splitted[1] == "" {
			continue
		}

		id, err := strconv.Atoi(splitted[0])
		if err != nil {
			continue
		}

		nodots := relpacer.Replace(splitted[1])
		lower := strings.ToLower(nodots)
		trimmed := strings.TrimSpace(lower)
		checksum64 := crc64.Checksum([]byte(trimmed), crcTable)

		channels[checksum64%uint64(len(channels))] <- Contact{id, checksum64}
	}

	done <- true
}

func read_files(file_names []string, parallelism int) []chan Contact {
	channels := make([]chan Contact, parallelism)

	for i := 0; i != parallelism; i++ {
		channels[i] = make(chan Contact)
	}

	read_done := make(chan bool)

	for _, f := range file_names {
		go read(f, channels, read_done)
	}

	go func() {
		for i := 0; i != len(file_names); i++ {
			<-read_done
		}

		close(read_done)

		for i := 0; i != parallelism; i++ {
			close(channels[i])
		}
	}()

	return channels
}

func Alloc() uint64 {
	var stats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&stats)
	return stats.Alloc
}

func main() {
	PARALLELISM := 8
	runtime.GOMAXPROCS(PARALLELISM)

	channels := read_files(os.Args[1:], PARALLELISM)

	ch_done := make(chan bool)
	ch_read_cnt := make(chan int)

	before := Alloc()
	m := make([]map[uint64][]int, len(channels))

	for i := 0; i != len(channels); i++ {
		m[i] = make(map[uint64][]int, 1000)

		go func(i int) {
			cnt := 0
			for c := range channels[i] {
				cnt += 1
				if cnt%100000 == 0 {
					ch_read_cnt <- cnt
					cnt = 0
				}

				if m[i][c.email_hash] == nil {
					m[i][c.email_hash] = []int{c.id}
				} else {
					m[i][c.email_hash] = append(m[i][c.email_hash], c.id)
				}
			}

			ch_read_cnt <- cnt
			ch_done <- true
		}(i)
	}

	go func() {
		cnt := 0
		for c := range ch_read_cnt {
			cnt += c
			fmt.Printf("Read %.1fM contacts...\n", float32(cnt)/1000000.0)
		}
	}()

	for i := 0; i != len(channels); i++ {
		<-ch_done
	}
	close(ch_done)
	close(ch_read_cnt)

	after := Alloc()

	uniq, all := 0, 0
	for _, h := range m {
		for _, obj := range h {
			all += 1
			if len(obj) == 1 {
				uniq += 1
			}
		}
	}

	fmt.Printf("\nFound %d unique emails, %d only once (%.2f%%)\n", all, uniq, float32(uniq)/float32(all)*100.0)
	fmt.Printf("Memory used: %.2f MB\n", float64(after-before)/(1024*1024))
}
