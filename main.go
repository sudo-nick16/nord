package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

func decodeKv(data []byte) (key, val []byte) {
	// tstamp := binary.LittleEndian.Uint32(data[0:4])
	keyLen := binary.LittleEndian.Uint32(data[4:8])
	// valLen := binary.LittleEndian.Uint32(data[8:12])

	keyOff := 12
	valOff := 12 + int(keyLen)

	key = data[keyOff:valOff]
	val = data[valOff:]

	return key, val
}

func encodeKv(key, val []byte) []byte {
	// timestamp (4 bytes) + key length (4 bytes) + key + value length (4 bytes) + value
	data := make([]byte, 4+4+4+len(key)+len(val))

	tstamp := uint32(time.Now().Unix())

	// header
	binary.LittleEndian.PutUint32(data[0:4], tstamp)
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(key)))
	binary.LittleEndian.PutUint32(data[8:12], uint32(len(val)))

	keyOff := 12
	valOff := 12 + len(key)

	copy(data[keyOff:], key)
	copy(data[valOff:], val)

	return data
}

type ValInfo struct {
	FileId    uint32
	ValueSz   uint32
	ValuePos  int
	Timestamp uint32
}

type KeyDir map[string]ValInfo

func main() {
	keyDir := make(KeyDir)
	writePos := 0

	file, err := os.OpenFile("data.db", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	kvOff := 0

	for {
		data := make([]byte, 12)

		_, err = file.ReadAt(data, int64(kvOff))
		if err != nil {
			break
		}

		tstamp := binary.LittleEndian.Uint32(data[0:4])
		ksz := binary.LittleEndian.Uint32(data[4:8])
		vsz := binary.LittleEndian.Uint32(data[8:12])

		key := make([]byte, ksz)
		_, err = file.ReadAt(key, int64(kvOff)+12)
		if err != nil {
			break
		}

		if vsz == 0 {
			delete(keyDir, string(key))
		} else {
			keyDir[string(key)] = ValInfo{
				FileId:    0,
				ValueSz:   12 + ksz + vsz,
				ValuePos:  kvOff,
				Timestamp: tstamp,
			}
		}

		kvOff += 12 + int(ksz) + int(vsz)
	}

	writePos = kvOff

	reader := bufio.NewReader(os.Stdin)
repl:
	for true {
		fmt.Print(">>> ")
		inp, _, err := reader.ReadLine()
		if err != nil {
			break
		}

		inpList := bytes.SplitN(inp, []byte(" "), 3)

		cmd := inpList[0]

		switch string(cmd) {
		case "set":
			{
				key := inpList[1]
				val := inpList[2]
				data := encodeKv(key, val)
				valInfo := ValInfo{
					FileId:    0,
					ValueSz:   uint32(len(data)),
					ValuePos:  writePos,
					Timestamp: uint32(time.Now().Unix()),
				}
				n, err := file.Write(data)
				if err != nil {
					panic(err)
				}
				if n != len(data) {
					panic("write failed")
				}
				keyDir[string(key)] = valInfo
				writePos += len(data)
			}

		case "get":
			{
				key := inpList[1]
				valInfo, found := keyDir[string(key)]
				if !found {
					fmt.Println("key not found.")
					continue
				}
				data := make([]byte, valInfo.ValueSz)
				_, err := file.ReadAt(data, int64(valInfo.ValuePos))
				if err != nil {
					panic(err)
				}
				_, val := decodeKv(data)
				fmt.Printf("value: %+v\n", string(val))
			}

		case "del":
			{
				key := inpList[1]
				_, found := keyDir[string(key)]
				if !found {
					fmt.Println("key not found.")
					continue
				}
				data := encodeKv(key, []byte{})
				n, err := file.Write(data)
				if err != nil {
					panic(err)
				}
				if n != len(data) {
					panic("write failed")
				}
				delete(keyDir, string(key))
				writePos += len(data)
			}

		case "clear":
			{
				fmt.Println("\x1b[2J\x1b[;H")
			}

		case "exit":
			{
				break repl
			}
		}
	}
}
