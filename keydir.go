package nord

import (
	"encoding/binary"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

type KeyDirEntry struct {
	FileId    uint32
	ValueSz   uint32
	ValuePos  uint32
	Timestamp uint32
}

type KeyDir map[string]KeyDirEntry

func BuildKeyDir(dirname string) (KeyDir, error) {
	keyDir := make(KeyDir, 0)

	files, err := os.ReadDir(dirname)
	if os.IsNotExist(err) {
		err = os.Mkdir(dirname, os.ModePerm)
		if err != nil {
			return nil, err
		}
		return keyDir, nil
	}
	if err != nil {
		return nil, err
	}

	sort.SliceStable(files, func(i, j int) bool {
		t1, _ := strconv.Atoi(strings.Split(files[i].Name(), ".")[0])
		t2, _ := strconv.Atoi(strings.Split(files[j].Name(), ".")[0])
		return t1 < t2
	})

	fileMap := make(map[string]bool, 0)

	for _, file := range files {
		f := strings.Split(file.Name(), ".")
		fname := f[0]
		ftype := f[1]
		if ftype == "hint" {
			fileMap[fname] = true
		}
	}

	for _, file := range files {
		f := strings.Split(file.Name(), ".")
		if len(f) < 2 {
			continue
		}
		fname := f[0]
		ftype := f[1]

		if ftype == "hint" {
			continue
		}

		fid, err := strconv.ParseUint(fname, 10, 32)
		if err != nil {
			return nil, err
		}

		if _, hasAdjHint := fileMap[fname]; hasAdjHint {
			filepath := path.Join(dirname, fname+".hint")
			readPos := 0
			hf, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
			if err != nil {
				panic(err)
			}
			// 4 + 4 + 4 + 4
			for {
				buf := make([]byte, 16)
				_, err = hf.ReadAt(buf, int64(readPos))
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, err
				}
				kdval := KeyDirEntry{
					FileId:    uint32(fid),
					Timestamp: binary.LittleEndian.Uint32(buf[0:4]),
					ValueSz:   binary.LittleEndian.Uint32(buf[8:12]),
					ValuePos:  binary.LittleEndian.Uint32(buf[12:16]),
				}
				ksz := binary.LittleEndian.Uint32(buf[4:8])
				kdkey := make([]byte, ksz)
				_, err := hf.ReadAt(kdkey, int64(readPos)+16)
				if err != nil {
					return nil, err
				}
				keyDir[string(kdkey)] = kdval
				readPos += 16 + int(ksz)
			}
		} else {
			// data file
			filepath := path.Join(dirname, file.Name())
			readPos := 0
			hf, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
			if err != nil {
				panic(err)
			}
			// 4 + 4 + 4
			for {
				buf := make([]byte, 12)
				_, err = hf.ReadAt(buf, int64(readPos))
				if err != nil {
					break
				}
				ksz := binary.LittleEndian.Uint32(buf[4:8])
				kdval := KeyDirEntry{
					FileId:    uint32(fid),
					Timestamp: binary.LittleEndian.Uint32(buf[0:4]),
					ValueSz:   binary.LittleEndian.Uint32(buf[8:12]),
					ValuePos:  uint32(readPos), // offset to the value
				}
				kdkey := make([]byte, ksz)
				_, err := hf.ReadAt(kdkey, int64(readPos)+12)
				if err != nil {
					return nil, err
				}
				keyDir[string(kdkey)] = kdval
				readPos += 12 + int(ksz+kdval.ValueSz)
			}
		}
	}
	return keyDir, nil
}
