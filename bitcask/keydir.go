package bitcask

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

func getFileInfo(fname string) (uint32, string) {
	f := strings.Split(fname, ".")
	id, _ := strconv.Atoi(f[0])
	return uint32(id), f[1]
}

func buildKeyDir(dirname string) (KeyDir, error) {
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
		iid, _ := getFileInfo(files[i].Name())
		jid, _ := getFileInfo(files[j].Name())
		return iid < jid
	})

	hintMap := make(map[uint32]bool, 0)

	for _, file := range files {
		fid, ftype := getFileInfo(file.Name())
		if ftype == "hint" {
			hintMap[uint32(fid)] = true
		}
	}

	for _, file := range files {
		fid, ftype := getFileInfo(file.Name())
		if ftype == "hint" {
			hintMap[uint32(fid)] = true
		}
		if ftype == "hint" {
			continue
		}
		if _, hasAdjHint := hintMap[fid]; hasAdjHint {
			// process hint file
			filepath := path.Join(dirname, strconv.Itoa(int(fid))+".hint")
			readPos := 0
			hf, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
			if err != nil {
				panic(err)
			}
			for {
				buf := make([]byte, HintFileEntryHdrLen)
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
				_, err := hf.ReadAt(kdkey, int64(readPos)+HintFileEntryHdrLen)
				if err != nil {
					return nil, err
				}
				keyDir[string(kdkey)] = kdval
				readPos += HintFileEntryHdrLen + int(ksz)
			}
		} else {
			// process data file
			filepath := path.Join(dirname, file.Name())
			readPos := 0
			hf, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
			if err != nil {
				panic(err)
			}
			for {
				buf := make([]byte, DataFileEntryHdrLen)
				_, err = hf.ReadAt(buf, int64(readPos))
				if err != nil {
					break
				}
				ksz := binary.LittleEndian.Uint32(buf[4:8])
				kdval := KeyDirEntry{
					FileId:    uint32(fid),
					Timestamp: binary.LittleEndian.Uint32(buf[0:4]),
					ValueSz:   binary.LittleEndian.Uint32(buf[8:12]),
					ValuePos:  uint32(readPos),
				}
				kdkey := make([]byte, ksz)
				_, err := hf.ReadAt(kdkey, int64(readPos)+DataFileEntryHdrLen)
				if err != nil {
					return nil, err
				}
				keyDir[string(kdkey)] = kdval
				readPos += DataFileEntryHdrLen + int(ksz+kdval.ValueSz)
			}
		}
	}
	return keyDir, nil
}
