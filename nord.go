package nord

import (
	"encoding/binary"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

type NordConfig struct {
	MaxFileSize uint
}

type Nord struct {
	dirname      string
	keyDir       KeyDir
	activeFileId uint32
	activeFile   *os.File
	writePos     int
	Config       NordConfig
}

func NewNord(dirname string, config NordConfig) Nord {
	nord := Nord{
		Config: config,
	}
	nord.Open(dirname)
	return nord
}

type DataFileEntry struct {
	Timestamp uint32
	Ksz       uint32
	Vsz       uint32
	Key       []byte
	Value     []byte
}

const (
	DataFileEntryHdrLen = 12
	HintFileEntryHdrLen = 16
)

func (df *DataFileEntry) Serialize() (int, []byte) {
	n := DataFileEntryHdrLen + len(df.Key) + len(df.Value)
	buf := make([]byte, n)
	binary.LittleEndian.PutUint32(buf[0:], df.Timestamp)
	binary.LittleEndian.PutUint32(buf[4:], df.Ksz)
	binary.LittleEndian.PutUint32(buf[8:], df.Vsz)
	copy(buf[DataFileEntryHdrLen:], df.Key)
	copy(buf[DataFileEntryHdrLen+len(df.Key):], df.Value)
	return n, buf
}

type HintFileEntry struct {
	Timestamp uint32
	Ksz       uint32
	Vsz       uint32
	ValPos    uint32
	Key       []byte
}

func (hf *HintFileEntry) Serialize() (int, []byte) {
	n := HintFileEntryHdrLen + len(hf.Key)
	buf := make([]byte, n)
	binary.LittleEndian.PutUint32(buf[0:], hf.Timestamp)
	binary.LittleEndian.PutUint32(buf[4:], hf.Ksz)
	binary.LittleEndian.PutUint32(buf[8:], hf.Vsz)
	binary.LittleEndian.PutUint32(buf[12:], hf.ValPos)
	copy(buf[HintFileEntryHdrLen:], hf.Key)
	return n, buf
}

func (n *Nord) Open(dirname string) {
	n.dirname = dirname
	kd, err := buildKeyDir(dirname)
	if err != nil {
		log.Panicf("could not build key dir - %+v", err)
	}
	n.keyDir = kd
	n.activeFileId = uint32(time.Now().Unix())
	filepath := path.Join(dirname, strconv.Itoa(int(n.activeFileId))+".data")
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		log.Panicf("could not create or open active file - %+v", err)
	}
	n.activeFile = f
}

func (n *Nord) Get(key []byte) ([]byte, bool) {
	e, found := n.keyDir[string(key)]
	if !found || e.ValueSz == 0 {
		return []byte{}, false
	}
	filepath := path.Join(n.dirname, strconv.Itoa(int(e.FileId))+".data")
	f, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("error: %+v", err)
		return []byte{}, false
	}
	val := make([]byte, e.ValueSz)
	_, err = f.ReadAt(val, int64(e.ValuePos)+12+int64(len(key)))
	if err != nil {
		log.Printf("error: %+v", err)
		return []byte{}, false
	}
	return val, true
}

func (n *Nord) Put(key, val []byte) error {
	ksz := len(key)
	vsz := len(val)
	dfe := DataFileEntry{
		Timestamp: uint32(time.Now().Unix()),
		Ksz:       uint32(ksz),
		Vsz:       uint32(vsz),
		Key:       key,
		Value:     val,
	}
	nbytes, buf := dfe.Serialize()
	// if adding exceeds the max file size create new active file
	if nbytes+n.writePos >= int(n.Config.MaxFileSize) {
		err := n.updateActiveFile()
		if err != nil {
			return err
		}
	}
	_, err := n.activeFile.Write(buf)
	if err != nil {
		return err
	}
	n.keyDir[string(key)] = KeyDirEntry{
		FileId:    n.activeFileId,
		ValueSz:   uint32(vsz),
		ValuePos:  uint32(n.writePos),
		Timestamp: uint32(time.Now().Unix()),
	}
	n.writePos += nbytes
	return nil
}

func (n *Nord) Delete(key []byte) {
	e, found := n.keyDir[string(key)]
	if !found || e.ValueSz == 0 {
		return
	}
	n.keyDir[string(key)] = KeyDirEntry{
		ValueSz: 0,
	}
}

func (n *Nord) ListKeys() []string {
	keys := make([]string, len(n.keyDir))
	for key := range n.keyDir {
		keys = append(keys, key)
	}
	return keys
}

func (n *Nord) Fold(dirname string) {
	panic("not implemented")
}

func (n *Nord) Merge() error {
	files, err := os.ReadDir(n.dirname)
	if err != nil {
		return err
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

	toMerge := make(map[uint32]bool)
	mergefid := n.activeFileId + 1

	for _, file := range files {
		f := strings.Split(file.Name(), ".")
		fname := f[0]
		ftype := f[1]
		if _, found := fileMap[fname]; found {
			continue
		}
		if ftype == "data" && fname != strconv.Itoa(int(n.activeFileId)) {
			if _, found := n.keyDir[string(fname)]; found {
				continue
			}
			id, _ := strconv.Atoi(fname)
			fid := uint32(id)
			if fid > mergefid {
				mergefid = fid
			}
			toMerge[fid] = true
		}
	}

	if len(toMerge) <= 1 {
		return nil
	}

	newKeyDir := make(map[string]KeyDirEntry, len(n.keyDir))

	outfpath := path.Join(n.dirname, strconv.Itoa(int(mergefid)))
	mergef, err := os.OpenFile(outfpath+".data", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer mergef.Close()
	hintf, err := os.OpenFile(outfpath+".hint", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer hintf.Close()

	writePos := 0

	for key, kdval := range n.keyDir {
		if _, found := toMerge[kdval.FileId]; found {
			fpath := path.Join(n.dirname, strconv.Itoa(int(kdval.FileId))+".data")
			f, err := os.OpenFile(fpath, os.O_RDONLY, 0644)
			if err != nil {
				log.Panicf("could not open data file - %+v", err)
			}
			buf := make([]byte, 12+len(key)+int(kdval.ValueSz))
			_, err = f.ReadAt(buf, int64(kdval.ValuePos))
			if err != nil {
				log.Panicf("could not read value - %+v", err)
			}
			_, err = mergef.Write(buf)
			if err != nil {
				log.Panicf("could not write to merge file - %+v", err)
			}
			newKeyDir[key] = KeyDirEntry{
				FileId:    mergefid,
				ValueSz:   kdval.ValueSz,
				ValuePos:  uint32(writePos),
				Timestamp: kdval.Timestamp,
			}
			he := HintFileEntry{
				Timestamp: kdval.Timestamp,
				Ksz:       uint32(len(key)),
				Vsz:       kdval.ValueSz,
				ValPos:    uint32(writePos),
				Key:       []byte(key),
			}
			_, buf = he.Serialize()
			_, err = hintf.Write(buf)
			if err != nil {
				log.Panicf("could not write to the hint file - %+v", err)
			}
			writePos += 12 + len(key) + int(kdval.ValueSz)
		}
	}
	for file := range toMerge {
		fpath := path.Join(n.dirname, strconv.Itoa(int(file))+".data")
		err := os.Remove(fpath)
		if err != nil {
			log.Printf("could not remove old data file - %v", err)
		}
	}
	return nil
}

func (n *Nord) Sync() {
	panic("not implemented")
}

func (n *Nord) Close() {
	n.activeFile.Close()
}

func (n *Nord) updateActiveFile() error {
	fid := uint32(time.Now().Unix())
	fpath := path.Join(n.dirname, strconv.Itoa(int(fid))+".data")
	err := n.activeFile.Close()
	if err != nil {
		return err
	}
	file, err := os.OpenFile(fpath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	n.activeFile = file
	n.activeFileId = fid
	n.writePos = 0
	return nil
}
