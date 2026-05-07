// docker-save-to-simg converts a `docker save` archive into an Apptainer/Singularity SIF image.
//
// Build:
//
//	CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o docker-save-to-simg builder/docker-save-to-simg.go
//
// Usage:
//
//	docker save alpine:3.20 -o alpine.tar
//	./docker-save-to-simg alpine.tar alpine.simg
//	docker save alpine:3.20 | ./docker-save-to-simg - alpine.simg
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

func main() {
	arch := flag.String("arch", "", "target architecture metadata (default: config or host)")
	imageIndex := flag.Int("image", 0, "image index from docker save manifest.json")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s [flags] <docker-save.tar|-> <out.simg>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(2)
	}
	if err := convertDockerSave(flag.Arg(0), flag.Arg(1), *arch, *imageIndex); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

type dockerManifestEntry struct {
	Config   string   `json:"Config"`
	RepoTags []string `json:"RepoTags"`
	Layers   []string `json:"Layers"`
}

func convertDockerSave(inPath, outPath, arch string, imageIndex int) error {
	tmp, err := os.MkdirTemp("", "docker-save-to-simg-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	manifest, configPath, layerPaths, err := extractDockerSave(inPath, tmp, imageIndex)
	if err != nil {
		return err
	}

	configData, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	cfg, err := ParseOCIImageConfig(configData)
	if err != nil {
		return err
	}
	cfg.Bootstrap = "docker-archive"
	if len(manifest.RepoTags) > 0 {
		cfg.ImageRef = manifest.RepoTags[0]
	}

	if strings.TrimSpace(arch) == "" {
		var meta struct {
			Architecture string `json:"architecture"`
		}
		_ = json.Unmarshal(configData, &meta)
		arch = meta.Architecture
	}

	layers := make([]LayerSource, 0, len(layerPaths))
	for _, p := range layerPaths {
		layerPath := p
		layers = append(layers, LayerSource{
			Name:      layerPath,
			MediaType: "application/vnd.docker.image.rootfs.diff.tar",
			Open:      func() (io.ReadCloser, error) { return os.Open(layerPath) },
		})
	}
	return WriteFromLayerSourcesWithConfig(layers, outPath, arch, cfg)
}

func extractDockerSave(inPath, tmp string, imageIndex int) (dockerManifestEntry, string, []string, error) {
	var r io.Reader
	var in *os.File
	if inPath == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(inPath)
		if err != nil {
			return dockerManifestEntry{}, "", nil, err
		}
		defer f.Close()
		in = f
		r = in
	}
	tr := tar.NewReader(r)
	entries := map[string]string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return dockerManifestEntry{}, "", nil, fmt.Errorf("read docker archive: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA {
			continue
		}
		rel, err := cleanArchiveName(hdr.Name)
		if err != nil {
			return dockerManifestEntry{}, "", nil, err
		}
		out := filepath.Join(tmp, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return dockerManifestEntry{}, "", nil, err
		}
		f, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return dockerManifestEntry{}, "", nil, err
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return dockerManifestEntry{}, "", nil, err
		}
		if err := f.Close(); err != nil {
			return dockerManifestEntry{}, "", nil, err
		}
		entries[rel] = out
	}
	manifestPath := entries["manifest.json"]
	if manifestPath == "" {
		return dockerManifestEntry{}, "", nil, errors.New("docker archive missing manifest.json")
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return dockerManifestEntry{}, "", nil, err
	}
	var manifests []dockerManifestEntry
	if err := json.Unmarshal(data, &manifests); err != nil {
		return dockerManifestEntry{}, "", nil, fmt.Errorf("decode manifest.json: %w", err)
	}
	if imageIndex < 0 || imageIndex >= len(manifests) {
		return dockerManifestEntry{}, "", nil, fmt.Errorf("image index %d out of range", imageIndex)
	}
	m := manifests[imageIndex]
	configRel, err := cleanArchiveName(m.Config)
	if err != nil {
		return dockerManifestEntry{}, "", nil, err
	}
	configPath := entries[configRel]
	if configPath == "" {
		return dockerManifestEntry{}, "", nil, fmt.Errorf("docker archive missing config %s", m.Config)
	}
	layerPaths := make([]string, 0, len(m.Layers))
	for _, layer := range m.Layers {
		rel, err := cleanArchiveName(layer)
		if err != nil {
			return dockerManifestEntry{}, "", nil, err
		}
		p := entries[rel]
		if p == "" {
			return dockerManifestEntry{}, "", nil, fmt.Errorf("docker archive missing layer %s", layer)
		}
		layerPaths = append(layerPaths, p)
	}
	if len(layerPaths) == 0 {
		return dockerManifestEntry{}, "", nil, errors.New("docker archive image has no layers")
	}
	return m, configPath, layerPaths, nil
}

func cleanArchiveName(name string) (string, error) {
	name = strings.TrimPrefix(path.Clean("/"+name), "/")
	if name == "" || name == "." || strings.HasPrefix(name, "../") || strings.Contains(name, "/../") {
		return "", fmt.Errorf("unsafe archive path %q", name)
	}
	return name, nil
}

const (
	sifHeaderSize     = 4096
	sifDescriptorSize = 585

	sifDataPartition = int32(0x4004)
	sifGroupMask     = uint32(0xf0000000)

	squashMagic = "hsqs"

	squashCompressionZlib = 1
	squashVersionMajor    = 4
	squashVersionMinor    = 0

	squashMetaBlockSize = 8192
	squashBlockSize     = 1 << 20 // 1 MiB

	squashMetaUncompressed = 0x8000
	squashDataUncompressed = 0x01000000
	squashDataSizeMask     = 0x00ffffff

	squashInodeBasicDir  = 1
	squashInodeBasicFile = 2
	squashInodeBasicSym  = 3
	squashInodeLongDir   = 8
	squashInodeLongFile  = 9

	squashNoFragments = 0

	squashNoXattrTable  = ^uint64(0)
	squashNoLookupTable = ^uint64(0)
	squashNoFragTable   = ^uint64(0)

	squashNoFragment = uint32(0xffffffff)
	squashNoXattr    = uint32(0xffffffff)
)

var errUnsupportedFileType = errors.New("unsupported file type")

type nodeKind uint8

const (
	nodeDirectory nodeKind = iota + 1
	nodeRegular
	nodeSymlink
)

type node struct {
	name    string
	absPath string
	kind    nodeKind
	mode    fs.FileMode
	mtime   uint32
	uid     uint32
	gid     uint32
	size    uint64
	link    string

	parent   *node
	children []*node

	inodeType uint16
	inodeSize int
	inodeNum  uint32
	inodeRef  uint64
	uidIndex  uint16
	gidIndex  uint16

	fileStartRel uint64
	fileBlocks   []uint32

	// For OCI-layer sourced regular files.
	sourceKey    string
	sourceLayer  int
	sourceSeq    int
	sourceOrigin bool

	dirLen       int
	dirStartRel  uint64
	dirStartBlk  uint32
	dirStartOff  uint16
	dirChildBase uint32
}

type sifHeader struct {
	LaunchScript      [32]byte
	Magic             [10]byte
	Version           [3]byte
	Arch              [3]byte
	UUID              [16]byte
	CreatedAt         int64
	ModifiedAt        int64
	DescriptorsFree   int64
	DescriptorsTotal  int64
	DescriptorsOffset int64
	DescriptorsSize   int64
	DataOffset        int64
	DataSize          int64
}

type sifDescriptor struct {
	DataType        int32
	Used            bool
	ID              uint32
	GroupID         uint32
	LinkedID        uint32
	Offset          int64
	Size            int64
	SizeWithPadding int64

	CreatedAt  int64
	ModifiedAt int64
	UID        int64
	GID        int64
	Name       [128]byte
	Extra      [384]byte
}

type sifPartitionExtra struct {
	FSType   int32
	PartType int32
	Arch     [3]byte
}

type squashSuperblock struct {
	Magic             [4]byte
	Inodes            uint32
	MkfsTime          uint32
	BlockSize         uint32
	Fragments         uint32
	Compression       uint16
	BlockLog          uint16
	Flags             uint16
	NoIDs             uint16
	Major             uint16
	Minor             uint16
	RootInode         uint64
	BytesUsed         uint64
	IDTableStart      uint64
	XattrIDTableStart uint64
	InodeTableStart   uint64
	DirectoryTable    uint64
	FragmentTable     uint64
	LookupTable       uint64
}

type writeState struct {
	f      *os.File
	base   int64
	relPos uint64
}

func WriteFromDir(srcDir, outPath, arch string) error {
	if strings.TrimSpace(srcDir) == "" {
		return errors.New("source directory is required")
	}
	if strings.TrimSpace(outPath) == "" {
		return errors.New("output path is required")
	}

	absSrc, err := filepath.Abs(srcDir)
	if err != nil {
		return fmt.Errorf("resolve source directory: %w", err)
	}

	st, err := os.Stat(absSrc)
	if err != nil {
		return fmt.Errorf("stat source directory: %w", err)
	}
	if !st.IsDir() {
		return fmt.Errorf("source path is not a directory: %s", absSrc)
	}

	normArch := normalizeArch(arch)

	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create output file %s: %w", outPath, err)
	}
	defer f.Close()

	dataOffset := int64(sifHeaderSize + sifDescriptorSize)
	squashOffset := alignUp(dataOffset, 4096)
	if _, err := f.Write(make([]byte, squashOffset)); err != nil {
		return fmt.Errorf("initialize output file: %w", err)
	}

	root, allNodes, err := buildTree(absSrc)
	if err != nil {
		return err
	}

	ws := &writeState{f: f, base: squashOffset, relPos: 0}
	if err := ws.seekRelative(0); err != nil {
		return err
	}

	squashSize, err := writeSquashFS(ws, root, allNodes, writeFileData)
	if err != nil {
		return err
	}

	now := time.Now().Unix()
	hdr, desc, err := newSIFHeaderAndDescriptor(normArch, now, squashOffset, squashSize)
	if err != nil {
		return err
	}

	if err := writeSIFHeaderAndDescriptor(f, hdr, desc); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync output file: %w", err)
	}

	return nil
}

func buildTree(rootDir string) (*node, []*node, error) {
	rootInfo, err := os.Lstat(rootDir)
	if err != nil {
		return nil, nil, fmt.Errorf("lstat %s: %w", rootDir, err)
	}

	root := &node{
		name:    "",
		absPath: rootDir,
		kind:    nodeDirectory,
		mode:    rootInfo.Mode(),
		mtime:   toUnix32(rootInfo.ModTime()),
	}
	root.uid, root.gid = fileInfoIDs(rootInfo)

	all := []*node{root}
	lookup := map[string]*node{rootDir: root}

	err = filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == rootDir {
			return nil
		}

		parentPath := filepath.Dir(path)
		parent := lookup[parentPath]
		if parent == nil {
			return fmt.Errorf("internal tree error: missing parent for %s", path)
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", path, err)
		}

		n := &node{
			name:    filepath.Base(path),
			absPath: path,
			mode:    info.Mode(),
			mtime:   toUnix32(info.ModTime()),
			parent:  parent,
		}
		n.uid, n.gid = fileInfoIDs(info)

		switch {
		case d.IsDir():
			n.kind = nodeDirectory
			lookup[path] = n
		case info.Mode().Type() == 0:
			n.kind = nodeRegular
			n.size = uint64(info.Size())
		case info.Mode()&os.ModeSymlink != 0:
			n.kind = nodeSymlink
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("read symlink %s: %w", path, err)
			}
			n.link = target
		default:
			return fmt.Errorf("%w: %s (%s)", errUnsupportedFileType, path, info.Mode().Type().String())
		}

		parent.children = append(parent.children, n)
		all = append(all, n)
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("walk source directory: %w", err)
	}

	for _, n := range all {
		if len(n.children) == 0 {
			continue
		}
		sort.Slice(n.children, func(i, j int) bool { return n.children[i].name < n.children[j].name })
	}

	return root, all, nil
}

func writeSquashFS(ws *writeState, root *node, all []*node, writeFiles func(*writeState, []*node) error) (int64, error) {
	if err := reserveSquashSuperblock(ws); err != nil {
		return 0, err
	}

	inodes := inodeOrder(root)
	for i, n := range inodes {
		n.inodeNum = uint32(i + 1)
	}

	if err := writeFiles(ws, inodes); err != nil {
		return 0, err
	}

	return finalizeSquashFS(ws, root, all, inodes)
}

func writeSquashFSPrepared(ws *writeState, root *node, all []*node) (int64, error) {
	inodes := inodeOrder(root)
	for i, n := range inodes {
		n.inodeNum = uint32(i + 1)
	}

	return finalizeSquashFS(ws, root, all, inodes)
}

func reserveSquashSuperblock(ws *writeState) error {
	if err := ws.write(make([]byte, binary.Size(squashSuperblock{}))); err != nil {
		return fmt.Errorf("reserve squashfs superblock: %w", err)
	}
	return nil
}

func finalizeSquashFS(ws *writeState, root *node, all, inodes []*node) (int64, error) {

	assignInodeTypesAndSizes(inodes)
	ids := assignIDIndexes(inodes)
	assignInodeRefs(inodes)

	dirs := directoryOrder(inodes)
	assignDirectoryLayout(dirs)
	for adjustLargeDirectoryTypes(dirs) {
		assignInodeRefs(inodes)
		assignDirectoryLayout(dirs)
	}

	inodeTableStart := ws.relPos
	if err := writeInodeTable(ws, inodes); err != nil {
		return 0, err
	}

	dirTableStart := ws.relPos
	if err := writeDirectoryTable(ws, dirs); err != nil {
		return 0, err
	}

	idTableStart, err := writeIDTable(ws, ids)
	if err != nil {
		return 0, err
	}

	bytesUsed := ws.relPos
	sb := squashSuperblock{
		Inodes:            uint32(len(all)),
		MkfsTime:          uint32(time.Now().Unix()),
		BlockSize:         squashBlockSize,
		Fragments:         squashNoFragments,
		Compression:       squashCompressionZlib,
		BlockLog:          blockLog2(squashBlockSize),
		Flags:             0,
		NoIDs:             uint16(len(ids)),
		Major:             squashVersionMajor,
		Minor:             squashVersionMinor,
		RootInode:         root.inodeRef,
		BytesUsed:         bytesUsed,
		IDTableStart:      idTableStart,
		XattrIDTableStart: squashNoXattrTable,
		InodeTableStart:   inodeTableStart,
		DirectoryTable:    dirTableStart,
		FragmentTable:     squashNoFragTable,
		LookupTable:       squashNoLookupTable,
	}
	copy(sb.Magic[:], []byte(squashMagic))

	if err := ws.writeAt(0, sb); err != nil {
		return 0, fmt.Errorf("write squashfs superblock: %w", err)
	}

	return int64(bytesUsed), nil
}

func writeFileData(ws *writeState, inodes []*node) error {
	buf := make([]byte, squashBlockSize)
	compressor, err := newDataBlockCompressor()
	if err != nil {
		return err
	}

	for _, n := range inodes {
		if n.kind != nodeRegular {
			continue
		}

		n.fileStartRel = ws.relPos
		n.fileBlocks = n.fileBlocks[:0]

		if n.size == 0 {
			continue
		}

		f, err := os.Open(n.absPath)
		if err != nil {
			return fmt.Errorf("open %s: %w", n.absPath, err)
		}

		for {
			nr, readErr := io.ReadFull(f, buf)
			if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
				f.Close()
				return fmt.Errorf("read %s: %w", n.absPath, readErr)
			}
			if nr > 0 {
				enc, err := writeDataBlock(ws, compressor, buf[:nr])
				if err != nil {
					f.Close()
					return fmt.Errorf("write file data for %s: %w", n.absPath, err)
				}
				n.fileBlocks = append(n.fileBlocks, enc)
			}
			if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
				break
			}
		}

		if err := f.Close(); err != nil {
			return fmt.Errorf("close %s: %w", n.absPath, err)
		}
	}

	return nil
}

type dataBlockCompressor struct {
	buf bytes.Buffer
	zw  *zlib.Writer
}

func newDataBlockCompressor() (*dataBlockCompressor, error) {
	var buf bytes.Buffer
	zw, err := zlib.NewWriterLevel(&buf, zlib.DefaultCompression)
	if err != nil {
		return nil, fmt.Errorf("create zlib writer: %w", err)
	}
	return &dataBlockCompressor{zw: zw}, nil
}

func writeDataBlock(ws *writeState, c *dataBlockCompressor, data []byte) (uint32, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if len(data) > squashDataSizeMask {
		return 0, fmt.Errorf("data block too large: %d bytes", len(data))
	}

	c.buf.Reset()
	c.zw.Reset(&c.buf)
	if _, err := c.zw.Write(data); err != nil {
		return 0, fmt.Errorf("compress data block: %w", err)
	}
	if err := c.zw.Close(); err != nil {
		return 0, fmt.Errorf("finish compressed data block: %w", err)
	}

	if c.buf.Len() >= len(data) {
		if err := ws.write(data); err != nil {
			return 0, err
		}
		return uint32(len(data)) | squashDataUncompressed, nil
	}

	if err := ws.write(c.buf.Bytes()); err != nil {
		return 0, err
	}
	return uint32(c.buf.Len()), nil
}

func inodeOrder(root *node) []*node {
	var out []*node
	var walk func(n *node)
	walk = func(n *node) {
		out = append(out, n)
		for _, c := range n.children {
			walk(c)
		}
	}
	walk(root)
	return out
}

func directoryOrder(inodes []*node) []*node {
	dirs := make([]*node, 0)
	for _, n := range inodes {
		if n.kind == nodeDirectory {
			dirs = append(dirs, n)
		}
	}
	return dirs
}

func assignInodeTypesAndSizes(inodes []*node) {
	for _, n := range inodes {
		switch n.kind {
		case nodeDirectory:
			n.inodeType = squashInodeBasicDir
			n.inodeSize = 32
		case nodeRegular:
			if n.size > 0xffffffff || n.fileStartRel > 0xffffffff {
				n.inodeType = squashInodeLongFile
				n.inodeSize = 56 + len(n.fileBlocks)*4
			} else {
				n.inodeType = squashInodeBasicFile
				n.inodeSize = 32 + len(n.fileBlocks)*4
			}
		case nodeSymlink:
			n.inodeType = squashInodeBasicSym
			n.inodeSize = 24 + len(n.link)
		}
	}
}

func adjustLargeDirectoryTypes(dirs []*node) bool {
	changed := false
	for _, n := range dirs {
		if n.dirLen+3 <= 0xffff || n.inodeType == squashInodeLongDir {
			continue
		}
		n.inodeType = squashInodeLongDir
		n.inodeSize = 40
		changed = true
	}
	return changed
}

func assignInodeRefs(inodes []*node) {
	var blockRel uint32
	var off uint16

	for _, n := range inodes {
		n.inodeRef = (uint64(blockRel) << 16) | uint64(off)
		advance := n.inodeSize

		for advance > 0 {
			left := squashMetaBlockSize - int(off)
			if advance < left {
				off += uint16(advance)
				advance = 0
				continue
			}
			advance -= left
			blockRel += uint32(2 + squashMetaBlockSize)
			off = 0
		}
	}
}

func assignDirectoryLayout(dirs []*node) {
	var logical uint64
	for _, d := range dirs {
		d.dirStartRel = logical
		d.dirLen, d.dirChildBase = estimateDirBytes(d)
		logical += uint64(d.dirLen)

		dirBlock := d.dirStartRel / squashMetaBlockSize
		d.dirStartBlk = uint32(dirBlock * (squashMetaBlockSize + 2))
		d.dirStartOff = uint16(d.dirStartRel % squashMetaBlockSize)
	}
}

func estimateDirBytes(d *node) (int, uint32) {
	if len(d.children) == 0 {
		return 0, 0
	}

	total := 0
	var currentStart uint32
	count := 0
	base := uint32(0)
	for i, c := range d.children {
		childStart := uint32(c.inodeRef >> 16)
		if i == 0 {
			currentStart = childStart
			base = c.inodeNum
			total += 12
			count = 0
		}
		if childStart != currentStart || count == 256 {
			currentStart = childStart
			total += 12
			count = 0
		}
		total += 8 + len(c.name)
		count++
	}
	return total, base
}

func writeInodeTable(ws *writeState, inodes []*node) error {
	mw := newMetaWriter(ws)
	for _, n := range inodes {
		rec, err := encodeInode(n)
		if err != nil {
			return err
		}
		if err := mw.write(rec); err != nil {
			return err
		}
	}
	return mw.close()
}

func writeDirectoryTable(ws *writeState, dirs []*node) error {
	mw := newMetaWriter(ws)
	for _, d := range dirs {
		if err := writeDirRecords(mw, d); err != nil {
			return err
		}
	}
	return mw.close()
}

func assignIDIndexes(inodes []*node) []uint32 {
	indexByID := make(map[uint32]uint16)
	ids := make([]uint32, 0, 1)
	add := func(id uint32) uint16 {
		if idx, ok := indexByID[id]; ok {
			return idx
		}
		idx := uint16(len(ids))
		indexByID[id] = idx
		ids = append(ids, id)
		return idx
	}

	for _, n := range inodes {
		n.uidIndex = add(n.uid)
		n.gidIndex = add(n.gid)
	}
	return ids
}

func writeIDTable(ws *writeState, ids []uint32) (uint64, error) {
	if len(ids) == 0 {
		ids = []uint32{0}
	}
	// SquashFS stores ID metadata blocks first, then an index of pointers.
	// The superblock's IDTableStart references the pointer table.
	metaPos := ws.relPos
	var hdr [2]byte
	payloadLen := len(ids) * 4
	if payloadLen > squashMetaBlockSize {
		return 0, fmt.Errorf("too many squashfs IDs: %d", len(ids))
	}
	binary.LittleEndian.PutUint16(hdr[:], squashMetaUncompressed|uint16(payloadLen))
	if err := ws.write(hdr[:]); err != nil {
		return 0, fmt.Errorf("write id table metadata header: %w", err)
	}

	payload := make([]byte, payloadLen)
	for i, id := range ids {
		binary.LittleEndian.PutUint32(payload[i*4:i*4+4], id)
	}
	if err := ws.write(payload); err != nil {
		return 0, fmt.Errorf("write id table metadata payload: %w", err)
	}

	pointerPos := ws.relPos
	var ptr [8]byte
	binary.LittleEndian.PutUint64(ptr[:], metaPos)
	if err := ws.write(ptr[:]); err != nil {
		return 0, fmt.Errorf("write id table pointer: %w", err)
	}

	return pointerPos, nil
}

func encodeInode(n *node) ([]byte, error) {
	mode, err := inodeMode(n)
	if err != nil {
		return nil, err
	}

	switch n.kind {
	case nodeDirectory:
		if n.inodeType == squashInodeLongDir {
			b := make([]byte, 40)
			putBaseInode(b, n, mode)
			binary.LittleEndian.PutUint32(b[16:20], uint32(len(n.children)+2))
			binary.LittleEndian.PutUint32(b[20:24], uint32(n.dirLen+3))
			binary.LittleEndian.PutUint32(b[24:28], n.dirStartBlk)
			parent := n.inodeNum
			if n.parent != nil {
				parent = n.parent.inodeNum
			}
			binary.LittleEndian.PutUint32(b[28:32], parent)
			binary.LittleEndian.PutUint16(b[32:34], 0)
			binary.LittleEndian.PutUint16(b[34:36], n.dirStartOff)
			binary.LittleEndian.PutUint32(b[36:40], squashNoXattr)
			return b, nil
		}

		b := make([]byte, 32)
		putBaseInode(b, n, mode)
		binary.LittleEndian.PutUint32(b[16:20], n.dirStartBlk)
		binary.LittleEndian.PutUint32(b[20:24], uint32(len(n.children)+2))
		binary.LittleEndian.PutUint16(b[24:26], uint16(n.dirLen+3))
		binary.LittleEndian.PutUint16(b[26:28], n.dirStartOff)
		parent := n.inodeNum
		if n.parent != nil {
			parent = n.parent.inodeNum
		}
		binary.LittleEndian.PutUint32(b[28:32], parent)
		return b, nil

	case nodeRegular:
		if n.inodeType == squashInodeLongFile {
			b := make([]byte, 56+4*len(n.fileBlocks))
			putBaseInode(b, n, mode)
			binary.LittleEndian.PutUint64(b[16:24], n.fileStartRel)
			binary.LittleEndian.PutUint64(b[24:32], n.size)
			binary.LittleEndian.PutUint64(b[32:40], 0)
			binary.LittleEndian.PutUint32(b[40:44], 1)
			binary.LittleEndian.PutUint32(b[44:48], squashNoFragment)
			binary.LittleEndian.PutUint32(b[48:52], 0)
			binary.LittleEndian.PutUint32(b[52:56], squashNoXattr)
			for i, enc := range n.fileBlocks {
				binary.LittleEndian.PutUint32(b[56+i*4:60+i*4], enc)
			}
			return b, nil
		}

		b := make([]byte, 32+4*len(n.fileBlocks))
		putBaseInode(b, n, mode)
		binary.LittleEndian.PutUint32(b[16:20], uint32(n.fileStartRel))
		binary.LittleEndian.PutUint32(b[20:24], squashNoFragment)
		binary.LittleEndian.PutUint32(b[24:28], 0)
		binary.LittleEndian.PutUint32(b[28:32], uint32(n.size))
		for i, enc := range n.fileBlocks {
			binary.LittleEndian.PutUint32(b[32+i*4:36+i*4], enc)
		}
		return b, nil

	case nodeSymlink:
		b := make([]byte, 24+len(n.link))
		putBaseInode(b, n, mode)
		binary.LittleEndian.PutUint32(b[16:20], 1)
		binary.LittleEndian.PutUint32(b[20:24], uint32(len(n.link)))
		copy(b[24:], []byte(n.link))
		return b, nil
	}

	return nil, fmt.Errorf("unknown inode kind for %s", n.absPath)
}

func writeDirRecords(mw *metaWriter, d *node) error {
	if len(d.children) == 0 {
		return nil
	}

	i := 0
	for i < len(d.children) {
		start := i
		startBlk := uint32(d.children[i].inodeRef >> 16)
		for i < len(d.children) && uint32(d.children[i].inodeRef>>16) == startBlk && (i-start) < 256 {
			i++
		}
		count := i - start

		hdr := make([]byte, 12)
		binary.LittleEndian.PutUint32(hdr[0:4], uint32(count-1))
		binary.LittleEndian.PutUint32(hdr[4:8], startBlk)
		binary.LittleEndian.PutUint32(hdr[8:12], d.children[start].inodeNum)
		if err := mw.write(hdr); err != nil {
			return err
		}

		for j := start; j < i; j++ {
			c := d.children[j]
			name := []byte(c.name)
			if len(name) == 0 || len(name) > 65536 {
				return fmt.Errorf("invalid entry name length %d in %s", len(name), c.absPath)
			}

			e := make([]byte, 8)
			binary.LittleEndian.PutUint16(e[0:2], uint16(c.inodeRef&0xffff))
			binary.LittleEndian.PutUint16(e[2:4], 0)
			binary.LittleEndian.PutUint16(e[4:6], dirEntryType(c))
			binary.LittleEndian.PutUint16(e[6:8], uint16(len(name)-1))
			if err := mw.write(e); err != nil {
				return err
			}
			if err := mw.write(name); err != nil {
				return err
			}
		}
	}

	return nil
}

func putBaseInode(b []byte, n *node, mode uint16) {
	binary.LittleEndian.PutUint16(b[0:2], n.inodeType)
	binary.LittleEndian.PutUint16(b[2:4], mode)
	binary.LittleEndian.PutUint16(b[4:6], n.uidIndex)
	binary.LittleEndian.PutUint16(b[6:8], n.gidIndex)
	binary.LittleEndian.PutUint32(b[8:12], n.mtime)
	binary.LittleEndian.PutUint32(b[12:16], n.inodeNum)
}

func inodeMode(n *node) (uint16, error) {
	perm := uint16(n.mode.Perm())
	switch n.kind {
	case nodeDirectory:
		return perm | 0x4000, nil
	case nodeRegular:
		return perm | 0x8000, nil
	case nodeSymlink:
		if perm == 0 {
			perm = 0o777
		}
		return perm | 0xa000, nil
	default:
		return 0, fmt.Errorf("unknown node kind for %s", n.absPath)
	}
}

func dirEntryType(n *node) uint16 {
	switch n.kind {
	case nodeDirectory:
		return squashInodeBasicDir
	case nodeRegular:
		return squashInodeBasicFile
	case nodeSymlink:
		return squashInodeBasicSym
	default:
		return 0
	}
}

type metaWriter struct {
	ws   *writeState
	buf  [squashMetaBlockSize]byte
	used int
}

func newMetaWriter(ws *writeState) *metaWriter {
	return &metaWriter{ws: ws}
}

func (mw *metaWriter) write(p []byte) error {
	for len(p) > 0 {
		left := len(mw.buf) - mw.used
		if left == 0 {
			if err := mw.flush(); err != nil {
				return err
			}
			left = len(mw.buf)
		}
		n := left
		if len(p) < n {
			n = len(p)
		}
		copy(mw.buf[mw.used:], p[:n])
		mw.used += n
		p = p[n:]
	}
	return nil
}

func (mw *metaWriter) close() error {
	if mw.used == 0 {
		return nil
	}
	return mw.flush()
}

func (mw *metaWriter) flush() error {
	if mw.used == 0 {
		return nil
	}

	var hdr [2]byte
	binary.LittleEndian.PutUint16(hdr[:], squashMetaUncompressed|uint16(mw.used))
	if err := mw.ws.write(hdr[:]); err != nil {
		return err
	}
	if err := mw.ws.write(mw.buf[:mw.used]); err != nil {
		return err
	}
	mw.used = 0
	return nil
}

func newSIFHeaderAndDescriptor(arch string, now int64, squashOffset int64, squashSize int64) (sifHeader, sifDescriptor, error) {
	if squashOffset < (sifHeaderSize + sifDescriptorSize) {
		return sifHeader{}, sifDescriptor{}, errors.New("invalid squashfs offset")
	}

	dataOffset := int64(sifHeaderSize + sifDescriptorSize)
	sizeWithPad := squashSize + (squashOffset - dataOffset)

	var h sifHeader
	copy(h.Magic[:], []byte("SIF_MAGIC\x00"))
	copy(h.Version[:], []byte{'0', '1', 0})
	h.Arch = sifArch(arch)
	if _, err := io.ReadFull(rand.Reader, h.UUID[:]); err != nil {
		return sifHeader{}, sifDescriptor{}, fmt.Errorf("generate UUID bytes: %w", err)
	}
	h.CreatedAt = now
	h.ModifiedAt = now
	h.DescriptorsFree = 0
	h.DescriptorsTotal = 1
	h.DescriptorsOffset = sifHeaderSize
	h.DescriptorsSize = sifDescriptorSize
	h.DataOffset = dataOffset
	h.DataSize = sizeWithPad

	var d sifDescriptor
	d.DataType = sifDataPartition
	d.Used = true
	d.ID = 1
	d.GroupID = sifGroupMask | 1
	d.LinkedID = 0
	d.Offset = squashOffset
	d.Size = squashSize
	d.SizeWithPadding = sizeWithPad
	d.CreatedAt = now
	d.ModifiedAt = now
	copy(d.Name[:], []byte("rootfs"))

	meta := sifPartitionExtra{FSType: 1, PartType: 2, Arch: sifArch(arch)}
	metaBytes, err := marshalLE(meta)
	if err != nil {
		return sifHeader{}, sifDescriptor{}, fmt.Errorf("encode partition descriptor metadata: %w", err)
	}
	copy(d.Extra[:], metaBytes)

	return h, d, nil
}

func writeSIFHeaderAndDescriptor(f *os.File, h sifHeader, d sifDescriptor) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek output file for SIF header: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, &h); err != nil {
		return fmt.Errorf("write SIF header: %w", err)
	}
	if _, err := f.Seek(sifHeaderSize, io.SeekStart); err != nil {
		return fmt.Errorf("seek output file for SIF descriptor: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, &d); err != nil {
		return fmt.Errorf("write SIF descriptor: %w", err)
	}
	return nil
}

func (ws *writeState) seekRelative(rel uint64) error {
	ws.relPos = rel
	_, err := ws.f.Seek(ws.base+int64(rel), io.SeekStart)
	if err != nil {
		return fmt.Errorf("seek output file: %w", err)
	}
	return nil
}

func (ws *writeState) write(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	n, err := ws.f.Write(p)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	ws.relPos += uint64(n)
	return nil
}

func (ws *writeState) writeAt(rel uint64, value any) error {
	if _, err := ws.f.Seek(ws.base+int64(rel), io.SeekStart); err != nil {
		return fmt.Errorf("seek output file: %w", err)
	}
	if err := binary.Write(ws.f, binary.LittleEndian, value); err != nil {
		return err
	}
	_, err := ws.f.Seek(ws.base+int64(ws.relPos), io.SeekStart)
	if err != nil {
		return fmt.Errorf("restore output file position: %w", err)
	}
	return nil
}

func marshalLE(v any) ([]byte, error) {
	var b strings.Builder
	w := &stringWriter{b: &b}
	if err := binary.Write(w, binary.LittleEndian, v); err != nil {
		return nil, err
	}
	return []byte(b.String()), nil
}

type stringWriter struct {
	b *strings.Builder
}

func (w *stringWriter) Write(p []byte) (int, error) {
	return w.b.Write(p)
}

func normalizeArch(arch string) string {
	a := strings.TrimSpace(strings.ToLower(arch))
	if a == "" {
		a = runtime.GOARCH
	}
	switch a {
	case "x86_64":
		return "amd64"
	case "aarch64":
		return "arm64"
	default:
		return a
	}
}

func sifArch(arch string) [3]byte {
	switch normalizeArch(arch) {
	case "386":
		return [3]byte{'0', '1', 0}
	case "amd64":
		return [3]byte{'0', '2', 0}
	case "arm":
		return [3]byte{'0', '3', 0}
	case "arm64":
		return [3]byte{'0', '4', 0}
	case "ppc64":
		return [3]byte{'0', '5', 0}
	case "ppc64le":
		return [3]byte{'0', '6', 0}
	case "mips":
		return [3]byte{'0', '7', 0}
	case "mipsle":
		return [3]byte{'0', '8', 0}
	case "mips64":
		return [3]byte{'0', '9', 0}
	case "mips64le":
		return [3]byte{'1', '0', 0}
	case "s390x":
		return [3]byte{'1', '1', 0}
	case "riscv64":
		return [3]byte{'1', '2', 0}
	default:
		return [3]byte{'0', '0', 0}
	}
}

func toUnix32(t time.Time) uint32 {
	u := t.Unix()
	if u < 0 {
		return 0
	}
	if u > int64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(u)
}

func fileInfoIDs(info fs.FileInfo) (uint32, uint32) {
	if info == nil {
		return 0, 0
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint32(st.Uid), uint32(st.Gid)
	}
	return 0, 0
}

func blockLog2(v uint32) uint16 {
	var n uint16
	for v > 1 {
		v >>= 1
		n++
	}
	return n
}

func alignUp(v int64, align int64) int64 {
	if align <= 1 {
		return v
	}
	mod := v % align
	if mod == 0 {
		return v
	}
	return v + align - mod
}

type fetchResult struct {
	ImageRef      string       `json:"image_ref"`
	Architecture  string       `json:"architecture"`
	OutputDir     string       `json:"output_dir"`
	ConfigPath    string       `json:"config_path"`
	Layers        []fetchLayer `json:"layers"`
	ResolvedImage string       `json:"resolved_image_name"`
}

type fetchLayer struct {
	Digest    string `json:"digest"`
	MediaType string `json:"media_type"`
	Path      string `json:"path"`
}

type OCIImageConfig struct {
	ImageRef  string             `json:"-"`
	Bootstrap string             `json:"-"`
	Config    OCIContainerConfig `json:"config"`
}

type OCIContainerConfig struct {
	Env        []string          `json:"Env"`
	User       string            `json:"User"`
	WorkingDir string            `json:"WorkingDir"`
	Entrypoint []string          `json:"Entrypoint"`
	Cmd        []string          `json:"Cmd"`
	Labels     map[string]string `json:"Labels"`
}

func ParseOCIImageConfig(data []byte) (OCIImageConfig, error) {
	var cfg OCIImageConfig
	if len(data) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return OCIImageConfig{}, fmt.Errorf("decode OCI image config: %w", err)
	}
	return cfg, nil
}

type LayerSource struct {
	Name      string
	MediaType string
	Open      func() (io.ReadCloser, error)
}

func WriteFromFetchDir(fetchDir, outPath, arch string) error {
	if strings.TrimSpace(fetchDir) == "" {
		return fmt.Errorf("fetch directory is required")
	}
	if strings.TrimSpace(outPath) == "" {
		return fmt.Errorf("output path is required")
	}

	absFetch, err := filepath.Abs(fetchDir)
	if err != nil {
		return fmt.Errorf("resolve fetch directory: %w", err)
	}

	result, err := readFetchResult(absFetch)
	if err != nil {
		return err
	}

	metaArch := normalizeArch(result.Architecture)
	normArch := normalizeArch(arch)
	if strings.TrimSpace(arch) == "" {
		normArch = metaArch
	}
	if metaArch != "" && normArch != "" && metaArch != normArch {
		return fmt.Errorf("arch mismatch: requested %q, fetched image is %q", normArch, metaArch)
	}

	layers := make([]LayerSource, 0, len(result.Layers))
	for _, layer := range result.Layers {
		layerPath := layer.Path
		layers = append(layers, LayerSource{
			Name:      layerPath,
			MediaType: layer.MediaType,
			Open: func() (io.ReadCloser, error) {
				f, err := os.Open(layerPath)
				if err != nil {
					return nil, fmt.Errorf("open layer %s: %w", layerPath, err)
				}
				return f, nil
			},
		})
	}

	var cfg OCIImageConfig
	if strings.TrimSpace(result.ConfigPath) != "" {
		data, err := os.ReadFile(result.ConfigPath)
		if err != nil {
			return fmt.Errorf("read OCI config %s: %w", result.ConfigPath, err)
		}
		cfg, err = ParseOCIImageConfig(data)
		if err != nil {
			return err
		}
	}
	cfg.ImageRef = result.ImageRef
	cfg.Bootstrap = "docker"

	return WriteFromLayerSourcesWithConfig(layers, outPath, normArch, cfg)
}

func WriteFromLayerSources(layers []LayerSource, outPath, arch string) error {
	return WriteFromLayerSourcesWithConfig(layers, outPath, arch, OCIImageConfig{})
}

func WriteFromLayerSourcesWithConfig(layers []LayerSource, outPath, arch string, cfg OCIImageConfig) error {
	if len(layers) == 0 {
		return fmt.Errorf("at least one OCI layer is required")
	}
	if strings.TrimSpace(outPath) == "" {
		return fmt.Errorf("output path is required")
	}

	normArch := normalizeArch(arch)

	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create output file %s: %w", outPath, err)
	}
	defer f.Close()

	dataOffset := int64(sifHeaderSize + sifDescriptorSize)
	squashOffset := alignUp(dataOffset, 4096)
	if _, err := f.Write(make([]byte, squashOffset)); err != nil {
		return fmt.Errorf("initialize output file: %w", err)
	}

	ws := &writeState{f: f, base: squashOffset, relPos: 0}
	if err := ws.seekRelative(0); err != nil {
		return err
	}

	if err := reserveSquashSuperblock(ws); err != nil {
		return err
	}

	root, allNodes, err := buildTreeAndWriteDataFromLayers(ws, layers, cfg)
	if err != nil {
		return err
	}

	squashSize, err := writeSquashFSPrepared(ws, root, allNodes)
	if err != nil {
		return err
	}

	now := time.Now().Unix()
	hdr, desc, err := newSIFHeaderAndDescriptor(normArch, now, squashOffset, squashSize)
	if err != nil {
		return err
	}

	if err := writeSIFHeaderAndDescriptor(f, hdr, desc); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync output file: %w", err)
	}

	return nil
}

func readFetchResult(fetchDir string) (*fetchResult, error) {
	resultPath := filepath.Join(fetchDir, "fetch-result.json")
	data, err := os.ReadFile(resultPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", resultPath, err)
	}
	var r fetchResult
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("decode %s: %w", resultPath, err)
	}
	if len(r.Layers) == 0 {
		return nil, fmt.Errorf("no layers in %s", resultPath)
	}

	for i := range r.Layers {
		p := r.Layers[i].Path
		if !filepath.IsAbs(p) {
			p = filepath.Join(fetchDir, p)
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			return nil, fmt.Errorf("resolve layer path %q: %w", p, err)
		}
		r.Layers[i].Path = abs
	}

	return &r, nil
}

func buildTreeAndWriteDataFromLayers(ws *writeState, layers []LayerSource, cfg OCIImageConfig) (*node, []*node, error) {
	root := &node{
		name:  "",
		kind:  nodeDirectory,
		mode:  0o755 | os.ModeDir,
		mtime: toUnix32(time.Now()),
	}
	nodeByPath := map[string]*node{"": root}
	ownerByPath := map[string]int{"": len(layers)}

	whiteoutPath := make(map[string]int)
	opaqueDir := make(map[string]int)

	buf := make([]byte, squashBlockSize)
	compressor, err := newDataBlockCompressor()
	if err != nil {
		return nil, nil, err
	}

	for layerIndex := len(layers) - 1; layerIndex >= 0; layerIndex-- {
		layer := layers[layerIndex]
		rc, err := openLayerStream(layer)
		if err != nil {
			return nil, nil, err
		}

		tr := tar.NewReader(rc)
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				rc.Close()
				return nil, nil, fmt.Errorf("read tar header from %s: %w", layer.Name, err)
			}

			rel, err := cleanRelPath(hdr.Name)
			if err != nil {
				rc.Close()
				return nil, nil, fmt.Errorf("invalid path %q in %s: %w", hdr.Name, layer.Name, err)
			}
			if rel == "" {
				continue
			}

			base := path.Base(rel)
			dir := path.Dir(rel)
			if dir == "." {
				dir = ""
			}

			if base == ".wh..wh..opq" {
				ensureDirAtLayer(nodeByPath, ownerByPath, dir, hdr.ModTime, layerIndex)
				removeOwnedChildren(nodeByPath, ownerByPath, dir, layerIndex)
				setLayerMarker(opaqueDir, dir, layerIndex)
				continue
			}

			if strings.HasPrefix(base, ".wh.") {
				victim := path.Join(dir, strings.TrimPrefix(base, ".wh."))
				removeOwnedSubtree(nodeByPath, ownerByPath, victim, layerIndex)
				setLayerMarker(whiteoutPath, victim, layerIndex)
				continue
			}

			if pathBlockedByHigherLayers(rel, layerIndex, nodeByPath, ownerByPath, whiteoutPath, opaqueDir) {
				continue
			}

			ensureDirAtLayer(nodeByPath, ownerByPath, dir, hdr.ModTime, layerIndex)

			switch hdr.Typeflag {
			case tar.TypeDir:
				n := &node{
					name:  path.Base(rel),
					kind:  nodeDirectory,
					mode:  os.FileMode(hdr.Mode) | os.ModeDir,
					mtime: toUnix32(hdr.ModTime),
					uid:   tarID(hdr.Uid),
					gid:   tarID(hdr.Gid),
				}
				setNodeAtLayer(nodeByPath, ownerByPath, rel, n, layerIndex)

			case tar.TypeReg, tar.TypeRegA:
				if hdr.Size < 0 {
					rc.Close()
					return nil, nil, fmt.Errorf("negative file size for %s in %s", rel, layer.Name)
				}

				startRel := ws.relPos
				blocks := make([]uint32, 0, int((hdr.Size+int64(squashBlockSize)-1)/int64(squashBlockSize)))
				remaining := uint64(hdr.Size)
				for remaining > 0 {
					chunk := len(buf)
					if remaining < uint64(chunk) {
						chunk = int(remaining)
					}
					nr, err := io.ReadFull(tr, buf[:chunk])
					if err != nil {
						rc.Close()
						return nil, nil, fmt.Errorf("read file payload for %s from %s: %w", rel, layer.Name, err)
					}
					enc, err := writeDataBlock(ws, compressor, buf[:nr])
					if err != nil {
						rc.Close()
						return nil, nil, fmt.Errorf("write file payload for %s: %w", rel, err)
					}
					blocks = append(blocks, enc)
					remaining -= uint64(nr)
				}

				n := &node{
					name:         path.Base(rel),
					kind:         nodeRegular,
					mode:         os.FileMode(hdr.Mode),
					mtime:        toUnix32(hdr.ModTime),
					uid:          tarID(hdr.Uid),
					gid:          tarID(hdr.Gid),
					size:         uint64(hdr.Size),
					fileStartRel: startRel,
					fileBlocks:   blocks,
				}
				setNodeAtLayer(nodeByPath, ownerByPath, rel, n, layerIndex)

			case tar.TypeSymlink:
				linkMode := os.FileMode(hdr.Mode & 0o777)
				if linkMode == 0 {
					linkMode = 0o777
				}
				n := &node{
					name:  path.Base(rel),
					kind:  nodeSymlink,
					mode:  os.ModeSymlink | linkMode,
					mtime: toUnix32(hdr.ModTime),
					uid:   tarID(hdr.Uid),
					gid:   tarID(hdr.Gid),
					link:  hdr.Linkname,
				}
				setNodeAtLayer(nodeByPath, ownerByPath, rel, n, layerIndex)

			case tar.TypeLink:
				target := resolveHardlinkTarget(nodeByPath, rel, hdr.Linkname)
				targetNode := nodeByPath[target]
				if targetNode == nil || targetNode.kind != nodeRegular {
					rc.Close()
					return nil, nil, fmt.Errorf("invalid hardlink %q -> %q in %s", rel, hdr.Linkname, layer.Name)
				}
				n := &node{
					name:         path.Base(rel),
					kind:         nodeRegular,
					mode:         os.FileMode(hdr.Mode),
					mtime:        toUnix32(hdr.ModTime),
					uid:          tarID(hdr.Uid),
					gid:          tarID(hdr.Gid),
					size:         targetNode.size,
					fileStartRel: targetNode.fileStartRel,
					fileBlocks:   append([]uint32(nil), targetNode.fileBlocks...),
				}
				setNodeAtLayer(nodeByPath, ownerByPath, rel, n, layerIndex)

			default:
				continue
			}
		}

		if err := rc.Close(); err != nil {
			return nil, nil, fmt.Errorf("close layer %s: %w", layer.Name, err)
		}
	}

	if err := addOCICompatibilityFiles(ws, nodeByPath, ownerByPath, cfg); err != nil {
		return nil, nil, err
	}

	return finalizeNodeTree(nodeByPath)
}

func finalizeNodeTree(nodeByPath map[string]*node) (*node, []*node, error) {
	root := nodeByPath[""]
	if root == nil {
		return nil, nil, fmt.Errorf("internal error: missing root node")
	}

	for _, n := range nodeByPath {
		n.children = nil
		n.parent = nil
	}

	keys := make([]string, 0, len(nodeByPath))
	for k := range nodeByPath {
		if k == "" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		di := strings.Count(keys[i], "/")
		dj := strings.Count(keys[j], "/")
		if di != dj {
			return di < dj
		}
		return keys[i] < keys[j]
	})

	for _, k := range keys {
		n := nodeByPath[k]
		parentKey := path.Dir(k)
		if parentKey == "." {
			parentKey = ""
		}
		p := nodeByPath[parentKey]
		if p == nil || p.kind != nodeDirectory {
			return nil, nil, fmt.Errorf("internal tree error: missing directory parent for %s", k)
		}
		n.parent = p
		if n.name == "" {
			n.name = path.Base(k)
		}
		p.children = append(p.children, n)
	}

	all := make([]*node, 0, len(nodeByPath))
	all = append(all, root)
	for _, k := range keys {
		all = append(all, nodeByPath[k])
	}
	for _, n := range all {
		if len(n.children) > 1 {
			sort.Slice(n.children, func(i, j int) bool { return n.children[i].name < n.children[j].name })
		}
	}

	return root, all, nil
}

func addOCICompatibilityFiles(ws *writeState, nodeByPath map[string]*node, ownerByPath map[string]int, cfg OCIImageConfig) error {
	metaLayer := int(^uint(0) >> 1)
	now := toUnix32(time.Now())
	ensureSyntheticDir(nodeByPath, ownerByPath, ".singularity.d", now, metaLayer)
	ensureSyntheticDir(nodeByPath, ownerByPath, ".singularity.d/env", now, metaLayer)

	env := renderOCIEnvironment(cfg.Config)
	if env != "" {
		if err := addSyntheticFile(ws, nodeByPath, ownerByPath, ".singularity.d/env/10-docker2singularity.sh", []byte(env), 0o644, now, metaLayer); err != nil {
			return err
		}
	}
	if err := addSyntheticFile(ws, nodeByPath, ownerByPath, ".singularity.d/env/90-environment.sh", []byte(apptainerEnvironmentTemplate), 0o644, now, metaLayer); err != nil {
		return err
	}

	runscript := renderOCIRunscript(cfg.Config)
	if runscript != "" {
		if err := addSyntheticFile(ws, nodeByPath, ownerByPath, ".singularity.d/runscript", []byte(runscript), 0o755, now, metaLayer); err != nil {
			return err
		}
	}
	if err := addSyntheticFile(ws, nodeByPath, ownerByPath, ".singularity.d/startscript", []byte(apptainerStartscriptTemplate), 0o755, now, metaLayer); err != nil {
		return err
	}
	if source := renderDefinitionFile(cfg); source != "" {
		if err := addSyntheticFile(ws, nodeByPath, ownerByPath, ".singularity.d/Singularity", []byte(source), 0o644, now, metaLayer); err != nil {
			return err
		}
	}

	labels := renderLabels(cfg)
	if len(labels) > 0 {
		data, err := json.MarshalIndent(labels, "", "\t")
		if err != nil {
			return fmt.Errorf("encode OCI labels: %w", err)
		}
		if err := addSyntheticFile(ws, nodeByPath, ownerByPath, ".singularity.d/labels.json", data, 0o644, now, metaLayer); err != nil {
			return err
		}
	}

	return nil
}

func ensureSyntheticDir(nodeByPath map[string]*node, ownerByPath map[string]int, rel string, mtime uint32, layer int) *node {
	rel = strings.Trim(strings.TrimPrefix(path.Clean("/"+rel), "/"), "/")
	if rel == "" {
		return nodeByPath[""]
	}
	parentRel := path.Dir(rel)
	if parentRel == "." {
		parentRel = ""
	}
	ensureSyntheticDir(nodeByPath, ownerByPath, parentRel, mtime, layer)
	if n := nodeByPath[rel]; n != nil && n.kind == nodeDirectory {
		return n
	}
	n := &node{
		name:  path.Base(rel),
		kind:  nodeDirectory,
		mode:  os.ModeDir | 0o755,
		mtime: mtime,
	}
	nodeByPath[rel] = n
	ownerByPath[rel] = layer
	return n
}

func addSyntheticFile(ws *writeState, nodeByPath map[string]*node, ownerByPath map[string]int, rel string, data []byte, mode os.FileMode, mtime uint32, layer int) error {
	rel = strings.Trim(strings.TrimPrefix(path.Clean("/"+rel), "/"), "/")
	if rel == "" {
		return fmt.Errorf("synthetic file path cannot be empty")
	}
	parentRel := path.Dir(rel)
	if parentRel == "." {
		parentRel = ""
	}
	ensureSyntheticDir(nodeByPath, ownerByPath, parentRel, mtime, layer)

	startRel := ws.relPos
	originalSize := len(data)
	blocks := make([]uint32, 0, (len(data)+squashBlockSize-1)/squashBlockSize)
	compressor, err := newDataBlockCompressor()
	if err != nil {
		return err
	}
	for len(data) > 0 {
		chunk := data
		if len(chunk) > squashBlockSize {
			chunk = chunk[:squashBlockSize]
		}
		enc, err := writeDataBlock(ws, compressor, chunk)
		if err != nil {
			return fmt.Errorf("write synthetic file %s: %w", rel, err)
		}
		blocks = append(blocks, enc)
		data = data[len(chunk):]
	}

	nodeByPath[rel] = &node{
		name:         path.Base(rel),
		kind:         nodeRegular,
		mode:         mode,
		mtime:        mtime,
		size:         uint64(originalSize),
		fileStartRel: startRel,
		fileBlocks:   blocks,
	}
	ownerByPath[rel] = layer
	return nil
}

func renderOCIEnvironment(cfg OCIContainerConfig) string {
	var b strings.Builder
	b.WriteString("#!/bin/sh\n")
	for _, kv := range cfg.Env {
		key, value, ok := strings.Cut(kv, "=")
		if !ok || !validShellName(key) {
			continue
		}
		if key == "PATH" {
			fmt.Fprintf(&b, "export %s=%s\n", key, shellDoubleQuote(value))
		} else {
			fmt.Fprintf(&b, "export %s=\"${%s:-%s}\"\n", key, key, shellDoubleQuote(value))
		}
	}
	return b.String()
}

func renderOCIRunscript(cfg OCIContainerConfig) string {
	if len(cfg.Entrypoint) == 0 && len(cfg.Cmd) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("#!/bin/sh\n")
	fmt.Fprintf(&b, "OCI_ENTRYPOINT='%s'\n", ociShellWords(cfg.Entrypoint))
	fmt.Fprintf(&b, "OCI_CMD='%s'\n\n", ociShellWords(cfg.Cmd))
	b.WriteString(`# When SINGULARITY_NO_EVAL set, use OCI compatible behavior that does
# not evaluate resolved CMD / ENTRYPOINT / ARGS through the shell, and
# does not modify expected quoting behavior of args.
if [ -n "$SINGULARITY_NO_EVAL" ]; then
    # ENTRYPOINT only - run entrypoint plus args
    if [ -z "$OCI_CMD" ] && [ -n "$OCI_ENTRYPOINT" ]; then
`)
	writeNoEvalSet(&b, cfg.Entrypoint, false)
	b.WriteString(`
        exec "$@"
    fi

    # CMD only - run CMD or override with args
    if [ -n "$OCI_CMD" ] && [ -z "$OCI_ENTRYPOINT" ]; then
        if [ $# -eq 0 ]; then
`)
	writeNoEvalSet(&b, cfg.Cmd, true)
	b.WriteString(`
        fi
        exec "$@"
    fi

    # ENTRYPOINT and CMD - run ENTRYPOINT with CMD as default args
    # override with user provided args
    if [ $# -gt 0 ]; then
`)
	writeNoEvalSet(&b, cfg.Entrypoint, false)
	b.WriteString(`
	else
`)
	writeNoEvalSet(&b, cfg.Cmd, true)
	b.WriteByte('\n')
	writeNoEvalSet(&b, cfg.Entrypoint, false)
	b.WriteString(`
    fi
    exec "$@"
fi

# Standard Apptainer behavior evaluates CMD / ENTRYPOINT / ARGS
# combination through shell before exec, and requires special quoting
# due to concatenation of CMDLINE_ARGS.
CMDLINE_ARGS=""
# prepare command line arguments for evaluation
for arg in "$@"; do
        CMDLINE_ARGS="${CMDLINE_ARGS} \"$arg\""
done

# ENTRYPOINT only - run entrypoint plus args
if [ -z "$OCI_CMD" ] && [ -n "$OCI_ENTRYPOINT" ]; then
    if [ $# -gt 0 ]; then
        SINGULARITY_OCI_RUN="${OCI_ENTRYPOINT} ${CMDLINE_ARGS}"
    else
        SINGULARITY_OCI_RUN="${OCI_ENTRYPOINT}"
    fi
fi

# CMD only - run CMD or override with args
if [ -n "$OCI_CMD" ] && [ -z "$OCI_ENTRYPOINT" ]; then
    if [ $# -gt 0 ]; then
        SINGULARITY_OCI_RUN="${CMDLINE_ARGS}"
    else
        SINGULARITY_OCI_RUN="${OCI_CMD}"
    fi
fi

# ENTRYPOINT and CMD - run ENTRYPOINT with CMD as default args
# override with user provided args
if [ $# -gt 0 ]; then
    SINGULARITY_OCI_RUN="${OCI_ENTRYPOINT} ${CMDLINE_ARGS}"
else
    SINGULARITY_OCI_RUN="${OCI_ENTRYPOINT} ${OCI_CMD}"
fi

# Evaluate shell expressions first and set arguments accordingly,
# then execute final command as first container process
eval "set ${SINGULARITY_OCI_RUN}"
exec "$@"
`)
	return b.String()
}

func writeNoEvalSet(b *strings.Builder, args []string, appendArgs bool) {
	for i := len(args) - 1; i >= 0; i-- {
		fmt.Fprintf(b, "        set -- %s", shellQuote(args[i]))
		if appendArgs || i < len(args)-1 {
			b.WriteString(" \"$@\"")
		}
		b.WriteByte('\n')
	}
}

func ociShellWords(args []string) string {
	parts := make([]string, 0, len(args))
	for _, arg := range args {
		parts = append(parts, shellDoubleQuote(arg))
	}
	return strings.Join(parts, " ")
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func shellDoubleQuote(s string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, `$`, `\$`, "`", "\\`")
	return `"` + replacer.Replace(s) + `"`
}

func renderDefinitionFile(cfg OCIImageConfig) string {
	bootstrap := strings.TrimSpace(cfg.Bootstrap)
	imageRef := strings.TrimSpace(cfg.ImageRef)
	if bootstrap == "" && imageRef == "" {
		return ""
	}
	if bootstrap == "" {
		bootstrap = "docker"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "bootstrap: %s\n", bootstrap)
	if imageRef != "" {
		fmt.Fprintf(&b, "from: %s\n", imageRef)
	}
	b.WriteByte('\n')
	return b.String()
}

func renderLabels(cfg OCIImageConfig) map[string]string {
	labels := make(map[string]string, len(cfg.Config.Labels)+4)
	labels["org.label-schema.schema-version"] = "1.0"
	labels["org.label-schema.build-arch"] = normalizeArch("")
	labels["org.label-schema.usage.singularity.deffile.bootstrap"] = defaultString(cfg.Bootstrap, "docker")
	if strings.TrimSpace(cfg.ImageRef) != "" {
		labels["org.label-schema.usage.singularity.deffile.from"] = cfg.ImageRef
	}
	for k, v := range cfg.Config.Labels {
		labels[k] = v
	}
	return labels
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

const apptainerEnvironmentTemplate = `#!/bin/sh
# Custom environment shell code should follow
`

const apptainerStartscriptTemplate = `#!/bin/sh
`

func validShellName(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return false
	}
	return true
}

func tarID(id int) uint32 {
	if id < 0 {
		return 0
	}
	return uint32(id)
}

func setLayerMarker(markers map[string]int, rel string, layer int) {
	rel = normalizeHardlinkPath(rel)
	if prev, ok := markers[rel]; ok && prev >= layer {
		return
	}
	markers[rel] = layer
}

func pathBlockedByHigherLayers(rel string, layer int, nodeByPath map[string]*node, ownerByPath, whiteoutPath, opaqueDir map[string]int) bool {
	if rel == "" {
		return false
	}

	if owner, ok := ownerByPath[rel]; ok && owner > layer {
		return true
	}

	if rootOpaque, ok := opaqueDir[""]; ok && rootOpaque > layer {
		return true
	}

	cur := rel
	for {
		if markLayer, ok := whiteoutPath[cur]; ok && markLayer > layer {
			return true
		}

		parent := path.Dir(cur)
		if parent == "." {
			parent = ""
		}
		if parent == cur {
			break
		}
		if parent != "" {
			if owner, ok := ownerByPath[parent]; ok && owner > layer {
				p := nodeByPath[parent]
				if p != nil && p.kind != nodeDirectory {
					return true
				}
			}
		}
		if parent == "" {
			break
		}
		cur = parent
	}

	cur = path.Dir(rel)
	if cur == "." {
		cur = ""
	}
	for cur != "" {
		if markLayer, ok := opaqueDir[cur]; ok && markLayer > layer {
			return true
		}
		parent := path.Dir(cur)
		if parent == "." {
			parent = ""
		}
		if parent == cur {
			break
		}
		cur = parent
	}

	return false
}

func ensureDirAtLayer(nodeByPath map[string]*node, ownerByPath map[string]int, rel string, mtime time.Time, layer int) {
	rel = normalizeHardlinkPath(rel)
	if rel == "" {
		return
	}

	parts := strings.Split(rel, "/")
	cur := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		if cur == "" {
			cur = part
		} else {
			cur = cur + "/" + part
		}

		existing := nodeByPath[cur]
		existingLayer := ownerByPath[cur]
		if existing != nil && existingLayer > layer {
			if existing.kind != nodeDirectory {
				return
			}
			continue
		}

		if existing != nil && existingLayer == layer {
			if existing.kind == nodeDirectory {
				continue
			}
			removeOwnedSubtree(nodeByPath, ownerByPath, cur, layer)
			existing = nil
		}

		if existing == nil {
			nodeByPath[cur] = &node{
				name:  part,
				kind:  nodeDirectory,
				mode:  0o755 | os.ModeDir,
				mtime: toUnix32(mtime),
			}
			ownerByPath[cur] = layer
		}
	}
}

func setNodeAtLayer(nodeByPath map[string]*node, ownerByPath map[string]int, rel string, n *node, layer int) {
	rel = normalizeHardlinkPath(rel)
	if rel == "" {
		return
	}

	if existingLayer, ok := ownerByPath[rel]; ok {
		if existingLayer > layer {
			return
		}
		if existingLayer == layer {
			removeOwnedSubtree(nodeByPath, ownerByPath, rel, layer)
		} else {
			delete(nodeByPath, rel)
			delete(ownerByPath, rel)
		}
	}

	nodeByPath[rel] = n
	ownerByPath[rel] = layer
}

func removeOwnedChildren(nodeByPath map[string]*node, ownerByPath map[string]int, dir string, layer int) {
	dir = normalizeHardlinkPath(dir)
	if dir == "" {
		for p, own := range ownerByPath {
			if p == "" || own != layer {
				continue
			}
			delete(ownerByPath, p)
			delete(nodeByPath, p)
		}
		return
	}

	prefix := dir + "/"
	for p, own := range ownerByPath {
		if own != layer {
			continue
		}
		if strings.HasPrefix(p, prefix) {
			delete(ownerByPath, p)
			delete(nodeByPath, p)
		}
	}
}

func removeOwnedSubtree(nodeByPath map[string]*node, ownerByPath map[string]int, rel string, layer int) {
	rel = normalizeHardlinkPath(rel)
	if rel == "" {
		return
	}

	if own, ok := ownerByPath[rel]; ok && own == layer {
		delete(ownerByPath, rel)
		delete(nodeByPath, rel)
	}

	prefix := rel + "/"
	for p, own := range ownerByPath {
		if own != layer {
			continue
		}
		if strings.HasPrefix(p, prefix) {
			delete(ownerByPath, p)
			delete(nodeByPath, p)
		}
	}
}

func resolveHardlinkTarget(nodeByPath map[string]*node, relPath, linkname string) string {
	cand := normalizeHardlinkPath(linkname)
	if _, ok := nodeByPath[cand]; ok {
		return cand
	}
	dir := path.Dir(relPath)
	if dir == "." {
		dir = ""
	}
	joined := normalizeHardlinkPath(path.Join(dir, linkname))
	return joined
}

func normalizeHardlinkPath(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "/")
	p = path.Clean(p)
	if p == "." {
		return ""
	}
	return p
}

func cleanRelPath(name string) (string, error) {
	clean := strings.TrimSpace(name)
	clean = strings.TrimPrefix(clean, "./")
	clean = strings.TrimPrefix(clean, "/")
	clean = path.Clean(clean)
	if clean == "." || clean == "" {
		return "", nil
	}
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("path escapes root")
	}
	return clean, nil
}

func openLayerStream(layer LayerSource) (io.ReadCloser, error) {
	if layer.Open == nil {
		return nil, fmt.Errorf("layer %s has no opener", layer.Name)
	}

	rc, err := layer.Open()
	if err != nil {
		return nil, err
	}

	comp, err := layerCompression(layer.MediaType)
	if err != nil {
		rc.Close()
		return nil, err
	}
	if comp == "none" {
		return rc, nil
	}

	gr, err := gzip.NewReader(rc)
	if err != nil {
		rc.Close()
		return nil, fmt.Errorf("open gzip layer %s: %w", layer.Name, err)
	}

	return &gzipLayerReader{rc: rc, gr: gr}, nil
}

func layerCompression(mediaType string) (string, error) {
	lower := strings.ToLower(strings.TrimSpace(mediaType))
	switch {
	case lower == "":
		return "gzip", nil
	case strings.Contains(lower, "tar+gzip"), strings.Contains(lower, "diff.tar.gzip"):
		return "gzip", nil
	case strings.Contains(lower, "tar") && strings.Contains(lower, "gzip"):
		return "gzip", nil
	case strings.Contains(lower, "tar"):
		return "none", nil
	default:
		return "", fmt.Errorf("unsupported layer media type %q", mediaType)
	}
}

type gzipLayerReader struct {
	rc io.Closer
	gr *gzip.Reader
}

func (g *gzipLayerReader) Read(p []byte) (int, error) {
	return g.gr.Read(p)
}

func (g *gzipLayerReader) Close() error {
	err1 := g.gr.Close()
	err2 := g.rc.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
