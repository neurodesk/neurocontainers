package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"testing"
)

func TestCleanArchiveNameRejectsAbsoluteAndTraversalPaths(t *testing.T) {
	for _, name := range []string{
		"../manifest.json",
		"layers/../../manifest.json",
		"/manifest.json",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := cleanArchiveName(name); err == nil {
				t.Fatalf("cleanArchiveName(%q) accepted an unsafe path", name)
			}
		})
	}
}

func TestRegularFileUsesLongInodeWhenDataStartExceeds32Bit(t *testing.T) {
	n := &node{
		kind:         nodeRegular,
		size:         12,
		fileStartRel: 0x1_0000_0000,
	}

	assignInodeTypesAndSizes([]*node{n})

	if n.inodeType != squashInodeLongFile {
		t.Fatalf("inodeType = %d, want %d", n.inodeType, squashInodeLongFile)
	}
}

func TestDockerSaveAcceptsPlainAndCompressedLayers(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		name := "plain"
		if compressed {
			name = "gzip"
		}
		t.Run(name, func(t *testing.T) {
			var layer bytes.Buffer
			writer := tar.NewWriter(&layer)
			payload := []byte("installed application data")
			if err := writer.WriteHeader(&tar.Header{Name: "application.txt", Mode: 0644, Size: int64(len(payload))}); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Write(payload); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			layerBytes := layer.Bytes()
			if compressed {
				var encoded bytes.Buffer
				zw := gzip.NewWriter(&encoded)
				if _, err := zw.Write(layerBytes); err != nil {
					t.Fatal(err)
				}
				if err := zw.Close(); err != nil {
					t.Fatal(err)
				}
				layerBytes = encoded.Bytes()
			}
			manifest, err := json.Marshal([]dockerManifestEntry{{
				Config: "config.json", Layers: []string{"blobs/sha256/layer"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			var archive bytes.Buffer
			tw := tar.NewWriter(&archive)
			for path, data := range map[string][]byte{
				"manifest.json":      manifest,
				"config.json":        []byte(`{"architecture":"amd64","config":{}}`),
				"blobs/sha256/layer": layerBytes,
			} {
				if err := tw.WriteHeader(&tar.Header{Name: path, Mode: 0644, Size: int64(len(data))}); err != nil {
					t.Fatal(err)
				}
				if _, err := tw.Write(data); err != nil {
					t.Fatal(err)
				}
			}
			if err := tw.Close(); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			input, output := filepath.Join(root, "image.tar"), filepath.Join(root, "image.simg")
			if err := os.WriteFile(input, archive.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			if err := convertDockerSave(input, output, "", 0); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type testLayerEntry struct {
	name, content, target string
	kind                  byte
}

func testLayer(entries []testLayerEntry) LayerSource {
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	for _, entry := range entries {
		kind := entry.kind
		if kind == 0 {
			kind = tar.TypeReg
		}
		hdr := &tar.Header{Name: entry.name, Mode: 0644, Typeflag: kind, Linkname: entry.target}
		if kind == tar.TypeReg {
			hdr.Size = int64(len(entry.content))
		}
		if err := tw.WriteHeader(hdr); err != nil {
			panic(err)
		}
		if _, err := tw.Write([]byte(entry.content)); err != nil {
			panic(err)
		}
	}
	if err := tw.Close(); err != nil {
		panic(err)
	}
	payload := append([]byte(nil), data.Bytes()...)
	return LayerSource{Name: "test layer", MediaType: "application/vnd.oci.image.layer.v1.tar", Open: func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(payload)), nil }}
}

func TestLayerHardlinksPreserveTheirOriginalTargets(t *testing.T) {
	file := func(name, data string) testLayerEntry { return testLayerEntry{name: name, content: data} }
	link := func(name, target string) testLayerEntry {
		return testLayerEntry{name: name, target: target, kind: tar.TypeLink}
	}
	tests := []struct {
		name   string
		layers [][]testLayerEntry
		want   map[string]string
		absent []string
	}{
		{"backward", [][]testLayerEntry{{file("target", "old"), link("alias", "target")}}, map[string]string{"target": "old", "alias": "old"}, nil},
		{"forward", [][]testLayerEntry{{link("dir/alias", "dir/target"), file("dir/target", "forward")}}, map[string]string{"dir/target": "forward", "dir/alias": "forward"}, nil},
		{"forward chain", [][]testLayerEntry{{link("first", "second"), link("second", "target"), file("target", "chain")}}, map[string]string{"target": "chain", "first": "chain", "second": "chain"}, nil},
		{"lower layer", [][]testLayerEntry{{file("target", "lower")}, {link("alias", "target")}}, map[string]string{"target": "lower", "alias": "lower"}, nil},
		{"overwritten target", [][]testLayerEntry{{file("target", "old")}, {link("alias", "target")}, {file("target", "new")}}, map[string]string{"target": "new", "alias": "old"}, nil},
		{"same layer overwrite", [][]testLayerEntry{{file("target", "old"), link("alias", "target"), file("target", "new")}}, map[string]string{"target": "new", "alias": "old"}, nil},
		{"whiteouted target", [][]testLayerEntry{{file("target", "old")}, {link("alias", "target")}, {file(".wh.target", "")}}, map[string]string{"alias": "old"}, []string{"target"}},
		{"opaque target parent", [][]testLayerEntry{{file("dir/target", "old")}, {link("alias", "dir/target")}, {file("dir/new", "new"), file("dir/.wh..wh..opq", "")}}, map[string]string{"alias": "old", "dir/new": "new"}, []string{"dir/target"}},
		{"directory replaced by file", [][]testLayerEntry{{file("dir/old", "old")}, {file("dir", "new")}}, map[string]string{"dir": "new"}, []string{"dir/old"}},
		{"file replaced by directory", [][]testLayerEntry{{file("dir", "old")}, {file("dir/new", "new")}}, map[string]string{"dir/new": "new"}, nil},
		{"directory whiteout", [][]testLayerEntry{{file("dir/old", "old"), file("outside", "retained")}, {file(".wh.dir", "")}}, map[string]string{"outside": "retained"}, []string{"dir", "dir/old"}},
		{"whiteout after replacement", [][]testLayerEntry{{file("target", "old")}, {file("target", "new"), file(".wh.target", "")}}, map[string]string{"target": "new"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var layers []LayerSource
			for _, entries := range tt.layers {
				layers = append(layers, testLayer(entries))
			}
			f, err := os.CreateTemp(t.TempDir(), "payloads")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			root, _, err := buildTreeAndWriteDataFromLayers(&writeState{f: f}, layers, OCIImageConfig{})
			if err != nil {
				t.Fatal(err)
			}
			nodes := testNodes(root)
			for name, want := range tt.want {
				n := nodes[name]
				if n == nil {
					t.Fatalf("missing %s", name)
				}
				got := readTestPayload(t, f, n)
				if got != want {
					t.Errorf("%s = %q, want %q", name, got, want)
				}
			}
			for _, name := range tt.absent {
				if nodes[name] != nil {
					t.Errorf("whiteouted path %s survived", name)
				}
			}
		})
	}
}

func testNodes(root *node) map[string]*node {
	out := make(map[string]*node)
	var visit func(*node, string)
	visit = func(n *node, name string) {
		out[name] = n
		for _, child := range n.children {
			visit(child, path.Join(name, child.name))
		}
	}
	visit(root, "")
	return out
}

func readTestPayload(t *testing.T, f *os.File, n *node) string {
	t.Helper()
	var payload bytes.Buffer
	offset := int64(n.fileStartRel)
	for _, block := range n.fileBlocks {
		length := int64(block & squashDataSizeMask)
		reader := io.NewSectionReader(f, offset, length)
		if block&squashDataUncompressed != 0 {
			if _, err := io.Copy(&payload, reader); err != nil {
				t.Fatal(err)
			}
		} else {
			zr, err := zlib.NewReader(reader)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(&payload, zr); err != nil {
				t.Fatal(err)
			}
			if err := zr.Close(); err != nil {
				t.Fatal(err)
			}
		}
		offset += length
	}
	return payload.String()
}

func TestLayerHardlinkToSymlinkPreservesLinkText(t *testing.T) {
	layer := testLayer([]testLayerEntry{{name: "alias", target: "sym", kind: tar.TypeLink}, {name: "sym", target: "../outside", kind: tar.TypeSymlink}})
	f, err := os.CreateTemp(t.TempDir(), "payloads")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	root, _, err := buildTreeAndWriteDataFromLayers(&writeState{f: f}, []LayerSource{layer}, OCIImageConfig{})
	if err != nil {
		t.Fatal(err)
	}
	alias := testNodes(root)["alias"]
	if alias.kind != nodeSymlink || alias.link != "../outside" {
		t.Fatalf("alias = %#v", alias)
	}
}

func TestLayerHardlinksRejectMissingCyclicAndEscapingTargets(t *testing.T) {
	for _, entries := range [][]testLayerEntry{
		{{name: "alias", target: "missing", kind: tar.TypeLink}},
		{{name: "a", target: "b", kind: tar.TypeLink}, {name: "b", target: "a", kind: tar.TypeLink}},
		{{name: "alias", target: "../../outside", kind: tar.TypeLink}},
	} {
		f, err := os.CreateTemp(t.TempDir(), "payloads")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, _, err := buildTreeAndWriteDataFromLayers(&writeState{f: f}, []LayerSource{testLayer(entries)}, OCIImageConfig{}); err == nil {
			t.Errorf("accepted invalid entries %#v", entries)
		}
	}
}

func TestLayerPayloadPassOmitsDiscardedAndDuplicateData(t *testing.T) {
	data := make([]byte, 1<<20)
	rand.New(rand.NewSource(1)).Read(data)
	layers := []LayerSource{
		testLayer([]testLayerEntry{{name: "discarded", content: string(data)}, {name: "target", content: string(data)}}),
		testLayer([]testLayerEntry{{name: ".wh.discarded"}, {name: "alias", target: "target", kind: tar.TypeLink}}),
	}
	f, err := os.CreateTemp(t.TempDir(), "payloads")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ws := &writeState{f: f}
	root, _, err := buildTreeAndWriteDataFromLayers(ws, layers, OCIImageConfig{})
	if err != nil {
		t.Fatal(err)
	}
	nodes := testNodes(root)
	if nodes["alias"].fileStartRel != nodes["target"].fileStartRel {
		t.Fatal("alias data was written twice")
	}
	if ws.relPos > uint64(len(data)+10000) {
		t.Fatalf("wrote discarded payload: %d bytes", ws.relPos)
	}
	if got := readTestPayload(t, f, nodes["alias"]); got != string(data) {
		t.Fatal("alias content differs")
	}
}

func TestLayerHardlinkRetainsTargetModeAndOwnership(t *testing.T) {
	var data bytes.Buffer
	writer := tar.NewWriter(&data)
	for _, hdr := range []*tar.Header{
		{Name: "target", Typeflag: tar.TypeReg, Mode: 0751, Uid: 42, Gid: 84},
		{Name: "alias", Typeflag: tar.TypeLink, Linkname: "target", Mode: 0},
	} {
		if err := writer.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	layer := LayerSource{Name: "permissions", MediaType: "application/vnd.oci.image.layer.v1.tar", Open: func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data.Bytes())), nil }}
	f, err := os.CreateTemp(t.TempDir(), "payloads")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	root, _, err := buildTreeAndWriteDataFromLayers(&writeState{f: f}, []LayerSource{layer}, OCIImageConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"target", "alias"} {
		n := testNodes(root)[name]
		if n.mode.Perm() != 0751 || n.uid != 42 || n.gid != 84 {
			t.Errorf("%s mode=%o uid=%d gid=%d", name, n.mode.Perm(), n.uid, n.gid)
		}
	}
}
