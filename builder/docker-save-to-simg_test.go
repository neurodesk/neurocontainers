package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

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
