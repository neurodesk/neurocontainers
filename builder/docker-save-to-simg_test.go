package main

import "testing"

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
