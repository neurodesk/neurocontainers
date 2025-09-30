package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/go-github/v61/github"
)

func TestParseIssue1504(t *testing.T) {
	data, err := os.ReadFile("local/issue-1504-comments.json")
	if err != nil {
		t.Fatalf("read json: %v", err)
	}

	var dump struct {
		Comments []struct {
			Body string `json:"body"`
		} `json:"comments"`
	}

	if err := json.Unmarshal(data, &dump); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	var comments []*github.IssueComment
	for _, c := range dump.Comments {
		body := c.Body
		comments = append(comments, &github.IssueComment{Body: &body})
	}

	entries := parseTestRunEntries(comments)
	if len(entries) == 0 {
		t.Fatalf("expected entries, got 0")
	}

	amico, ok := entries["amico"]
	if !ok {
		t.Fatalf("expected amico entry, not found")
	}
	if amico.ContainerVersion != "2.1.0" {
		t.Fatalf("expected amico version 2.1.0, got %q", amico.ContainerVersion)
	}
	if len(amico.Tests) == 0 {
		t.Fatalf("expected amico tests, got 0")
	}

	progress := []ContainerProgress{{Name: "amico"}}
	report := &TestRunReport{
		Entries: entries,
		Tested:  []TestRunEntry{amico},
	}

	for i := range progress {
		name := progress[i].Name
		entry, ok := report.Entries[name]
		if !ok {
			for _, candidate := range report.Tested {
				if candidate.Container == name {
					entry = candidate
					ok = true
					break
				}
			}
		}
		if !ok {
			t.Fatalf("expected entry for %s", name)
		}
		entryCopy := entry
		progress[i].TestRun = &entryCopy
	}

	if progress[0].TestRun == nil {
		t.Fatalf("expected test run to be attached")
	}
	if len(progress[0].TestRun.Tests) == 0 {
		t.Fatalf("expected attached tests, got 0")
	}
}
