package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/go-github/v61/github"
	"golang.org/x/oauth2"
)

func main() {
	issueNumber := flag.Int("issue", 0, "GitHub issue number to download")
	repoFlag := flag.String("repo", defaultRepo(), "GitHub repository in owner/repo form")
	outputFlag := flag.String("out", "", "Output file path (defaults to local/issue-<n>-comments.json)")
	flag.Parse()

	if *issueNumber <= 0 {
		log.Fatalf("issue number must be provided via --issue")
	}

	owner, repo, err := parseRepo(*repoFlag)
	if err != nil {
		log.Fatalf("parse repo: %v", err)
	}

	ctx := context.Background()
	client, err := newGitHubClient(ctx)
	if err != nil {
		log.Fatalf("github client: %v", err)
	}

	issue, _, err := client.Issues.Get(ctx, owner, repo, *issueNumber)
	if err != nil {
		log.Fatalf("fetch issue: %v", err)
	}

	comments, err := listAllIssueComments(ctx, client, owner, repo, *issueNumber)
	if err != nil {
		log.Fatalf("fetch comments: %v", err)
	}

	outPath := *outputFlag
	if outPath == "" {
		outPath = filepath.Join("local", fmt.Sprintf("issue-%d-comments.json", *issueNumber))
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		log.Fatalf("ensure output dir: %v", err)
	}

	dump := buildDump(*repoFlag, issue, comments)

	file, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(dump); err != nil {
		log.Fatalf("write json: %v", err)
	}

	log.Printf("Wrote %d comments to %s", len(dump.Comments), outPath)
}

type issueDump struct {
	Repository  string        `json:"repository"`
	IssueNumber int           `json:"issue_number"`
	IssueTitle  string        `json:"issue_title"`
	IssueBody   string        `json:"issue_body"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
	RetrievedAt time.Time     `json:"retrieved_at"`
	Comments    []commentDump `json:"comments"`
}

type commentDump struct {
	ID        int64     `json:"id"`
	Author    string    `json:"author"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	URL       string    `json:"url"`
}

func buildDump(repo string, issue *github.Issue, comments []*github.IssueComment) issueDump {
	dump := issueDump{
		Repository:  repo,
		IssueNumber: issue.GetNumber(),
		IssueTitle:  issue.GetTitle(),
		IssueBody:   issue.GetBody(),
		CreatedAt:   issue.GetCreatedAt().Time,
		UpdatedAt:   issue.GetUpdatedAt().Time,
		RetrievedAt: time.Now().UTC(),
	}

	dump.Comments = make([]commentDump, 0, len(comments))
	for _, c := range comments {
		if c == nil {
			continue
		}
		dump.Comments = append(dump.Comments, commentDump{
			ID:        c.GetID(),
			Author:    userLogin(c.GetUser()),
			Body:      c.GetBody(),
			CreatedAt: c.GetCreatedAt().Time,
			UpdatedAt: c.GetUpdatedAt().Time,
			URL:       c.GetHTMLURL(),
		})
	}

	return dump
}

func userLogin(user *github.User) string {
	if user == nil {
		return ""
	}
	return user.GetLogin()
}

func defaultRepo() string {
	if env := strings.TrimSpace(os.Getenv("DASHBOARD_TEST_RUN_REPO")); env != "" {
		return env
	}
	return "neurodesk/neurocontainers"
}

func parseRepo(repo string) (string, string, error) {
	trimmed := strings.TrimSpace(repo)
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid owner/repo format: %q", repo)
	}
	return parts[0], parts[1], nil
}

func newGitHubClient(ctx context.Context) (*github.Client, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}

	token := strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
	if token == "" {
		return github.NewClient(nil), nil
	}

	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
	tc := oauth2.NewClient(ctx, ts)
	return github.NewClient(tc), nil
}

func listAllIssueComments(ctx context.Context, client *github.Client, owner, repo string, number int) ([]*github.IssueComment, error) {
	opt := &github.IssueListCommentsOptions{ListOptions: github.ListOptions{PerPage: 100}}

	var comments []*github.IssueComment
	for {
		batch, resp, err := client.Issues.ListComments(ctx, owner, repo, number, opt)
		if err != nil {
			return nil, fmt.Errorf("list comments: %w", err)
		}
		comments = append(comments, batch...)
		if resp == nil || resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}

	return comments, nil
}
