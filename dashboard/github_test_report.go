package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"regexp"

	"github.com/google/go-github/v61/github"
	"golang.org/x/oauth2"
)

// TestRunStatus represents the outcome of a container test from the GitHub report.
type TestRunStatus string

const (
	TestRunStatusPassed  TestRunStatus = "passed"
	TestRunStatusFailed  TestRunStatus = "failed"
	TestRunStatusSkipped TestRunStatus = "skipped"
	TestRunStatusUnknown TestRunStatus = "unknown"
)

// TestRunEntry captures the parsed status information for a single container.
type TestRunEntry struct {
	Container        string
	ContainerVersion string
	Status           TestRunStatus
	Details          string
	Info             ContainerRunInfo
	Tests            []ContainerTest
}

// ContainerRunInfo captures high-level metadata about a container test execution.
type ContainerRunInfo struct {
	ImagePath    string
	Runtime      string
	TestsPassed  int
	TestsTotal   int
	TestsFailed  int
	TestsSkipped int
}

// ContainerTest represents the outcome and captured IO for an individual test case.
type ContainerTest struct {
	Name       string
	Status     TestRunStatus
	Summary    string
	ReturnCode *int
	Stdout     string
	Stderr     string
	Details    []string
}

// TestRunSummary aggregates the headline numbers from the GitHub issue body.
type TestRunSummary struct {
	ContainersProcessed int
	Passed              int
	Failed              int
	Skipped             int
}

// TestRunReport represents the combined data parsed from the GitHub issue body and comments.
type TestRunReport struct {
	IssueNumber int
	IssueURL    string
	CreatedAt   time.Time
	Summary     TestRunSummary
	Entries     map[string]TestRunEntry
	Failures    []TestRunEntry
	Skipped     []TestRunEntry
	PassedCount int
	Tested      []TestRunEntry
}

// loadLatestTestRunReport fetches and parses the most recent container test run issue for the configured repository.
func loadLatestTestRunReport(ctx context.Context) (*TestRunReport, error) {
	owner, repo := gitHubRepo()

	client, err := newGitHubClient(ctx)
	if err != nil {
		return nil, err
	}

	issue, err := findLatestTestRunIssue(ctx, client, owner, repo)
	if err != nil {
		return nil, err
	}
	if issue == nil {
		return nil, nil
	}

	summary := parseTestRunSummary(issue.GetBody())

	comments, err := listAllIssueComments(ctx, client, owner, repo, issue.GetNumber())
	if err != nil {
		return nil, err
	}

	entries := parseTestRunEntries(comments)

	var failures []TestRunEntry
	var skipped []TestRunEntry
	var tested []TestRunEntry
	var passedCount int

	for _, entry := range entries {
		switch entry.Status {
		case TestRunStatusFailed:
			failures = append(failures, entry)
		case TestRunStatusSkipped:
			skipped = append(skipped, entry)
		case TestRunStatusPassed:
			passedCount++
		}
		if len(entry.Tests) > 0 {
			tested = append(tested, entry)
		}
	}

	sort.Slice(failures, func(i, j int) bool { return failures[i].Container < failures[j].Container })
	sort.Slice(skipped, func(i, j int) bool { return skipped[i].Container < skipped[j].Container })
	sort.Slice(tested, func(i, j int) bool { return tested[i].Container < tested[j].Container })

	report := &TestRunReport{
		IssueNumber: issue.GetNumber(),
		IssueURL:    issue.GetHTMLURL(),
		CreatedAt:   issue.GetCreatedAt().Time,
		Summary:     summary,
		Entries:     entries,
		Failures:    failures,
		Skipped:     skipped,
		PassedCount: passedCount,
		Tested:      tested,
	}

	return report, nil
}

func gitHubRepo() (owner string, repo string) {
	const defaultRepo = "neurodesk/neurocontainers"

	ownerRepo := strings.TrimSpace(os.Getenv("DASHBOARD_TEST_RUN_REPO"))
	if ownerRepo == "" {
		ownerRepo = defaultRepo
	}

	parts := strings.Split(ownerRepo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "neurodesk", "neurocontainers"
	}
	return parts[0], parts[1]
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

func findLatestTestRunIssue(ctx context.Context, client *github.Client, owner, repo string) (*github.Issue, error) {
	const titlePrefix = "Container test run"

	listOpt := &github.IssueListByRepoOptions{
		State:     "all",
		Sort:      "created",
		Direction: "desc",
		ListOptions: github.ListOptions{
			PerPage: 20,
		},
	}

	for {
		issues, resp, err := client.Issues.ListByRepo(ctx, owner, repo, listOpt)
		if err != nil {
			return nil, fmt.Errorf("list issues: %w", err)
		}
		for _, issue := range issues {
			if issue.IsPullRequest() {
				continue
			}
			if strings.HasPrefix(issue.GetTitle(), titlePrefix) {
				return issue, nil
			}
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
		listOpt.Page = resp.NextPage
	}

	return nil, nil
}

func listAllIssueComments(ctx context.Context, client *github.Client, owner, repo string, number int) ([]*github.IssueComment, error) {
	opt := &github.IssueListCommentsOptions{
		ListOptions: github.ListOptions{PerPage: 100},
	}

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

func parseTestRunSummary(body string) TestRunSummary {
	summary := TestRunSummary{}

	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "-") {
			continue
		}
		parts := strings.SplitN(strings.TrimPrefix(line, "-"), ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		n, err := strconv.Atoi(strings.Fields(value)[0])
		if err != nil {
			continue
		}
		switch strings.ToLower(key) {
		case "containers processed":
			summary.ContainersProcessed = n
		case "passed":
			summary.Passed = n
		case "failed":
			summary.Failed = n
		case "skipped":
			summary.Skipped = n
		}
	}

	return summary
}

func parseTestRunEntries(comments []*github.IssueComment) map[string]TestRunEntry {
	results := make(map[string]TestRunEntry)

	for _, comment := range comments {
		body := comment.GetBody()
		blocks := splitContainerBlocks(body)
		for _, block := range blocks {
			entry := parseContainerBlock(block)
			if entry.Container == "" {
				continue
			}
			current := results[entry.Container]
			results[entry.Container] = mergeTestRunEntries(current, entry)
		}

		for container, entry := range parseSimpleStatusLines(body) {
			current, exists := results[container]
			if !exists || len(current.Tests) == 0 {
				results[container] = mergeTestRunEntries(current, entry)
				continue
			}
			if current.Status == TestRunStatusUnknown && entry.Status != TestRunStatusUnknown {
				current.Status = entry.Status
			}
			if current.Details == "" {
				current.Details = entry.Details
			}
			results[container] = current
		}
	}

	return results
}

func mergeTestRunEntries(base, update TestRunEntry) TestRunEntry {
	if update.Container == "" {
		return base
	}
	if base.Container == "" {
		base.Container = update.Container
	}
	if update.ContainerVersion != "" {
		base.ContainerVersion = update.ContainerVersion
	}
	if update.Status != TestRunStatusUnknown {
		base.Status = update.Status
	}
	if update.Details != "" {
		base.Details = update.Details
	}
	if len(update.Tests) > 0 {
		base.Tests = update.Tests
		base.Info = update.Info
	} else {
		if base.Info.ImagePath == "" && update.Info.ImagePath != "" {
			base.Info.ImagePath = update.Info.ImagePath
		}
		if base.Info.Runtime == "" && update.Info.Runtime != "" {
			base.Info.Runtime = update.Info.Runtime
		}
		if base.Info.TestsTotal == 0 && update.Info.TestsTotal != 0 {
			base.Info.TestsTotal = update.Info.TestsTotal
			base.Info.TestsPassed = update.Info.TestsPassed
			base.Info.TestsFailed = update.Info.TestsFailed
			base.Info.TestsSkipped = update.Info.TestsSkipped
		}
	}
	return base
}

func parseSimpleStatusLines(body string) map[string]TestRunEntry {
	results := make(map[string]TestRunEntry)
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "- ") {
			continue
		}
		status, remainder := parseStatusPrefix(strings.TrimPrefix(trimmed, "- "))
		if status == TestRunStatusUnknown || remainder == "" {
			continue
		}
		container, details := parseContainerDetails(remainder)
		baseName, version := splitContainerName(container)
		if baseName == "" {
			continue
		}
		if details != "" {
			details = sanitizeDetails(details)
		}
		if details == "" && version != "" {
			details = fmt.Sprintf("Version %s", version)
		}
		results[baseName] = TestRunEntry{Container: baseName, ContainerVersion: version, Status: status, Details: details}
	}
	return results
}

func splitContainerBlocks(body string) [][]string {
	lines := strings.Split(body, "\n")
	var blocks [][]string
	var current []string

	flush := func() {
		if len(current) == 0 {
			return
		}
		block := make([]string, len(current))
		copy(block, current)
		blocks = append(blocks, block)
		current = current[:0]
	}

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if isContainerHeader(trimmed) {
			flush()
			current = append(current, line)
			continue
		}
		if len(current) > 0 {
			current = append(current, line)
		}
	}

	flush()
	return blocks
}

func isContainerHeader(line string) bool {
	if line == "" {
		return false
	}
	if strings.HasPrefix(line, "- ") {
		return false
	}
	head := []rune(line)
	if len(head) == 0 {
		return false
	}
	switch head[0] {
	case '✅', '❌', '⚠':
	default:
		return false
	}
	return strings.Contains(line, "**")
}

func parseContainerBlock(lines []string) TestRunEntry {
	var entry TestRunEntry
	if len(lines) == 0 {
		return entry
	}
	header := strings.TrimSpace(lines[0])
	status, remainder := parseStatusPrefix(header)
	if status == TestRunStatusUnknown {
		return entry
	}
	name, rest := extractBoldName(remainder)
	containerName := strings.TrimSpace(name)
	if containerName == "" {
		containerName = strings.TrimSpace(remainder)
	}
	containerName = sanitizeDetails(containerName)
	baseName, version := splitContainerName(containerName)
	entry.Container = baseName
	entry.ContainerVersion = version
	if entry.Container == "" {
		return TestRunEntry{}
	}
	entry.Status = status
	entry.Details = sanitizeDetails(strings.TrimPrefix(rest, "—"))
	if entry.Details == "" && entry.ContainerVersion != "" {
		entry.Details = fmt.Sprintf("Version %s", entry.ContainerVersion)
	}

	var info ContainerRunInfo
	var tests []ContainerTest
	var currentTest *ContainerTest
	var currentSection string
	var inCode bool
	var codeBuffer []string

	for i := 1; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "```") {
			if !inCode {
				inCode = true
				codeBuffer = codeBuffer[:0]
			} else {
				text := strings.Join(codeBuffer, "\n")
				if currentTest != nil {
					switch currentSection {
					case "stdout":
						currentTest.Stdout = text
					case "stderr":
						currentTest.Stderr = text
					default:
						if text != "" {
							currentTest.Details = append(currentTest.Details, text)
						}
					}
				}
				inCode = false
				codeBuffer = nil
			}
			continue
		}

		if inCode {
			codeBuffer = append(codeBuffer, strings.TrimSuffix(line, "\r"))
			continue
		}

		if trimmed == "" {
			if currentSection == "stdout" && currentTest != nil {
				if currentTest.Stdout != "" {
					currentTest.Stdout += "\n"
				}
			}
			if currentSection == "stderr" && currentTest != nil {
				if currentTest.Stderr != "" {
					currentTest.Stderr += "\n"
				}
			}
			continue
		}

		if strings.HasPrefix(trimmed, "- Container:") {
			info.ImagePath = extractBacktickValue(trimmed)
			continue
		}
		if strings.HasPrefix(trimmed, "- Runtime:") {
			info.Runtime = extractBacktickValue(trimmed)
			continue
		}
		if strings.HasPrefix(trimmed, "- Tests:") {
			entry.Details = sanitizeDetails(parseTestsLine(trimmed, &info))
			continue
		}
		if strings.HasPrefix(trimmed, "### ") {
			continue
		}

		indent := countLeadingSpaces(line)
		if indent == 0 && strings.HasPrefix(trimmed, "- ") {
			candidate := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
			status, remainder := parseStatusPrefix(candidate)
			if status != TestRunStatusUnknown {
				name, rest := extractBoldName(remainder)
				test := ContainerTest{
					Name:    sanitizeDetails(name),
					Status:  status,
					Summary: sanitizeDetails(strings.TrimSpace(strings.TrimPrefix(rest, "—"))),
				}
				tests = append(tests, test)
				currentTest = &tests[len(tests)-1]
				currentSection = ""
				continue
			}
		}

		if indent >= 2 && strings.HasPrefix(strings.TrimSpace(line), "- ") {
			if currentTest == nil {
				continue
			}
			sub := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- "))
			lower := strings.ToLower(sub)
			switch {
			case strings.HasPrefix(lower, "stdout"):
				currentSection = "stdout"
				inline := strings.TrimSpace(strings.TrimPrefix(sub, "stdout:"))
				if inline != "" {
					appendLine := sanitizeDetails(inline)
					if currentTest.Stdout != "" {
						currentTest.Stdout += "\n"
					}
					currentTest.Stdout += appendLine
				}
				continue
			case strings.HasPrefix(lower, "stderr"):
				currentSection = "stderr"
				inline := strings.TrimSpace(strings.TrimPrefix(sub, "stderr:"))
				if inline != "" {
					appendLine := sanitizeDetails(inline)
					if currentTest.Stderr != "" {
						currentTest.Stderr += "\n"
					}
					currentTest.Stderr += appendLine
				}
				continue
			case strings.HasPrefix(lower, "return code"):
				value := extractBacktickValue(sub)
				if value == "" {
					value = strings.TrimSpace(strings.TrimPrefix(sub, "Return code:"))
				}
				if n, ok := parseInt(value); ok {
					currentTest.ReturnCode = intPtr(n)
				}
				continue
			default:
				currentSection = ""
				currentTest.Details = append(currentTest.Details, sanitizeDetails(sub))
				continue
			}
		}

		if currentTest != nil {
			text := sanitizeDetails(trimmed)
			switch currentSection {
			case "stdout":
				if text != "" {
					if currentTest.Stdout != "" {
						currentTest.Stdout += "\n"
					}
					currentTest.Stdout += text
				}
			case "stderr":
				if text != "" {
					if currentTest.Stderr != "" {
						currentTest.Stderr += "\n"
					}
					currentTest.Stderr += text
				}
			default:
				if text != "" {
					currentTest.Details = append(currentTest.Details, text)
				}
			}
		}
	}

	entry.Info = info
	entry.Tests = tests
	if entry.Details == "" && info.TestsTotal > 0 {
		entry.Details = fmt.Sprintf("Tests: %d/%d passed (failed %d, skipped %d)", info.TestsPassed, info.TestsTotal, info.TestsFailed, info.TestsSkipped)
	}

	return entry
}

func parseTestsLine(line string, info *ContainerRunInfo) string {
	summary := strings.TrimSpace(strings.TrimPrefix(line, "- "))
	re := regexp.MustCompile(`(?i)tests:\s*(\d+)/(\d+)\s+passed\s+\(failed\s+(\d+),\s+skipped\s+(\d+)\)`)
	if matches := re.FindStringSubmatch(summary); len(matches) == 5 {
		if n, ok := parseInt(matches[1]); ok {
			info.TestsPassed = n
		}
		if n, ok := parseInt(matches[2]); ok {
			info.TestsTotal = n
		}
		if n, ok := parseInt(matches[3]); ok {
			info.TestsFailed = n
		}
		if n, ok := parseInt(matches[4]); ok {
			info.TestsSkipped = n
		}
	}
	return summary
}

func extractBoldName(text string) (string, string) {
	start := strings.Index(text, "**")
	if start == -1 {
		return "", text
	}
	remaining := text[start+2:]
	end := strings.Index(remaining, "**")
	if end == -1 {
		return "", text
	}
	name := remaining[:end]
	rest := strings.TrimSpace(remaining[end+2:])
	return name, rest
}

func extractBacktickValue(text string) string {
	start := strings.Index(text, "`")
	if start == -1 {
		return ""
	}
	remaining := text[start+1:]
	end := strings.Index(remaining, "`")
	if end == -1 {
		return ""
	}
	return strings.TrimSpace(remaining[:end])
}

func countLeadingSpaces(text string) int {
	count := 0
	for _, ch := range text {
		if ch == ' ' {
			count++
			continue
		}
		if ch == '\t' {
			count += 4
			continue
		}
		break
	}
	return count
}

func parseInt(text string) (int, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0, false
	}
	n, err := strconv.Atoi(text)
	if err != nil {
		return 0, false
	}
	return n, true
}

func intPtr(n int) *int {
	value := n
	return &value
}

func splitContainerName(raw string) (string, string) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", ""
	}
	idx := strings.Index(trimmed, ":")
	if idx == -1 {
		return trimmed, ""
	}
	name := strings.TrimSpace(trimmed[:idx])
	version := strings.TrimSpace(trimmed[idx+1:])
	if name == "" {
		return trimmed, ""
	}
	return name, version
}

func parseStatusPrefix(text string) (TestRunStatus, string) {
	statusPrefixes := []struct {
		prefix string
		status TestRunStatus
	}{
		{"✅", TestRunStatusPassed},
		{"❌", TestRunStatusFailed},
		{"⚠️", TestRunStatusSkipped},
	}

	for _, candidate := range statusPrefixes {
		if strings.HasPrefix(text, candidate.prefix) {
			remainder := strings.TrimSpace(text[len(candidate.prefix):])
			return candidate.status, remainder
		}
	}

	return TestRunStatusUnknown, ""
}

func parseContainerDetails(text string) (string, string) {
	start := strings.Index(text, "`")
	if start == -1 {
		return "", ""
	}
	remaining := text[start+1:]
	end := strings.Index(remaining, "`")
	if end == -1 {
		return "", ""
	}

	container := remaining[:end]
	details := strings.TrimSpace(remaining[end+1:])
	details = strings.TrimPrefix(details, "—")
	details = strings.TrimPrefix(details, "-")
	details = strings.TrimSpace(details)

	return container, details
}

func sanitizeDetails(text string) string {
	replacer := strings.NewReplacer(
		"—", "-",
		"–", "-",
	)
	return strings.TrimSpace(replacer.Replace(text))
}
