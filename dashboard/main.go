package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:embed templates/*.html
var tplFs embed.FS

var templates = template.Must(template.ParseFS(tplFs, "templates/*.html"))

type ContainerProgress struct {
	Name            string
	ReleaseVersions []string
	AppVersions     []string
	MatchedVersions []string
	MissingVersions []string
	ExtraVersions   []string
	Warnings        []string
	HasBuildYAML    bool
	BuildVersion    string
	BuildReleased   bool
	TestStatus      TestRunStatus
	TestDetails     string
	TestRun         *TestRunEntry
}

type indexData struct {
	Containers      []ContainerProgress
	Summary         dashboardSummary
	TestRun         *TestRunReport
	ContainerGroups []ContainerGroup
}

type dashboardSummary struct {
	TotalContainers      int
	ReleasedContainers   int
	UnreleasedContainers int
	MissingBuildYAML     int
	TestsPassed          int
	TestsFailed          int
	TestsSkipped         int
	TestsUnknown         int
}

type ContainerGroup struct {
	Key        string
	Title      string
	Containers []ContainerProgress
}

type DataSources struct {
	ReleasesDir  string
	RecipesDir   string
	AppsJSONPath string
}

func (ds *DataSources) normalize() error {
	if ds.ReleasesDir == "" {
		ds.ReleasesDir = firstExisting(
			filepath.Join("data", "neurocontainers", "releases"),
			"releases",
		)
		if ds.ReleasesDir == "" {
			return fmt.Errorf("releases directory not found; set --releases-dir")
		}
	} else if !dirExists(ds.ReleasesDir) {
		return fmt.Errorf("releases directory %q not found", ds.ReleasesDir)
	}

	if ds.RecipesDir == "" {
		ds.RecipesDir = firstExisting(
			filepath.Join("data", "neurocontainers", "recipes"),
			"recipes",
		)
		if ds.RecipesDir == "" {
			return fmt.Errorf("recipes directory not found; set --recipes-dir")
		}
	} else if !dirExists(ds.RecipesDir) {
		return fmt.Errorf("recipes directory %q not found", ds.RecipesDir)
	}

	if ds.AppsJSONPath == "" {
		ds.AppsJSONPath = firstExisting(
			filepath.Join("data", "neurocommand", "neurodesk", "apps.json"),
			"apps.json",
			filepath.Join("dashboard", "apps.json"),
		)
		if ds.AppsJSONPath == "" {
			return fmt.Errorf("apps.json not found; generate via tools/generate_apps_json.py or set --apps-json")
		}
	} else if !fileExists(ds.AppsJSONPath) {
		return fmt.Errorf("apps.json file %q not found", ds.AppsJSONPath)
	}

	return nil
}

func firstExisting(paths ...string) string {
	for _, p := range paths {
		if p == "" {
			continue
		}
		if dirExists(p) || fileExists(p) {
			return p
		}
	}
	return ""
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func loadContainerProgress(cfg DataSources) ([]ContainerProgress, error) {
	releasesDir := cfg.ReleasesDir
	appsPath := cfg.AppsJSONPath

	type appEntry struct {
		Apps map[string]json.RawMessage `json:"apps"`
	}

	appsBytes, err := os.ReadFile(appsPath)
	if err != nil {
		return nil, err
	}

	appsContainers := map[string]appEntry{}
	if err := json.Unmarshal(appsBytes, &appsContainers); err != nil {
		return nil, err
	}

	type containerData struct {
		releaseVersions map[string]struct{}
		appVersions     map[string]struct{}
		hasBuildYAML    bool
		hasBuildSH      bool
		buildVersion    string
		buildVersionErr string
	}

	containers := map[string]*containerData{}

	addContainer := func(name string) *containerData {
		c, ok := containers[name]
		if !ok {
			c = &containerData{
				releaseVersions: map[string]struct{}{},
				appVersions:     map[string]struct{}{},
			}
			containers[name] = c
		}
		return c
	}

	releaseDirEntries, err := os.ReadDir(releasesDir)
	if err != nil {
		return nil, err
	}

	for _, entry := range releaseDirEntries {
		if !entry.IsDir() {
			continue
		}

		containerName := entry.Name()
		releaseFiles, err := os.ReadDir(filepath.Join(releasesDir, containerName))
		if err != nil {
			return nil, err
		}

		container := addContainer(containerName)

		for _, f := range releaseFiles {
			if f.IsDir() {
				continue
			}
			name := f.Name()
			if !strings.HasSuffix(name, ".json") {
				continue
			}
			version := strings.TrimSuffix(name, ".json")
			if version == "" {
				continue
			}
			container.releaseVersions[version] = struct{}{}
		}
	}

	recipesDir := cfg.RecipesDir
	recipeEntries, err := os.ReadDir(recipesDir)
	if err != nil {
		return nil, err
	}

	for _, entry := range recipeEntries {
		if !entry.IsDir() {
			continue
		}

		containerName := entry.Name()
		container := addContainer(containerName)
		recipePath := filepath.Join(recipesDir, containerName)

		buildYAMLPath := filepath.Join(recipePath, "build.yaml")
		if _, err := os.Stat(buildYAMLPath); err == nil {
			container.hasBuildYAML = true
			version, parseErr := parseBuildYAMLVersion(buildYAMLPath)
			if parseErr != nil {
				container.buildVersionErr = parseErr.Error()
			} else {
				container.buildVersion = version
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}

		if _, err := os.Stat(filepath.Join(recipePath, "build.sh")); err == nil {
			container.hasBuildSH = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}

	for containerName, entry := range appsContainers {
		container := addContainer(containerName)
		for appName := range entry.Apps {
			version := parseVersionFromAppKey(appName)
			if version == "" {
				continue
			}
			container.appVersions[version] = struct{}{}
		}
	}

	if len(containers) == 0 {
		return nil, errors.New("no containers found")
	}

	containerNames := make([]string, 0, len(containers))
	for name := range containers {
		containerNames = append(containerNames, name)
	}
	sort.Strings(containerNames)

	progress := make([]ContainerProgress, 0, len(containers))
	for _, name := range containerNames {
		data := containers[name]

		releaseVersions := setToSortedSlice(data.releaseVersions)
		appVersions := setToSortedSlice(data.appVersions)
		var warnings []string
		if !data.hasBuildYAML && data.hasBuildSH {
			warnings = append(warnings, "Found build.sh but missing build.yaml")
		}
		if data.hasBuildYAML && len(data.releaseVersions) == 0 {
			warnings = append(warnings, "build.yaml present but no releases found")
		}
		var buildReleased bool
		if data.buildVersionErr != "" {
			warnings = append(warnings, fmt.Sprintf("Failed to parse build.yaml: %s", data.buildVersionErr))
		} else if data.buildVersion != "" {
			_, buildReleased = data.releaseVersions[data.buildVersion]
			if !buildReleased {
				warnings = append(warnings, fmt.Sprintf("build.yaml version %s has no release", data.buildVersion))
			}
		}

		matched := sortedIntersection(data.releaseVersions, data.appVersions)
		missing := sortedDifference(data.releaseVersions, data.appVersions)
		extra := sortedDifference(data.appVersions, data.releaseVersions)

		progress = append(progress, ContainerProgress{
			Name:            name,
			ReleaseVersions: releaseVersions,
			AppVersions:     appVersions,
			MatchedVersions: matched,
			MissingVersions: missing,
			ExtraVersions:   extra,
			Warnings:        warnings,
			HasBuildYAML:    data.hasBuildYAML,
			BuildVersion:    data.buildVersion,
			BuildReleased:   buildReleased,
			TestStatus:      TestRunStatusUnknown,
		})
	}

	return progress, nil
}

func setToSortedSlice(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	slice := make([]string, 0, len(set))
	for v := range set {
		slice = append(slice, v)
	}
	sort.Strings(slice)
	return slice
}

func sortedIntersection(a map[string]struct{}, b map[string]struct{}) []string {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	var result []string
	for value := range a {
		if _, ok := b[value]; ok {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func sortedDifference(source, other map[string]struct{}) []string {
	if len(source) == 0 {
		return nil
	}
	var diff []string
	for value := range source {
		if _, ok := other[value]; ok {
			continue
		}
		diff = append(diff, value)
	}
	if len(diff) == 0 {
		return nil
	}
	sort.Strings(diff)
	return diff
}

func groupContainers(containers []ContainerProgress) []ContainerGroup {
	if len(containers) == 0 {
		return nil
	}

	orderedGroups := []struct {
		key   string
		title string
	}{
		{"released_passed", "Released · Tests Passed"},
		{"released_failed", "Released · Tests Failed"},
		{"released_skipped", "Released · Tests Skipped"},
		{"released_unknown", "Released · Tests Unknown"},
		{"unreleased", "Unreleased"},
	}

	classify := func(c ContainerProgress) (string, string) {
		if len(c.ReleaseVersions) == 0 {
			return "unreleased", "Unreleased"
		}
		switch c.TestStatus {
		case TestRunStatusPassed:
			return "released_passed", "Released · Tests Passed"
		case TestRunStatusFailed:
			return "released_failed", "Released · Tests Failed"
		case TestRunStatusSkipped:
			return "released_skipped", "Released · Tests Skipped"
		default:
			return "released_unknown", "Released · Tests Unknown"
		}
	}

	groups := make(map[string]*ContainerGroup)
	for _, c := range containers {
		key, title := classify(c)
		grp, ok := groups[key]
		if !ok {
			grp = &ContainerGroup{Key: key, Title: title}
			groups[key] = grp
		}
		grp.Containers = append(grp.Containers, c)
	}

	var result []ContainerGroup
	for _, def := range orderedGroups {
		grp, ok := groups[def.key]
		if !ok || len(grp.Containers) == 0 {
			continue
		}
		if grp.Title == "" {
			grp.Title = def.title
		}
		result = append(result, *grp)
		delete(groups, def.key)
	}

	if len(groups) > 0 {
		extra := make([]ContainerGroup, 0, len(groups))
		for _, grp := range groups {
			extra = append(extra, *grp)
		}
		sort.Slice(extra, func(i, j int) bool {
			return extra[i].Title < extra[j].Title
		})
		result = append(result, extra...)
	}

	return result
}

func parseBuildYAMLVersion(path string) (string, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var doc struct {
		Version string `yaml:"version"`
	}
	if err := yaml.Unmarshal(contents, &doc); err != nil {
		return "", err
	}
	return strings.TrimSpace(doc.Version), nil
}

func parseVersionFromAppKey(key string) string {
	fields := strings.Fields(strings.TrimSpace(key))
	if len(fields) == 0 {
		return ""
	}
	return fields[len(fields)-1]
}

type BuildOptions struct {
	SkipGitHub  bool
	DataSources DataSources
}

func buildDashboardData(ctx context.Context, opts BuildOptions) (indexData, error) {
	sources := opts.DataSources
	if err := sources.normalize(); err != nil {
		return indexData{}, err
	}

	progress, err := loadContainerProgress(sources)
	if err != nil {
		return indexData{}, err
	}

	var testRun *TestRunReport
	if !opts.SkipGitHub {
		var loadErr error
		testRun, loadErr = loadLatestTestRunReport(ctx)
		if loadErr != nil {
			slog.Warn("load github test report", "error", loadErr)
		}
	}

	if testRun != nil {
		for i := range progress {
			name := progress[i].Name
			entry, ok := testRun.Entries[name]
			if !ok {
				for _, candidate := range testRun.Tested {
					if candidate.Container == name {
						entry = candidate
						ok = true
						break
					}
				}
			}
			if !ok {
				for _, candidate := range testRun.Failures {
					if candidate.Container == name {
						entry = candidate
						ok = true
						break
					}
				}
			}
			if !ok {
				for _, candidate := range testRun.Skipped {
					if candidate.Container == name {
						entry = candidate
						ok = true
						break
					}
				}
			}
			if !ok {
				continue
			}
			entryCopy := entry
			if entryCopy.Status == "" {
				entryCopy.Status = TestRunStatusUnknown
			}
			progress[i].TestStatus = entryCopy.Status
			progress[i].TestDetails = entryCopy.Details
			progress[i].TestRun = &entryCopy
		}
	}

	summary := dashboardSummary{TotalContainers: len(progress)}
	for _, container := range progress {
		if len(container.ReleaseVersions) > 0 {
			summary.ReleasedContainers++
		} else {
			summary.UnreleasedContainers++
		}
		if !container.HasBuildYAML {
			summary.MissingBuildYAML++
		}
		switch container.TestStatus {
		case TestRunStatusPassed:
			summary.TestsPassed++
		case TestRunStatusFailed:
			summary.TestsFailed++
		case TestRunStatusSkipped:
			summary.TestsSkipped++
		default:
			summary.TestsUnknown++
		}
	}

	groups := groupContainers(progress)

	data := indexData{
		Containers:      progress,
		Summary:         summary,
		TestRun:         testRun,
		ContainerGroups: groups,
	}

	return data, nil
}

func renderDashboard(ctx context.Context, outDir string, opts BuildOptions) (string, error) {
	data, err := buildDashboardData(ctx, opts)
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := templates.ExecuteTemplate(&buf, "index.html", data); err != nil {
		return "", err
	}

	dest := filepath.Join(outDir, "index.html")
	if err := os.WriteFile(dest, buf.Bytes(), 0o644); err != nil {
		return "", err
	}

	slog.Info("dashboard generated", "path", dest, "containers", len(data.Containers))
	return dest, nil
}

func main() {
	if err := loadEnvFile(".env"); err != nil {
		slog.Warn("load env file", "error", err)
	}

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	outDir := fs.String("out", "dist", "directory to write the generated site")
	skipGitHub := fs.Bool("skip-github", false, "skip fetching the latest GitHub test run report")
	releasesDir := fs.String("releases-dir", "", "path to container release metadata (defaults to releases/)")
	recipesDir := fs.String("recipes-dir", "", "path to container recipes (defaults to recipes/)")
	appsJSON := fs.String("apps-json", "", "path to apps.json for comparing published apps")

	if err := fs.Parse(os.Args[1:]); err != nil {
		slog.Error("parse flags", "error", err)
		os.Exit(2)
	}

	ctx := context.Background()

	opts := BuildOptions{
		SkipGitHub: *skipGitHub,
		DataSources: DataSources{
			ReleasesDir:  *releasesDir,
			RecipesDir:   *recipesDir,
			AppsJSONPath: *appsJSON,
		},
	}

	if _, err := renderDashboard(ctx, *outDir, opts); err != nil {
		slog.Error("generate dashboard", "error", err)
		os.Exit(1)
	}
}
