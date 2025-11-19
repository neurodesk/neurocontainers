package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type ExecutableType string

const (
	ExecutableTypeUnknown       ExecutableType = "unknown"
	ExecutableTypeScript        ExecutableType = "script"
	ExecutableTypeStaticBinary  ExecutableType = "static-binary"
	ExecutableTypeDynamicBinary ExecutableType = "dynamic-binary"
)

type ExecutableResult struct {
	Error string `json:",omitempty"`

	FullPath string `json:",omitempty"`

	ExecutableType ExecutableType `json:",omitempty"`

	Dependencies []ExecutableResult `json:",omitempty"`

	// Only added if captureOutput is true
	Output string `json:",omitempty"`
}

type TestResults struct {
	DeployBins  []string
	DeployPaths []string

	Executables map[string]ExecutableResult
}

type containerTester struct {
	captureOutput bool
}

func (ct *containerTester) isScript(fullPath string) (bool, error) {
	// Open the file
	f, err := os.Open(fullPath)
	if err != nil {
		return false, fmt.Errorf("opening file %q: %w", fullPath, err)
	}
	defer f.Close()

	// Read the first few bytes to check for a shebang (#!)
	buf := make([]byte, 2)
	n, err := f.Read(buf)
	if err != nil {
		return false, fmt.Errorf("reading file %q: %w", fullPath, err)
	}
	if n < 2 {
		return false, nil // File is too short to be a script
	}

	return buf[0] == '#' && buf[1] == '!', nil
}

func (ct *containerTester) testExecutable(name string, top bool) (ExecutableResult, error) {
	var ret ExecutableResult

	// Look up the full path of the executable
	full, err := exec.LookPath(name)
	if err != nil {
		return ret, fmt.Errorf("looking up path for executable %q: %w", name, err)
	}
	ret.FullPath = full

	// Determine if the executable is a script or binary
	if isScript, err := ct.isScript(full); err != nil {
		return ret, fmt.Errorf("checking if executable %q is a script: %w", full, err)
	} else if isScript {
		// It's a script. Determine the interpreter from the shebang line and use testExecutable.
		ret.ExecutableType = ExecutableTypeScript

		f, err := os.Open(full)
		if err != nil {
			return ret, fmt.Errorf("opening script %q: %w", full, err)
		}
		defer f.Close()

		reader := bufio.NewReader(f)
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return ret, fmt.Errorf("reading shebang for %q: %w", full, err)
		}
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "#!") {
			return ret, fmt.Errorf("script %q missing shebang", full)
		}

		shebang := strings.TrimSpace(strings.TrimPrefix(line, "#!"))
		if shebang == "" {
			return ret, fmt.Errorf("script %q has empty shebang", full)
		}

		interpreter := shebang
		var interpreterArgs string
		if idx := strings.IndexAny(shebang, " \t"); idx != -1 {
			interpreter = strings.TrimSpace(shebang[:idx])
			interpreterArgs = strings.TrimSpace(shebang[idx+1:])
		}

		dep, err := ct.testExecutable(interpreter, false)
		if err != nil {
			dep.Error = err.Error()
		}
		ret.Dependencies = append(ret.Dependencies, dep)

		if filepath.Base(interpreter) == "env" && interpreterArgs != "" {
			envArgs := strings.Fields(interpreterArgs)
			for _, arg := range envArgs {
				if arg == "" {
					continue
				}
				if arg == "-S" {
					// The next arguments are already split by Fields, so continue.
					continue
				}
				if strings.HasPrefix(arg, "-") {
					// Flags for env itself; skip.
					continue
				}
				if strings.Contains(arg, "=") {
					// Variable assignment consumed by env; skip.
					continue
				}

				dep, err := ct.testExecutable(arg, false)
				if err != nil {
					dep.Error = err.Error()
				}
				ret.Dependencies = append(ret.Dependencies, dep)
				break
			}
		}
	} else {
		// Assume an ELF binary. Use ldd to find dependencies and handle the case it's a static executable.
		cmd := exec.Command("ldd", full)
		output, lddErr := cmd.CombinedOutput()
		lddOut := string(output)
		if strings.Contains(lddOut, "statically linked") || strings.Contains(lddOut, "not a dynamic executable") {
			// Static binary - no shared library dependencies to record.
			lddErr = nil
			ret.ExecutableType = ExecutableTypeStaticBinary
		} else {
			ret.ExecutableType = ExecutableTypeDynamicBinary
		}
		if lddErr != nil && len(lddOut) == 0 {
			return ret, fmt.Errorf("running ldd on %q: %w", full, lddErr)
		}

		scanner := bufio.NewScanner(strings.NewReader(lddOut))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if strings.HasPrefix(line, "ldd:") {
				// Warning/error from ldd itself; surface as a dependency error for visibility.
				ret.Dependencies = append(ret.Dependencies, ExecutableResult{Error: line})
				continue
			}

			var dep ExecutableResult

			if strings.Contains(line, "=>") {
				parts := strings.SplitN(line, "=>", 2)
				left := strings.TrimSpace(parts[0])
				right := strings.TrimSpace(parts[1])
				if strings.Contains(right, "not found") {
					dep.FullPath = left
					dep.Error = fmt.Sprintf("dependency missing: %s", line)
				} else {
					fields := strings.Fields(right)
					if len(fields) > 0 && strings.HasPrefix(fields[0], "/") {
						dep.FullPath = fields[0]
						if _, err := os.Stat(fields[0]); err != nil {
							dep.Error = fmt.Sprintf("stat %q: %v", fields[0], err)
						}
					}
				}
			} else {
				fields := strings.Fields(line)
				if len(fields) > 0 && strings.HasPrefix(fields[0], "/") {
					dep.FullPath = fields[0]
					if _, err := os.Stat(fields[0]); err != nil {
						dep.Error = fmt.Sprintf("stat %q: %v", fields[0], err)
					}
				} else {
					continue
				}
			}

			ret.Dependencies = append(ret.Dependencies, dep)
		}
		if err := scanner.Err(); err != nil {
			return ret, fmt.Errorf("parsing ldd output for %q: %w", full, err)
		}
		if lddErr != nil {
			return ret, fmt.Errorf("running ldd on %q: %w", full, lddErr)
		}
	}

	// If captureOutput is true, run the executable and capture its output
	if ct.captureOutput && top {
		// Use a 5 second timeout to avoid hanging indefinitely
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, full)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return ret, fmt.Errorf("running executable %q: %w", full, err)
		}
		ret.Output = string(output)
	}

	return ret, nil
}

func (ct *containerTester) run() error {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	captureOutput := fs.Bool("capture-output", false, "Capture output of running each executable")
	deployBins := fs.String("deploy-bins", os.Getenv("DEPLOY_BINS"), "Colon-separated list of binaries to test")
	deployPaths := fs.String("deploy-paths", os.Getenv("DEPLOY_PATHS"), "Colon-separated list of paths to search for executables to test")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return fmt.Errorf("parsing flags: %w", err)
	}

	ct.captureOutput = *captureOutput

	deployBinsList := strings.Split(*deployBins, ":")
	deployPathsList := strings.Split(*deployPaths, ":")

	results := TestResults{
		DeployBins:  deployBinsList,
		DeployPaths: deployPathsList,
		Executables: make(map[string]ExecutableResult),
	}

	for _, bin := range deployBinsList {
		res, err := ct.testExecutable(bin, true)
		if err != nil {
			res.Error = err.Error()
		}

		results.Executables[bin] = res
	}

	for _, path := range deployPathsList {
		if path == "" {
			continue
		}

		files, err := os.ReadDir(path)
		if err != nil {
			return fmt.Errorf("reading deploy path %q: %w", path, err)
		}
		for _, file := range files {
			if file.IsDir() {
				continue
			}

			// check if the file is executable
			info, err := file.Info()
			if err != nil {
				return fmt.Errorf("getting info for file %q in path %q: %w", file.Name(), path, err)
			}
			if info.Mode()&0111 == 0 {
				continue
			}

			res, err := ct.testExecutable(filepath.Join(path, file.Name()), true)
			if err != nil {
				res.Error = err.Error()
			}

			results.Executables[file.Name()] = res
		}
	}

	if err := json.NewEncoder(os.Stdout).Encode(results); err != nil {
		return fmt.Errorf("encoding test results: %w", err)
	}

	return nil
}

func main() {
	tester := &containerTester{}
	if err := tester.run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
