package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

type setupInfo struct {
	GoBin        string `json:"go_bin"`
	OnPath       bool   `json:"on_path"`
	BinaryPath   string `json:"binary_path,omitempty"`
	BinaryExists bool   `json:"binary_exists"`
	Shell        string `json:"shell,omitempty"`
	Profile      string `json:"profile,omitempty"`
	ExportLine   string `json:"export_line"`
}

func runSetupSubcommand(args []string) int {
	info, err := detectSetupInfo()
	if err != nil {
		fmt.Fprintf(os.Stderr, "setup: %v\n", err)
		return 1
	}
	for _, arg := range args {
		switch arg {
		case "--check":
			writeJSON(info)
			if info.OnPath {
				return 0
			}
			return 1
		case "--apply":
			if err := appendPathExport(info.Profile, info.ExportLine); err != nil {
				fmt.Fprintf(os.Stderr, "setup: %v\n", err)
				return 1
			}
			fmt.Printf("updated %s\n", info.Profile)
			return 0
		case "-h", "--help":
			fmt.Fprintln(os.Stderr, "usage: minnow setup [--check|--apply]")
			return 0
		default:
			fmt.Fprintf(os.Stderr, "unknown setup flag: %s\n", arg)
			return 2
		}
	}

	program := tea.NewProgram(newSetupModel(info))
	if _, err := program.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "setup: %v\n", err)
		return 1
	}
	return 0
}

func writeJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func detectSetupInfo() (setupInfo, error) {
	goBin, err := goInstallBin()
	if err != nil {
		return setupInfo{}, err
	}
	info := setupInfo{
		GoBin:      goBin,
		OnPath:     pathContainsDir(os.Getenv("PATH"), goBin),
		BinaryPath: filepath.Join(goBin, "minnow"),
		Shell:      filepath.Base(os.Getenv("SHELL")),
		Profile:    defaultShellProfile(),
	}
	info.ExportLine = fmt.Sprintf("export PATH=\"$PATH:%s\"", goBin)
	if runtime.GOOS == "windows" {
		info.BinaryPath += ".exe"
	}
	if _, err := os.Stat(info.BinaryPath); err == nil {
		info.BinaryExists = true
	}
	return info, nil
}

func goInstallBin() (string, error) {
	if gobin := strings.TrimSpace(os.Getenv("GOBIN")); gobin != "" {
		return filepath.Clean(gobin), nil
	}
	gobin, err := goEnv("GOBIN")
	if err != nil {
		return "", err
	}
	if gobin != "" {
		return filepath.Clean(gobin), nil
	}
	gopath, err := goEnv("GOPATH")
	if err != nil {
		return "", err
	}
	if gopath == "" {
		return "", errors.New("go env GOPATH returned empty value")
	}
	return filepath.Join(gopath, "bin"), nil
}

func goEnv(key string) (string, error) {
	out, err := exec.Command("go", "env", key).Output()
	if err != nil {
		return "", fmt.Errorf("go env %s: %w", key, err)
	}
	return strings.TrimSpace(string(out)), nil
}

func pathContainsDir(pathValue, dir string) bool {
	dir = filepath.Clean(dir)
	for _, entry := range filepath.SplitList(pathValue) {
		if filepath.Clean(entry) == dir {
			return true
		}
	}
	return false
}

func defaultShellProfile() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	switch filepath.Base(os.Getenv("SHELL")) {
	case "zsh":
		return filepath.Join(home, ".zshrc")
	case "bash":
		if runtime.GOOS == "darwin" {
			return filepath.Join(home, ".bash_profile")
		}
		return filepath.Join(home, ".bashrc")
	case "fish":
		return filepath.Join(home, ".config", "fish", "config.fish")
	default:
		return filepath.Join(home, ".profile")
	}
}

func appendPathExport(profile, exportLine string) error {
	if profile == "" {
		return errors.New("could not determine shell profile")
	}
	data, err := os.ReadFile(profile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if strings.Contains(string(data), exportLine) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(profile), 0o755); err != nil {
		return err
	}
	content := string(data)
	if content != "" && !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	content += "\n# Added by minnow setup\n" + exportLine + "\n"
	return os.WriteFile(profile, []byte(content), 0o644)
}

type setupModel struct {
	info    setupInfo
	message string
	done    bool
}

func newSetupModel(info setupInfo) setupModel { return setupModel{info: info} }

func (m setupModel) Init() tea.Cmd { return nil }

func (m setupModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return m, tea.Quit
		case "a":
			if m.info.OnPath {
				m.message = "Go bin is already on PATH."
				return m, nil
			}
			if err := appendPathExport(m.info.Profile, m.info.ExportLine); err != nil {
				m.message = "Could not update profile: " + err.Error()
				return m, nil
			}
			m.done = true
			m.message = "Updated " + m.info.Profile + ". Restart your shell or run: source " + m.info.Profile
			return m, nil
		case "j":
			out, _ := json.MarshalIndent(m.info, "", "  ")
			m.message = string(out)
			return m, nil
		}
	}
	return m, nil
}

func (m setupModel) View() string {
	var b strings.Builder
	b.WriteString("Minnow setup\n\n")
	b.WriteString(fmt.Sprintf("Go install bin: %s\n", m.info.GoBin))
	b.WriteString(fmt.Sprintf("minnow binary: %s\n", yesNo(m.info.BinaryExists)))
	b.WriteString(fmt.Sprintf("Go bin on PATH: %s\n", yesNo(m.info.OnPath)))
	if m.info.Profile != "" {
		b.WriteString(fmt.Sprintf("Shell profile: %s\n", m.info.Profile))
	}
	b.WriteString("\n")
	if m.info.OnPath {
		b.WriteString("Your shell can already run `minnow`.\n")
	} else {
		b.WriteString("Add this line to your shell profile:\n\n")
		b.WriteString("  " + m.info.ExportLine + "\n\n")
		b.WriteString("Press a to append it automatically.\n")
	}
	b.WriteString("Press j for JSON diagnostics, q to quit.\n")
	if m.message != "" {
		b.WriteString("\n" + m.message + "\n")
	}
	return b.String()
}

func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}
