package htmx

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadVariablesReadsTheJSONFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "variables.json")
	if err := os.WriteFile(path, []byte(`{"Job1":"A","Job2":"B","Job3":"C"}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	got, err := LoadVariables(path)
	if err != nil {
		t.Fatalf("LoadVariables() error = %v", err)
	}
	if got.Job1 != "A" || got.Job2 != "B" || got.Job3 != "C" {
		t.Fatalf("LoadVariables() = %+v, want A/B/C", got)
	}
}

func TestLoadVariablesFailsLoudlyOnMissingFile(t *testing.T) {
	if _, err := LoadVariables(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Fatal("LoadVariables() returned nil error for a missing file")
	}
}

// The shipped file is the source of truth for the form defaults — the README requires
// the form to match it, so a change there must not silently diverge from the UI.
func TestShippedVariablesFileMatchesTheDocumentedDefaults(t *testing.T) {
	got, err := LoadVariables("../../web/variables.json")
	if err != nil {
		t.Fatalf("LoadVariables() error = %v", err)
	}
	if got.Job1 != "JobTest1" || got.Job2 != "JobTest2" || got.Job3 != "JobTest3" {
		t.Fatalf("web/variables.json = %+v, want JobTest1/2/3", got)
	}
}
