package htmx

import (
	"encoding/json"
	"fmt"
	"os"
)

// Variables holds the dashboard form defaults. They come from web/variables.json —
// the same file GraphiQL is seeded with — so the two surfaces never disagree.
type Variables struct {
	Job1 string `json:"Job1"`
	Job2 string `json:"Job2"`
	Job3 string `json:"Job3"`
}

// LoadVariables reads the defaults file. It is called once at startup and returns an
// error rather than falling back to hard-coded values: a missing or malformed file is
// a deployment problem that should be loud, not papered over.
func LoadVariables(path string) (Variables, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Variables{}, fmt.Errorf("reading %s: %w", path, err)
	}

	var variables Variables
	if err := json.Unmarshal(raw, &variables); err != nil {
		return Variables{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	return variables, nil
}
