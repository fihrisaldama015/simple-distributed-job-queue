package schema

import (
	"strings"
	"testing"
)

func TestStringContainsEverySchemaFile(t *testing.T) {
	got := String()

	for _, fragment := range []string{
		"schema {",
		"type Query {",
		"type Mutation {",
		"type Job {",
		"type JobStatus {",
		"Enqueue(",
		"Jobs:",
		"JobStatus:",
	} {
		if !strings.Contains(got, fragment) {
			t.Errorf("schema is missing %q", fragment)
		}
	}
}

// go-bindata's output happened to be newline-terminated per file; the concatenation
// must stay that way or two type definitions would fuse into one invalid line.
func TestStringSeparatesFilesWithNewlines(t *testing.T) {
	if strings.Contains(String(), "}type ") {
		t.Fatal("schema files were concatenated without a separating newline")
	}
}
