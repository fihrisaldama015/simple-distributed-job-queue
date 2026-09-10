// Package schema holds the GraphQL SDL for this service.
//
// The .graphql files are embedded into the binary with the standard library's
// //go:embed (Go 1.16+), so the server has no runtime dependency on the working
// directory and the build has no dependency on a code-generation tool. This replaces
// the previous go-bindata setup, which required an unmaintained external binary that
// is not installed on this machine — leaving the schema impossible to regenerate.
package schema

import (
	"bytes"
	"embed"
	"io/fs"
	"sort"
)

//go:embed *.graphql type/*.graphql
var schemaFS embed.FS

// String concatenates every .graphql file in this package into a single schema
// document. Files are concatenated in sorted path order; GraphQL type definitions are
// order-independent, so the only thing that matters is that the order is stable.
func String() string {
	names := make([]string, 0, 8)
	_ = fs.WalkDir(schemaFS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		names = append(names, path)
		return nil
	})
	sort.Strings(names)

	buf := bytes.Buffer{}
	for _, name := range names {
		b, err := schemaFS.ReadFile(name)
		if err != nil {
			continue
		}
		buf.Write(b)

		// Add a newline if the file does not end in a newline.
		if len(b) > 0 && b[len(b)-1] != '\n' {
			buf.WriteByte('\n')
		}
	}

	return buf.String()
}
