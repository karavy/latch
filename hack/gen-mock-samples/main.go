// Command gen-mock-samples writes the mock copies of the samples (see internal/mocksamples):
// config/samples/p2p-workflow/mock and config/samples/terraform-workflow/mock. Run it from the
// repository root after changing a real sample.
package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/karavy/latch/internal/mocksamples"
)

func main() {
	for _, sample := range []struct {
		dir      string
		generate func(string) ([]mocksamples.File, error)
	}{
		{"config/samples/p2p-workflow", mocksamples.P2P},
		{"config/samples/terraform-workflow", mocksamples.Terraform},
	} {
		files, err := sample.generate(sample.dir)
		if err != nil {
			log.Fatal(err)
		}
		out := filepath.Join(sample.dir, "mock")
		if err := os.MkdirAll(out, 0o755); err != nil {
			log.Fatal(err)
		}
		for _, file := range files {
			if err := os.WriteFile(filepath.Join(out, file.Name), file.Content, 0o644); err != nil {
				log.Fatal(err)
			}
		}
		log.Printf("Wrote %d files in %s", len(files), out)
	}
}
