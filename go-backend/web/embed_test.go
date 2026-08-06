package web

import (
	"io/fs"
	"testing"
)

func TestEmbeddedPublicContainsIndex(t *testing.T) {
	data, err := fs.ReadFile(FS, "public/index.html")
	if err != nil {
		t.Fatalf("ReadFile(public/index.html) error = %v", err)
	}
	if len(data) == 0 {
		t.Fatal("public/index.html is empty")
	}
}
