package terraform

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

// testRepository is a git repository on disk with two commits on master: the first, tagged v1
// (an annotated tag), and the second.
type testRepository struct {
	url             string
	first, second   string
	firstMain       string
	secondMain      string
	variables       string
	ignoredInSubdir string
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func newTestRepository(t *testing.T) testRepository {
	t.Helper()
	// go-git clones a file:// URL by running git-upload-pack.
	if _, err := exec.LookPath("git-upload-pack"); err != nil {
		t.Skip("git-upload-pack is not installed")
	}
	dir := t.TempDir()
	repository, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := repository.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	signature := &object.Signature{Name: "test", Email: "test@example.com", When: time.Unix(1_700_000_000, 0)}
	commit := func(message string) string {
		t.Helper()
		if err := worktree.AddGlob("."); err != nil {
			t.Fatal(err)
		}
		hash, err := worktree.Commit(message, &git.CommitOptions{Author: signature})
		if err != nil {
			t.Fatal(err)
		}
		return hash.String()
	}
	repo := testRepository{
		url:             "file://" + filepath.ToSlash(dir),
		firstMain:       `resource "terraform_data" "first" {}`,
		secondMain:      `resource "terraform_data" "second" {}`,
		variables:       `variable "name" {}`,
		ignoredInSubdir: `# only in sub`,
	}
	writeFile(t, dir, "main.tf", repo.firstMain)
	writeFile(t, dir, "variables.tf", repo.variables)
	writeFile(t, dir, "README.md", "# not Terraform")
	writeFile(t, dir, "empty.tf", "")
	writeFile(t, dir, "sub/ignored.tf", repo.ignoredInSubdir)
	writeFile(t, dir, "docs/README.md", "# no Terraform here")
	repo.first = commit("first")
	if _, err := repository.CreateTag("v1", plumbing.NewHash(repo.first), &git.CreateTagOptions{Tagger: signature, Message: "v1"}); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dir, "main.tf", repo.secondMain)
	repo.second = commit("second")
	return repo
}

func fetch(t *testing.T, repo testRepository, ref dinovav1alpha1.TerraformGitReference, path string) (map[string]string, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return GoGit{}.Fetch(ctx, dinovav1alpha1.TerraformGitSource{URL: repo.url, Ref: ref, Path: path}, nil)
}

func TestGoGitReadsTheFilesOfTheDirectory(t *testing.T) {
	repo := newTestRepository(t)
	for name, test := range map[string]struct {
		ref    dinovav1alpha1.TerraformGitReference
		path   string
		files  map[string]string
		commit string
	}{
		"branch": {
			ref:    dinovav1alpha1.TerraformGitReference{Branch: "master"},
			files:  map[string]string{"main.tf": repo.secondMain, "variables.tf": repo.variables},
			commit: repo.second,
		},
		"annotated tag, peeled to its commit": {
			ref:    dinovav1alpha1.TerraformGitReference{Tag: "v1"},
			path:   ".",
			files:  map[string]string{"main.tf": repo.firstMain, "variables.tf": repo.variables},
			commit: repo.first,
		},
		"commit pinned on a branch": {
			ref:    dinovav1alpha1.TerraformGitReference{Branch: "master", Commit: repo.first},
			files:  map[string]string{"main.tf": repo.firstMain, "variables.tf": repo.variables},
			commit: repo.first,
		},
		"commit alone, on the default branch": {
			ref:    dinovav1alpha1.TerraformGitReference{Commit: repo.first},
			files:  map[string]string{"main.tf": repo.firstMain, "variables.tf": repo.variables},
			commit: repo.first,
		},
		"a subdirectory, without what is above it": {
			ref:    dinovav1alpha1.TerraformGitReference{Branch: "master"},
			path:   "sub",
			files:  map[string]string{"ignored.tf": repo.ignoredInSubdir},
			commit: repo.second,
		},
	} {
		t.Run(name, func(t *testing.T) {
			files, commit, err := fetch(t, repo, test.ref, test.path)
			if err != nil {
				t.Fatal(err)
			}
			if commit != test.commit {
				t.Errorf("commit %s, want %s", commit, test.commit)
			}
			if len(files) != len(test.files) {
				t.Errorf("files %v, want %v", sortedNames(files), sortedNames(test.files))
			}
			for name, content := range test.files {
				if files[name] != content {
					t.Errorf("file %s = %q, want %q", name, files[name], content)
				}
			}
		})
	}
}

func TestGoGitErrors(t *testing.T) {
	repo := newTestRepository(t)
	for name, test := range map[string]struct {
		ref    dinovav1alpha1.TerraformGitReference
		path   string
		reason string
		retry  bool
	}{
		"missing branch, maybe not pushed yet": {ref: dinovav1alpha1.TerraformGitReference{Branch: "nope"}, reason: "GitFetchFailed", retry: true},
		"missing directory":                    {ref: dinovav1alpha1.TerraformGitReference{Branch: "master"}, path: "nope", reason: "GitPathNotFound"},
		"directory without Terraform files":    {ref: dinovav1alpha1.TerraformGitReference{Tag: "v1"}, path: "docs", reason: "GitNoFiles"},
		"commit not on the branch": {
			ref: dinovav1alpha1.TerraformGitReference{Branch: "master", Commit: "0123456789012345678901234567890123456789"}, reason: "GitCommitNotFound",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := fetch(t, repo, test.ref, test.path)
			var sourceError *SourceError
			if !errors.As(err, &sourceError) {
				t.Fatalf("error %v, want a SourceError", err)
			}
			if sourceError.Reason != test.reason || sourceError.Retry != test.retry {
				t.Errorf("reason %s retry %v, want %s retry %v (%s)", sourceError.Reason, sourceError.Retry, test.reason, test.retry, sourceError.Message)
			}
		})
	}
}

func TestRedactURL(t *testing.T) {
	if got := RedactURL("https://user:secret@gitlab.com/group/repo"); got != "https://gitlab.com/group/repo" {
		t.Errorf("RedactURL = %q", got)
	}
}
