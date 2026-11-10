package terraform

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/storage/memory"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

const (
	// GitRefreshAnnotation, set to a new value on a TerraformTemplate, makes the operator read
	// its git source again: the way to pick up a new commit of a branch without changing the
	// spec.
	GitRefreshAnnotation = "dinova.one/git-refresh"
	// MaxGitBytes is the most the files read from git may weigh together. The files of a
	// revision end up in a ControllerRevision and in the ConfigMap of each Job, both limited
	// to about 1 MiB.
	MaxGitBytes = 512 << 10
	// GitUsernameKey and GitPasswordKey are the keys of the Secret of spec.git.secretRef.
	GitUsernameKey = "username"
	GitPasswordKey = "password"
)

// GitAuth are the HTTP basic credentials of a git repository.
type GitAuth struct {
	Username, Password string
}

// GitFetcher reads the Terraform files of one directory of a git repository, and the commit
// they were read from. Problems with the content (no files, files too large, a commit that
// is not there) are a *SourceError; problems that a later attempt may fix (network,
// credentials, a branch not pushed yet) are a *SourceError with Retry.
type GitFetcher interface {
	Fetch(ctx context.Context, source dinovav1alpha1.TerraformGitSource, auth *GitAuth) (files map[string]string, commit string, err error)
}

// GoGit is the GitFetcher of the operator: it clones in memory with go-git, without a git
// binary and without writing to disk. A branch or a tag is cloned with depth 1; a pinned
// commit needs the history of its branch.
type GoGit struct{}

// Fetch clones the repository and reads the files of source.Path.
func (GoGit) Fetch(ctx context.Context, source dinovav1alpha1.TerraformGitSource, auth *GitAuth) (map[string]string, string, error) {
	options := &git.CloneOptions{URL: source.URL, SingleBranch: true, Tags: git.NoTags}
	if auth != nil {
		options.Auth = &githttp.BasicAuth{Username: auth.Username, Password: auth.Password}
	}
	switch {
	case source.Ref.Branch != "":
		options.ReferenceName = plumbing.NewBranchReferenceName(source.Ref.Branch)
	case source.Ref.Tag != "":
		options.ReferenceName = plumbing.NewTagReferenceName(source.Ref.Tag)
	}
	if source.Ref.Commit == "" {
		options.Depth = 1
	}
	filesystem := memfs.New()
	repository, err := git.CloneContext(ctx, memory.NewStorage(), filesystem, options)
	if err != nil {
		return nil, "", &SourceError{Reason: "GitFetchFailed", Message: fmt.Sprintf("cannot read %s at %s: %v", RedactURL(source.URL), GitRef(source.Ref), err), Retry: true}
	}
	var commit plumbing.Hash
	if source.Ref.Commit != "" {
		commit = plumbing.NewHash(source.Ref.Commit)
		if _, err := repository.CommitObject(commit); err != nil {
			return nil, "", &SourceError{Reason: "GitCommitNotFound", Message: fmt.Sprintf("commit %s is not in %s at %s", source.Ref.Commit, RedactURL(source.URL), GitRef(source.Ref))}
		}
		worktree, err := repository.Worktree()
		if err == nil {
			err = worktree.Checkout(&git.CheckoutOptions{Hash: commit, Force: true})
		}
		if err != nil {
			return nil, "", fmt.Errorf("checking out commit %s: %w", source.Ref.Commit, err)
		}
	} else {
		head, err := repository.Head()
		if err != nil {
			return nil, "", fmt.Errorf("reading the commit of %s: %w", GitRef(source.Ref), err)
		}
		commit = head.Hash()
	}
	files, err := readGitDirectory(filesystem, source.Path, commit.String())
	if err != nil {
		return nil, "", err
	}
	return files, commit.String(), nil
}

// readGitDirectory reads the Terraform files directly in dir: regular files only (symbolic
// links are skipped), with a name valid for spec.files, not empty.
func readGitDirectory(filesystem billy.Filesystem, dir, commit string) (map[string]string, error) {
	if dir == "" {
		dir = "."
	}
	entries, err := filesystem.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, &SourceError{Reason: "GitPathNotFound", Message: fmt.Sprintf("directory %q does not exist at commit %s", dir, commit)}
	}
	if err != nil {
		return nil, fmt.Errorf("reading directory %q: %w", dir, err)
	}
	files := map[string]string{}
	total := 0
	for _, entry := range entries {
		if !entry.Mode().IsRegular() || !fileName.MatchString(entry.Name()) || entry.Size() == 0 {
			continue
		}
		total += int(entry.Size())
		if total > MaxGitBytes {
			return nil, &SourceError{Reason: "GitFilesTooLarge", Message: fmt.Sprintf("the Terraform files in %q at commit %s weigh more than %d bytes", dir, commit, MaxGitBytes)}
		}
		content, err := readGitFile(filesystem, path.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		files[entry.Name()] = content
	}
	if len(files) == 0 {
		return nil, &SourceError{Reason: "GitNoFiles", Message: fmt.Sprintf("directory %q has no .tf or .tf.tmpl file at commit %s", dir, commit)}
	}
	return files, nil
}

func readGitFile(filesystem billy.Filesystem, name string) (string, error) {
	file, err := filesystem.Open(name)
	if err != nil {
		return "", fmt.Errorf("opening %q: %w", name, err)
	}
	defer func() { _ = file.Close() }()
	content, err := io.ReadAll(io.LimitReader(file, MaxGitBytes+1))
	if err != nil {
		return "", fmt.Errorf("reading %q: %w", name, err)
	}
	return string(content), nil
}

// GitRef describes a reference as the status records it: "branch/<name>", "tag/<name>" or
// "commit" for a commit alone.
func GitRef(ref dinovav1alpha1.TerraformGitReference) string {
	switch {
	case ref.Branch != "":
		return "branch/" + ref.Branch
	case ref.Tag != "":
		return "tag/" + ref.Tag
	}
	return "commit"
}

// GitPath is the directory as the status records it: "." for the root.
func GitPath(dir string) string {
	if dir == "" {
		return "."
	}
	return dir
}

// RedactURL drops credentials written into a URL, so that messages and logs never show them.
func RedactURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "<invalid URL>"
	}
	parsed.User = nil
	return parsed.String()
}

// sortedNames returns the names of files in order.
func sortedNames(files map[string]string) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// trimCommit shortens a commit for messages.
func trimCommit(commit string) string {
	return strings.TrimSpace(commit)[:min(12, len(commit))]
}
