package main

import (
	"os"

	"github.com/google/zoekt"
	"github.com/google/zoekt/build"
)

// mergeMeta updates the .meta files for the shards on disk for o.
//
// This process is best effort. If anything fails we return on the first
// failure. This means you might have an inconsistent state on disk if an
// error is returned. It is recommended to fallback to re-indexing in that
// case.
func mergeMeta(o *build.Options) error {
	todo := map[string]string{}
	for i := 0; ; i++ {
		fn := o.ShardName(i)

		repo, _, err := zoekt.ReadMetadataPath(fn)
		if os.IsNotExist(err) {
			break
		} else if err != nil {
			return err
		}

		if updated, err := repo.MergeMutable(&o.RepositoryDescription); err != nil {
			return err
		} else if !updated {
			// This shouldn't happen, but ignore it if it does. We may be working on
			// an interrupted shard. This helps us converge to something correct.
			continue
		}

		dst := fn + ".meta"
		tmp, err := build.JsonMarshalTmpFile(repo, dst)
		if err != nil {
			return err
		}

		todo[tmp] = dst

		// if we fail to rename, this defer will attempt to remove the tmp file.
		defer os.Remove(tmp)
	}

	// best effort once we get here. Rename everything. Return error of last
	// failure.
	var renameErr error
	for tmp, dst := range todo {
		if err := os.Rename(tmp, dst); err != nil {
			renameErr = err
		}
	}

	return renameErr
}
