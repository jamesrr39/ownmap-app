package gofs

import (
	"path/filepath"
	"sort"
	"strings"

	"github.com/jamesrr39/goutil/excludesmatcher"
)

type WalkOptions struct {
	ExcludesMatcher excludesmatcher.Matcher
}

func Walk(fs Fs, path string, walkFunc filepath.WalkFunc, options WalkOptions) error {
	return walk(fs, path, path, walkFunc, options)
}

func walk(fs Fs, basePath, path string, walkFunc filepath.WalkFunc, options WalkOptions) error {
	relativePath := strings.TrimPrefix(strings.TrimPrefix(path, basePath), string(filepath.Separator))
	isExcluded := options.ExcludesMatcher != nil && options.ExcludesMatcher.Matches(relativePath)
	if isExcluded {
		return nil
	}

	fileInfo, err := fs.Lstat(path)
	if err != nil {
		return err
	}

	err = walkFunc(path, fileInfo, nil)
	if err != nil {
		return err
	}

	if fileInfo.IsDir() {
		dirEntryInfos, err := fs.ReadDir(path)
		if err != nil {
			return err
		}

		sort.Slice(dirEntryInfos, func(i int, j int) bool {
			return dirEntryInfos[i].Name() > dirEntryInfos[j].Name()
		})

		for _, dirEntryInfo := range dirEntryInfos {
			err = walk(fs, basePath, filepath.Join(path, dirEntryInfo.Name()), walkFunc, options)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
