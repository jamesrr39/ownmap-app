package ownmapdal

import (
	"os"

	"github.com/jamesrr39/goutil/errorsx"
)

type PathsConfig struct {
	StylesDir       string
	DataDir         string
	RawDataFilesDir string
	TempDir         string
	TraceDir        string
}

func (pc *PathsConfig) EnsurePaths() errorsx.Error {
	for _, dirPath := range []string{pc.StylesDir, pc.DataDir, pc.RawDataFilesDir, pc.TempDir, pc.TraceDir} {
		err := os.MkdirAll(dirPath, 0755)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	return nil
}
