package snapshot

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

const UpdateSnapshotEnvVariableName = "UPDATE_SNAPSHOTS"

type SnapshotDataType string

const (
	SnapshotDataTypeText  SnapshotDataType = "TEXT"
	SnapshotDataTypeImage SnapshotDataType = "IMAGE"
)

type SnapshotType struct {
	DataType   SnapshotDataType
	Value      string
	OnBadMatch func(t *testing.T, expected string) error `json:"-"`
}

func AssertMatchesSnapshot(t *testing.T, snapshotName string, actualSnapshot *SnapshotType) {
	if snapshotName == "" {
		t.Error("no snapshot name provided")
	}

	_, filePath, _, ok := runtime.Caller(1)
	if !ok {
		panic("couldn't call runtime.Caller")
	}

	// encode snapshot name to escape "/" and other filesystem-illegal characters
	encodedSnapshotName := url.PathEscape(snapshotName)

	snapshotDirPath := filepath.Join(filepath.Dir(filePath), "__snapshots__", filepath.Base(filePath))
	snapshotFilePath := filepath.Join(snapshotDirPath, encodedSnapshotName+".snap.json")

	actioner := shouldUpdateViaEnv()

	file, err := actioner.OpenFile(t, snapshotFilePath)
	if err != nil {
		actioner.OnSnapshotFileOpenError(t, snapshotFilePath, actualSnapshot)
		return
	}
	defer file.Close()

	snapshot := new(SnapshotType)
	err = json.NewDecoder(file).Decode(&snapshot)
	if err != nil {
		actioner.OnExpectedSnapshotJsonDecodeFail(t, snapshotFilePath)
	}

	if snapshot.Value != actualSnapshot.Value {
		actioner.OnSnapshotNotMatched(t, file, snapshotFilePath, snapshot, actualSnapshot)
	}
}

func shouldUpdateViaEnv() snapshotActioner {
	shouldUpdateVal, ok := os.LookupEnv(UpdateSnapshotEnvVariableName)
	if ok && shouldUpdateVal == "1" {
		return updateSnapshotActioner{}
	}

	return noUpdateSnapshotActioner{}
}

type snapshotActioner interface {
	OpenFile(t *testing.T, filePath string) (*os.File, error)
	OnSnapshotFileOpenError(t *testing.T, snapshotFilePath string, actual *SnapshotType)
	OnSnapshotNotMatched(t *testing.T, file *os.File, snapshotFilePath string, expectedSnapshot, actualSnapshot *SnapshotType)
	OnExpectedSnapshotJsonDecodeFail(t *testing.T, snapshotFilePath string)
}

type noUpdateSnapshotActioner struct {
}

func (a noUpdateSnapshotActioner) OnExpectedSnapshotJsonDecodeFail(t *testing.T, snapshotFilePath string) {
	t.Errorf("couldn't open file at %q", snapshotFilePath)
}

func (a noUpdateSnapshotActioner) OpenFile(t *testing.T, filePath string) (*os.File, error) {
	return os.Open(filePath)
}

func (a noUpdateSnapshotActioner) OnSnapshotFileOpenError(t *testing.T, snapshotFilePath string, actual *SnapshotType) {
	t.Errorf("couldn't open snapshot file at %q", snapshotFilePath)
}

func (a noUpdateSnapshotActioner) OnSnapshotNotMatched(t *testing.T, file *os.File, snapshotFilePath string, expected, actual *SnapshotType) {
	t.Errorf("expected %q but got %q", expected.Value, actual.Value)

	if actual.OnBadMatch != nil {
		err := actual.OnBadMatch(t, expected.Value)
		if err != nil {
			t.Errorf("failed to execute OnBadMatch callback. Error: %q", err)
			return
		}
	}
}

type updateSnapshotActioner struct {
}

func (a updateSnapshotActioner) OnExpectedSnapshotJsonDecodeFail(t *testing.T, snapshotFilePath string) {
	// no-op
}

func (a updateSnapshotActioner) OpenFile(t *testing.T, filePath string) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (a updateSnapshotActioner) OnSnapshotFileOpenError(t *testing.T, snapshotFilePath string, actual *SnapshotType) {
	err := os.MkdirAll(filepath.Dir(snapshotFilePath), 0755)
	if err != nil {
		panic(err)
	}
	file, err := os.Create(snapshotFilePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	newSnapshot := actual
	b, err := json.MarshalIndent(newSnapshot, "", "\t")
	if err != nil {
		panic(err)
	}

	_, err = io.Copy(file, bytes.NewBuffer(b))
	if err != nil {
		panic(err)
	}

	log.Printf("Created new snapshot file file at %q\n", snapshotFilePath)
}

func (a updateSnapshotActioner) OnSnapshotNotMatched(t *testing.T, file *os.File, snapshotFilePath string, snapshotValue, actual *SnapshotType) {
	newSnapshot := actual
	b, err := json.MarshalIndent(newSnapshot, "", "\t")
	if err != nil {
		panic(err)
	}

	err = file.Truncate(0)
	if err != nil {
		panic(err)
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		panic(err)
	}

	_, err = io.Copy(file, bytes.NewBuffer(b))
	if err != nil {
		panic(err)
	}

	log.Printf("Updated snapshot file file at %q\n", snapshotFilePath)
}
