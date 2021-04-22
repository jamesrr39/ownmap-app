package mockfs

import (
	"os"

	"github.com/jamesrr39/goutil/gofs"
	"github.com/spf13/afero"
)

type MockFs struct {
	CreateFunc    func(name string) (gofs.File, error)
	RemoveFunc    func(path string) error
	RemoveAllFunc func(path string) error
	StatFunc      func(path string) (os.FileInfo, error)
	LstatFunc     func(path string) (os.FileInfo, error)
	ReadFileFunc  func(path string) ([]byte, error)
	ReadDirFunc   func(dirname string) ([]os.FileInfo, error)
	MkdirFunc     func(path string, perm os.FileMode) error
	MkdirAllFunc  func(path string, perm os.FileMode) error
	OpenFunc      func(path string) (gofs.File, error)
	WriteFileFunc func(path string, data []byte, perm os.FileMode) error
	RenameFunc    func(old, new string) error
	OpenFileFunc  func(name string, flag int, perm os.FileMode) (gofs.File, error)
	ChmodFunc     func(name string, mode os.FileMode) error
	SymlinkFunc   func(oldname, newname string) error
	ReadlinkFunc  func(path string) (string, error)
}

var (
	_ gofs.Fs = MockFs{}
)

func NewMockFs() MockFs {
	mockFs := MockFs{}
	aferoFs := afero.NewMemMapFs()

	mockFs.CreateFunc = func(name string) (gofs.File, error) {
		return aferoFs.Create(name)
	}
	mockFs.RemoveFunc = aferoFs.Remove
	mockFs.RemoveAllFunc = aferoFs.RemoveAll
	mockFs.StatFunc = aferoFs.Stat
	mockFs.ReadFileFunc = func(path string) ([]byte, error) {
		return afero.ReadFile(aferoFs, path)
	}
	mockFs.ReadDirFunc = func(dirname string) ([]os.FileInfo, error) {
		return afero.ReadDir(aferoFs, dirname)
	}
	mockFs.MkdirFunc = aferoFs.Mkdir
	mockFs.MkdirAllFunc = aferoFs.MkdirAll
	mockFs.OpenFunc = func(name string) (gofs.File, error) {
		return aferoFs.Open(name)
	}
	mockFs.WriteFileFunc = func(path string, data []byte, perm os.FileMode) error {
		return afero.WriteFile(aferoFs, path, data, perm)
	}
	mockFs.RenameFunc = aferoFs.Rename
	mockFs.OpenFileFunc = func(name string, flag int, perm os.FileMode) (gofs.File, error) {
		return aferoFs.OpenFile(name, flag, perm)
	}
	mockFs.ChmodFunc = aferoFs.Chmod

	return mockFs
}

func (fs MockFs) Create(name string) (gofs.File, error) {
	return fs.CreateFunc(name)
}
func (fs MockFs) Remove(path string) error {
	return fs.RemoveFunc(path)
}
func (fs MockFs) RemoveAll(path string) error {
	return fs.RemoveAllFunc(path)
}
func (fs MockFs) Stat(path string) (os.FileInfo, error) {
	return fs.StatFunc(path)
}
func (fs MockFs) Lstat(path string) (os.FileInfo, error) {
	return fs.LstatFunc(path)
}
func (fs MockFs) ReadFile(path string) ([]byte, error) {
	return fs.ReadFileFunc(path)
}
func (fs MockFs) ReadDir(path string) ([]os.FileInfo, error) {
	return fs.ReadDirFunc(path)
}
func (fs MockFs) Mkdir(path string, perm os.FileMode) error {
	return fs.MkdirFunc(path, perm)
}

func (fs MockFs) MkdirAll(path string, perm os.FileMode) error {
	return fs.MkdirAllFunc(path, perm)
}

func (fs MockFs) Open(name string) (gofs.File, error) {
	return fs.OpenFunc(name)
}
func (fs MockFs) OpenFile(name string, flag int, perm os.FileMode) (gofs.File, error) {
	return fs.OpenFileFunc(name, flag, perm)
}

func (fs MockFs) WriteFile(path string, data []byte, perm os.FileMode) error {
	return fs.WriteFileFunc(path, data, perm)
}
func (fs MockFs) Rename(old, new string) error {
	return fs.RenameFunc(old, new)
}
func (fs MockFs) Symlink(oldName, newName string) error {
	return fs.SymlinkFunc(oldName, newName)
}
func (fs MockFs) Chmod(name string, mode os.FileMode) error {
	return fs.ChmodFunc(name, mode)
}
func (fs MockFs) Readlink(path string) (string, error) {
	return fs.ReadlinkFunc(path)
}
