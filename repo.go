package main

import (
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/Fishwaldo/restic-nats-server/protocol"

	"github.com/pkg/errors"
)

func FSStat(repo string) (fs.FileInfo, error) {
	pwd, _ := os.Getwd()
	fs, err := os.Stat(path.Join(pwd, "repo", repo))
	if err != nil {
		return nil, errors.Wrap(err, "Stat on Repo Failed")
	}
	return fs, nil
}

func FSMkDir(dir string) (error) {
	pwd, _ := os.Getwd()
	return os.MkdirAll(path.Join(pwd, "repo", dir), 0700)
}

func FSSave(file string, data *[]byte, offset int64) (int, error) {
	pwd, _ := os.Getwd()
	filename := path.Join(pwd, "repo", file)
	tmpname := filepath.Base(filename) + "-tmp-"
	f, err := ioutil.TempFile(filepath.Dir(filename), tmpname)
	if err != nil {
		return 0, errors.Wrap(err, "Tempfile")
	}
	defer func(f *os.File) {
		if err != nil {
			_ = f.Close() // Double Close is harmless.
			// Remove after Rename is harmless: we embed the final name in the
			// temporary's name and no other goroutine will get the same data to
			// Save, so the temporary name should never be reused by another
			// goroutine.
			_ = os.Remove(f.Name())
		}
	}(f)

	len, err := f.Write(*data)
	if err != nil {
		return 0, errors.Wrap(err, "Write Failed")
	}
	if err := f.Close(); err != nil {
		return 0, errors.Wrap(err, "Close")
	}
	if err := os.Rename(f.Name(), filename); err != nil {
		return 0, errors.Wrap(err, "Rename")
	}
	return len, nil
}

func FSListFiles(dir string, recursive bool) ([]protocol.FileInfo, error) {
	pwd, _ := os.Getwd()
	finaldir := path.Join(pwd, "repo", dir)	

	d, err := os.Open(finaldir)
	if err != nil {
		return nil, err
	}
	defer d.Close()

	sub, err := d.Readdir(-1)
	if err != nil {
		return nil, err
	}

	var result []protocol.FileInfo
	for _, fi := range sub {
		if fi.IsDir() {
			/* dont' recursive more than 1 level */
			test, err := FSListFiles(path.Join(dir, fi.Name()), false)
			if err != nil {
				return nil, err
			}
			result = append(result, test...)
		}
		if fi.Name() != "" {
			result = append(result, protocol.FileInfo{Name: fi.Name(), Size: fi.Size()})
		}
	}
	return result, err
}

func FSLoadFile(filename string) (*os.File, error) {
	pwd, _ := os.Getwd()
	finalname := path.Join(pwd, "repo", filename)
	fs, err := os.Open(finalname)
	if err != nil {
		return nil, errors.Wrap(err, "LoadFile")
	}
	return fs, nil
}
func FSRemove(filename string) error {
	pwd, _ := os.Getwd()
	finalname := path.Join(pwd, "repo", filename)
	return os.Remove(finalname)
}