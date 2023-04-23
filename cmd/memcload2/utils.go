package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

// dotRename rename file to dotted filename
func dotRename(fp string) {
	dir, oldfname := filepath.Split(fp)
	if isDotted(oldfname) {
		return
	} // already dotted
	newfname := "." + oldfname
	if err := os.Rename(fp, filepath.Join(dir, newfname)); err != nil {
		log.Printf("Error rename file: %v\n", fp)
	}
}

// isDotted check if file is already dotted
func isDotted(s string) bool {
	return bytes.IndexRune([]byte(s), '.') == 0
}

// readGzipFile читает данные из файла и кладет в канал
// по завершению чтения переименовавает файл
func readGzipFile(fpList []string, readCh chan []byte) {
	defer close(readCh)

	for _, fname := range fpList {
		if _, fn := filepath.Split(fname); isDotted(fn) {
			continue // file is dotted, skip it
		}

		log.Printf("Processing: %s\n", fname)
		fraw, err := os.Open(fname)
		if err != nil {
			log.Fatal(fname, err.Error())
		}
		defer fraw.Close()

		zraw, err := gzip.NewReader(fraw)
		if err != nil {
			log.Fatal(fname, err.Error())
		}
		defer zraw.Close()

		content := bufio.NewScanner(zraw)
		contentBuffer := make([]byte, 0, bufio.MaxScanTokenSize)
		content.Buffer(contentBuffer, bufio.MaxScanTokenSize*50)

		for content.Scan() {
			readCh <- content.Bytes()
		}
		// rename file
		if !dry {
			dotRename(fname)
		}
	}
}

// cleanPath приводит относительные пути к полному (относительно текущей директории)
func cleanPath(path string) (string, error) {
	currentUser, _ := user.Current()
	homeDir := currentUser.HomeDir

	if path == "~" {
		path = homeDir
	} else if strings.HasPrefix(path, "~/") {
		path = filepath.Join(homeDir, path[2:])
	} else if !filepath.IsAbs(path) {
		currDir, err := os.Getwd()
		if err != nil {
			return "", err
		}
		path = filepath.Join(currDir, path)
	}
	return path, nil
}
