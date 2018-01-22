package main

import (
	"os"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/jessevdk/go-flags"
	"github.com/btcsuite/goleveldb/leveldb/opt"
)

type config struct {
	Stats     bool   `short:"u" long:"stat" description:"Display stats of source storage"`
	SourceDir string `short:"s" long:"sourcedir" description:"Path to source storage" required:"true"`
	MaxSize   int64  `short:"m" long:"maxsize" description:"Max size of source storage"`
	TargetDir string `short:"t" long:"targetdir" description:"Path to target storage"`
	BatchSize int    `short:"b" long:"batchsize" description:"Batch size of moving" default:"1000"`
}

func main() {
	cfg := config{}
	parser := flags.NewParser(&cfg, flags.Default)

	appName := filepath.Base(os.Args[0])
	appName = strings.TrimSuffix(appName, filepath.Ext(appName))
	usageMessage := fmt.Sprintf("Use %s -h to show usage", appName)

	_, err := parser.Parse()
	if err != nil {
		if e, ok := err.(*flags.Error); !ok || e.Type != flags.ErrHelp {
			fmt.Fprintln(os.Stderr, usageMessage)
		}
		return
	}

	if cfg.Stats {
		size, err := getSize(cfg.SourceDir)
		if err != nil {
			fmt.Printf("Error: %+v\n", err)
			return
		}
		fmt.Printf("Size %d\n", size)
		return
	}

	if cfg.MaxSize < 1 {
		fmt.Print("MaxSize should be more then zero")
		return
	}

	fmt.Printf("Config: %+v\n", cfg)

	sdb, err := leveldb.OpenFile(cfg.SourceDir, nil)
	if err != nil {
		fmt.Printf("Error: %+v\n", err)
		return
	}
	defer sdb.Close()

	tdb, err := leveldb.OpenFile(cfg.TargetDir, nil)
	if err != nil {
		fmt.Printf("Error: %+v\n", err)
		return
	}
	defer tdb.Close()

	iterator := sdb.NewIterator(nil, nil)
	batchIndex := 0

	sourceBatch := &leveldb.Batch{}
	targetBatch := &leveldb.Batch{}

	for iterator.Next() {
		if batchIndex > cfg.BatchSize {
			if err := sdb.Write(sourceBatch, &opt.WriteOptions{Sync: true}); err != nil {
				fmt.Printf("Error: %+v\n", err)
				break
			}

			if err := tdb.Write(targetBatch, &opt.WriteOptions{Sync: true}); err != nil {
				fmt.Printf("Error: %+v\n", err)
				break
			}

			size, err := getSize(cfg.SourceDir)
			if err != nil {
				fmt.Printf("Error: %+v\n", err)
				break
			}

			fmt.Printf("Size: %d, target size: %d\n", size, cfg.MaxSize)

			if size <= cfg.MaxSize {
				break
			}
		}

		sourceBatch.Put(iterator.Key(), iterator.Value())
		targetBatch.Delete(iterator.Key())

		batchIndex++
	}
	iterator.Release()

	fmt.Println("DONE")
}

func getSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}

		return err
	})
	if err != nil {
		return 0, err
	}

	return size, nil
}
