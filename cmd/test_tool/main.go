package main

import (
	"fmt"
	"log"
	"os"

	"github.com/iho/kvs"

	"github.com/iho/etf"
	"github.com/linxGnu/grocksdb"
)

func main() {
	// Set up database options
	dbPath := "testdb"
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	// Remove any existing database for clean testing
	if err := os.RemoveAll(dbPath); err != nil {
		log.Fatalf("Failed to remove existing db: %v", err)
	}

	// Open the database
	db, err := grocksdb.OpenDb(opts, dbPath)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Initialize RocksDB wrapper
	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	rdb := kvs.NewRocksDB(
		db,
		ro,
		wo,
	)

	// Test Append operation
	fmt.Println("Testing Append...")
	rec := etf.Tuple{etf.Atom("test_record"), 1}
	feed := etf.Atom("feed1")
	_, err = rdb.Append(rec, feed)
	if err != nil {
		log.Fatalf("Append failed: %v", err)
	}
	fmt.Println("Append successful.")

	// Test Take operation
	fmt.Println("\nTesting Take...")
	startKey := rec
	num := 10
	results, err := rdb.Take(startKey, num)
	if err != nil {
		log.Fatalf("Take failed: %v", err)
	}
	fmt.Printf("Take results: %v\n", results)

	// Test Top operation
	fmt.Println("\nTesting Top...")
	topKey, topValue, err := rdb.Top()
	if err != nil {
		log.Fatalf("Top failed: %v", err)
	}
	fmt.Printf("Top key: %v, value: %v\n", topKey, topValue)

	// Test Bot operation
	fmt.Println("\nTesting Bot...")
	botKey, botValue, err := rdb.Bot()
	if err != nil {
		log.Fatalf("Bot failed: %v", err)
	}
	fmt.Printf("Bot key: %v, value: %v\n", botKey, botValue)

	// Test Next operation
	fmt.Println("\nTesting Next...")
	nextKey, nextValue, err := rdb.Next(startKey)
	if err != nil {
		fmt.Printf("Next returned error: %v\n", err)
	} else {
		fmt.Printf("Next key: %v, value: %v\n", nextKey, nextValue)
	}

	// Test Prev operation
	fmt.Println("\nTesting Prev...")
	prevKey, prevValue, err := rdb.Prev(startKey)
	if err != nil {
		fmt.Printf("Prev returned error: %v\n", err)
	} else {
		fmt.Printf("Prev key: %v, value: %v\n", prevKey, prevValue)
	}

	// Test LoadReader and SaveReader operations
	fmt.Println("\nTesting SaveReader and LoadReader...")
	readerID := etf.Atom("reader1")
	readerData := etf.Tuple{etf.Atom("state"), 42}
	err = rdb.SaveReader(readerID, readerData)
	if err != nil {
		log.Fatalf("SaveReader failed: %v", err)
	}
	loadedReader, err := rdb.LoadReader(readerID)
	if err != nil {
		log.Fatalf("LoadReader failed: %v", err)
	}
	fmt.Printf("Loaded reader: %v\n", loadedReader)

	// Test Remove operation
	fmt.Println("\nTesting Remove...")
	err = rdb.Remove(rec)
	if err != nil {
		log.Fatalf("Remove failed: %v", err)
	}
	// Verify removal
	_, err = rdb.LoadReader(rec)
	if err != nil {
		fmt.Printf("Record successfully removed: %v\n", err)
	} else {
		fmt.Println("Record still exists after removal.")
	}

	// Test Cut operation
	fmt.Println("\nTesting Cut...")
	// Append multiple records to test Cut
	for i := 2; i <= 5; i++ {
		rec := etf.Tuple{etf.Atom("test_record"), i}
		_, err = rdb.Append(rec, feed)
		if err != nil {
			log.Fatalf("Append failed: %v", err)
		}
	}
	err = rdb.Cut(feed)
	if err != nil {
		log.Fatalf("Cut failed: %v", err)
	}
	// Verify that records are removed
	results, err = rdb.Take(startKey, num)
	if err != nil {
		log.Fatalf("Take failed after Cut: %v", err)
	}
	fmt.Printf("Records after Cut: %v\n", results)

	// Clean up database
	db.Close()
	err = os.RemoveAll(dbPath)
	if err != nil {
		log.Fatalf("Failed to clean up db: %v", err)
	}
	fmt.Println("\nAll tests completed successfully.")
}
