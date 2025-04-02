package main

import (
	"build-your-own-database/pkg/db"
	"fmt"
	"log"
)

func main() {
	// Initialize the database
	database, err := db.NewDB("data/db")
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer database.Close()

	// Insert some key-value pairs
	keyValuePairs := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"grape":  "purple",
		"orange": "orange",
		"cherry": "red",
	}

	fmt.Println("Inserting key-value pairs...")
	for key, value := range keyValuePairs {
		if err := database.Put([]byte(key), []byte(value)); err != nil {
			log.Printf("Failed to insert %s: %v", key, err)
		}
	}

	// Traverse the database and print all key-value pairs
	fmt.Println("\nDatabase Contents:")
	database.Traverse(func(key, value []byte) {
		fmt.Printf("%s -> %s\n", string(key), string(value))
	})

	// Test searching for keys
	searchKeys := []string{"apple", "banana", "mango"}
	fmt.Println("\nSearch Results:")
	for _, key := range searchKeys {
		if value, found := database.Get([]byte(key)); found {
			fmt.Printf("Found: %s -> %s\n", key, string(value))
		} else {
			fmt.Printf("Not Found: %s\n", key)
		}
	}

	// Test deletion
	fmt.Println("\nTesting deletion...")
	if err := database.Delete([]byte("apple")); err != nil {
		log.Printf("Failed to delete apple: %v", err)
	}

	// Verify deletion
	if value, found := database.Get([]byte("apple")); found {
		fmt.Printf("Apple still exists: %s\n", string(value))
	} else {
		fmt.Println("Apple successfully deleted")
	}
}
