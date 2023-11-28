package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
)

type Config struct {
	ReservedWords []string `json:"reserved_words"`
	RecordStart   string   `json:"record_start"`
}

func startMemoryProfile() *os.File {
	memProfile, err := os.Create("mem_profile.pprof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.WriteHeapProfile(memProfile)
	return memProfile
}

func readConfig(configFilePath string) (Config, error) {
	var config Config
	configFile, err := os.Open(configFilePath)
	if err != nil {
		return config, err
	}
	defer configFile.Close()

	decoder := json.NewDecoder(configFile)
	err = decoder.Decode(&config)
	if err != nil {
		return config, err
	}

	return config, nil
}

// DownloadFile downloads a file from a URL and saves it to a local path.
func downloadFile(url, localFilePath string) error {

	response, err := http.Get(url)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Failed to download file. Status code: %d", response.StatusCode)
	}

	file, err := os.Create(localFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, response.Body)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	// Read the environment variable for debug mode
	debugModeEnabled := os.Getenv("DEBUG_MODE") == "true"

	if debugModeEnabled {
		memProfile := startMemoryProfile()
		defer memProfile.Close()
	}
	// Parse command line arguments
	url := flag.String("url", "", "URL of the file to download and process")
	flag.Parse()

	if *url == "" {
		fmt.Println("Please provide a URL using the -url flag")
		return
	}

	// Create data directory if it doesn't exist
	dataDir := "data"
	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Read the config from config.json
	configFilePath := filepath.Join("config", "config.json")
	config, err := readConfig(configFilePath)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}
	// Print the value of the config for debugging purposes
	if debugModeEnabled {
		log.Printf("Config: %+v\n", config)
	}

	// Download the file
	localFilePath := filepath.Join(dataDir, filepath.Base(*url))
	if err := downloadFile(*url, localFilePath); err != nil {
		log.Fatalf("Failed to download file: %v", err)
	}
	defer os.Remove(localFilePath)

	baseOutputFolder := filepath.Join(dataDir, "output")
	os.RemoveAll(baseOutputFolder) // Remove the existing output folder
	// Process the file
	if err := processFile(localFilePath, config, baseOutputFolder); err != nil {
		log.Fatalf("Failed to process file: %v", err)
	}

}

// isReservedWord checks if a key exists in ReservedWords
func isReservedWord(config Config, key string) bool {
	for _, word := range config.ReservedWords {
		if strings.EqualFold(word, key) {
			return true
		}
	}
	return false
}

func processFile(filePath string, config Config, outputDir string) error {
	inFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer inFile.Close()

	gzipReader, err := gzip.NewReader(inFile)
	if err != nil {
		return err
	}
	defer gzipReader.Close()

	reader := bufio.NewReader(gzipReader)
	record := make(map[string]string)
	jsonlFiles := make(map[string]*os.File)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break // Reached end of file
			}
			return err
		}

		if strings.TrimSpace(line) != config.RecordStart {
			parts := strings.Split(line, ":")
			key := parts[0]
			if len(parts) >= 2 && isReservedWord(config, key) {
				value := strings.TrimSpace(strings.Join(parts[1:], ":"))
				record[key] = value
				//log.Printf("isReservedWord: %+v\n", strings.TrimSpace(line))
			} else {
				//log.Printf("Not isReservedWord: %+v\n", strings.TrimSpace(line))
				//record["data"] += line + "\n"
				record["data"] += line
			}
		} else {
			//log.Printf("Line: %+v; PreviousRecord: %+v\n", strings.TrimSpace(line), record)
			if err := writeRecordToJSONL(record, outputDir, jsonlFiles); err != nil {
				return err
			}

			// Clear the record for the next one
			record = make(map[string]string)
		}
	}

	log.Printf("LastLine; PreviousRecord: %+v\n", record)

	if err := writeRecordToJSONL(record, outputDir, jsonlFiles); err != nil {
		return err
	}

	// Close all JSONL files
	for _, jsonlFile := range jsonlFiles {
		jsonlFile.Close()
	}

	return nil
}

func writeRecordToJSONL(record map[string]string, outputDir string, jsonlFiles map[string]*os.File) error {
	language := strings.ReplaceAll(record["WARC-Identified-Content-Language"], ",", "_")

	if language == "" {
		language = "eng" // Default language if not specified
	}

	outputFolder := filepath.Join(outputDir, language)
	os.MkdirAll(outputFolder, os.ModePerm)
	outputFile := filepath.Join(outputFolder, "output.jsonl")

	// Open or create the JSONL file
	jsonlFile, exists := jsonlFiles[outputFile]
	if !exists {
		var err error
		jsonlFile, err = os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644) // Set permissions to 0644
		if err != nil {
			return err
		}
		jsonlFiles[outputFile] = jsonlFile
	}

	// Write the record to the JSONL file
	jsonEncoder := json.NewEncoder(jsonlFile)
	jsonEncoder.SetEscapeHTML(false) // Disable HTML escaping
	if err := jsonEncoder.Encode(record); err != nil {
		return err
	}

	return nil
}
