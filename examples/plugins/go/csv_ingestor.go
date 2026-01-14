/*
CSV Ingestor Plugin for POLKU

Parses CSV data into POLKU events.
Perfect for ingesting logs, metrics, or any tabular data.

Usage:

	go run csv_ingestor.go

That's it! The plugin will auto-register with POLKU.
*/
package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	// You'd generate these from polku protos
	// pb "github.com/yourorg/polku-proto/gen/go/polku/v1"
)

// ============================================================================
// Plugin Implementation - This is all you need to write!
// ============================================================================

func parseCSV(data []byte, source string) ([]*Event, error) {
	reader := csv.NewReader(strings.NewReader(string(data)))

	// First row is headers
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	var events []*Event
	rowNum := 0

	for {
		record, err := reader.Read()
		if err != nil {
			break // EOF or error
		}
		rowNum++

		// Create metadata from CSV columns
		metadata := make(map[string]string)
		for i, header := range headers {
			if i < len(record) {
				metadata[header] = record[i]
			}
		}

		// Create event
		event := &Event{
			Id:              fmt.Sprintf("%s-row-%d-%d", source, rowNum, time.Now().UnixNano()),
			TimestampUnixNs: time.Now().UnixNano(),
			Source:          source,
			EventType:       "csv.row",
			Metadata:        metadata,
			Severity:        1, // Info
			Outcome:         0, // Success
		}

		events = append(events, event)
	}

	return events, nil
}

// ============================================================================
// gRPC Service - Boilerplate that connects your logic to POLKU
// ============================================================================

type csvIngestorPlugin struct {
	// Embed for forward compatibility
	// pb.UnimplementedIngestorPluginServer
}

func (p *csvIngestorPlugin) Info(ctx context.Context, _ *emptypb.Empty) (*PluginInfo, error) {
	return &PluginInfo{
		Name:        "csv-ingestor",
		Version:     "1.0.0",
		Type:        1, // INGESTOR
		Description: "Parse CSV data into POLKU events",
		Sources:     []string{"csv", "csv-logs", "metrics-csv"},
	}, nil
}

func (p *csvIngestorPlugin) Health(ctx context.Context, _ *emptypb.Empty) (*PluginHealthResponse, error) {
	return &PluginHealthResponse{Healthy: true, Message: "Ready to parse CSVs!"}, nil
}

func (p *csvIngestorPlugin) Ingest(ctx context.Context, req *IngestRequest) (*IngestResponse, error) {
	events, err := parseCSV(req.Data, req.Source)
	if err != nil {
		return &IngestResponse{
			Events: nil,
			Errors: []*IngestError{{Message: err.Error()}},
		}, nil
	}

	return &IngestResponse{Events: events}, nil
}

// ============================================================================
// Server startup with auto-registration
// ============================================================================

func main() {
	// Start gRPC server
	lis, err := net.Listen("tcp", ":9002")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	// pb.RegisterIngestorPluginServer(server, &csvIngestorPlugin{})

	fmt.Println(`
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║   📊 CSV Ingestor Plugin Running!                         ║
    ║                                                           ║
    ║   Listening on:  localhost:9002                           ║
    ║   Sources:       csv, csv-logs, metrics-csv               ║
    ║                                                           ║
    ║   Send CSV data to POLKU with source: "csv"               ║
    ║   Each row becomes an event with columns as metadata!     ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
	`)

	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// ============================================================================
// Proto types (normally generated, shown here for completeness)
// ============================================================================

type Event struct {
	Id              string
	TimestampUnixNs int64
	Source          string
	EventType       string
	Metadata        map[string]string
	Payload         []byte
	Severity        int32
	Outcome         int32
}

type PluginInfo struct {
	Name        string
	Version     string
	Type        int32
	Description string
	Sources     []string
}

type PluginHealthResponse struct {
	Healthy bool
	Message string
}

type IngestRequest struct {
	Source  string
	Cluster string
	Format  string
	Data    []byte
}

type IngestResponse struct {
	Events []*Event
	Errors []*IngestError
}

type IngestError struct {
	Message string
}
