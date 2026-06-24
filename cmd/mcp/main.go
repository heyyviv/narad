package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/narad/narad/internal/config"
	"github.com/narad/narad/internal/mcp"
	"github.com/narad/narad/internal/storage"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func main() {
	// Parse command line arguments
	transportFlag := flag.String("transport", "", "mcp transport type: stdio or sse (defaults to TRANSPORT env var, fallback to stdio)")
	portFlag := flag.String("port", "", "mcp server port for sse (defaults to PORT env var, fallback to 8090)")
	flag.Parse()

	cfg := config.Load()

	// Resolve transport
	transport := *transportFlag
	if transport == "" {
		transport = os.Getenv("TRANSPORT")
	}
	if transport == "" {
		transport = "stdio"
	}
	transport = strings.ToLower(strings.TrimSpace(transport))

	// Resolve port
	port := *portFlag
	if port == "" {
		port = cfg.Port
	}
	if port == "" || port == "8080" { // Avoid conflicting with default server port
		port = "8090"
	}

	apiKey := os.Getenv("API_KEY")

	// Set up TimescaleDB storage
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.NewStorage(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// Initialize MCP server
	mcpServer := server.NewMCPServer(
		"Narad Log Intelligence MCP Server",
		"1.0.0",
	)

	handlers := mcp.NewToolHandlers(store)

	// Register search_logs tool
	mcpServer.AddTool(
		mcpgo.NewTool("search_logs",
			mcpgo.WithDescription("Search structured logs with optional query substring, service, level, timestamps, limit, and dimension filters."),
			mcpgo.WithString("query", mcpgo.Description("Substring to search for in log messages")),
			mcpgo.WithString("service", mcpgo.Description("Filter by service name")),
			mcpgo.WithString("level", mcpgo.Description("Filter by log level (e.g., ERROR, INFO)")),
			mcpgo.WithString("from", mcpgo.Description("Start time (RFC3339 format, e.g., 2026-06-24T15:00:00Z)")),
			mcpgo.WithString("to", mcpgo.Description("End time (RFC3339 format, e.g., 2026-06-24T16:00:00Z)")),
			mcpgo.WithNumber("limit", mcpgo.Description("Maximum number of logs to return (default 100, max 1000)")),
			mcpgo.WithObject("dims", mcpgo.Description("Key-value object of dimensions to filter by (e.g., {\"customer_id\": \"cust_123\"})")),
		),
		handlers.SearchLogs,
	)

	// Register trace_request tool
	mcpServer.AddTool(
		mcpgo.NewTool("trace_request",
			mcpgo.WithDescription("Trace a request or trace ID across services via log dimensions."),
			mcpgo.WithString("trace_id", mcpgo.Required(), mcpgo.Description("The trace ID or request ID to search for")),
			mcpgo.WithString("trace_key", mcpgo.Description("The dimension key to match (default: trace_id). If not specified, matches standard trace keys or any dimension key if no match.")),
		),
		handlers.TraceRequest,
	)

	// Register get_errors tool
	mcpServer.AddTool(
		mcpgo.NewTool("get_errors",
			mcpgo.WithDescription("Get recent errors grouped by message pattern to identify root causes of spikes."),
			mcpgo.WithString("service", mcpgo.Description("Filter by service name")),
			mcpgo.WithNumber("lookback_minutes", mcpgo.Description("Minutes of history to search (default: 60)")),
			mcpgo.WithNumber("limit", mcpgo.Description("Maximum number of error groups to return (default: 20)")),
		),
		handlers.GetErrors,
	)

	// Register explain_incident tool
	mcpServer.AddTool(
		mcpgo.NewTool("explain_incident",
			mcpgo.WithDescription("Examine log context surrounding a specific incident timestamp (±5 minutes) to identify contributing factors."),
			mcpgo.WithString("timestamp", mcpgo.Required(), mcpgo.Description("The timestamp of the incident (RFC3339 format, e.g., 2026-06-24T15:00:00Z)")),
			mcpgo.WithString("service", mcpgo.Description("Target service name to focus on")),
			mcpgo.WithNumber("lookback_minutes", mcpgo.Description("Minutes before and after the incident timestamp to include (default: 5)")),
			mcpgo.WithNumber("limit", mcpgo.Description("Maximum number of logs to return (default: 100)")),
		),
		handlers.ExplainIncident,
	)

	// Register tail_service tool
	mcpServer.AddTool(
		mcpgo.NewTool("tail_service",
			mcpgo.WithDescription("Tail the latest logs from a specific service."),
			mcpgo.WithString("service", mcpgo.Required(), mcpgo.Description("The service name to tail")),
			mcpgo.WithNumber("limit", mcpgo.Description("Number of recent logs to return (default: 50)")),
			mcpgo.WithString("level", mcpgo.Description("Filter by log level (e.g., ERROR, INFO)")),
		),
		handlers.TailService,
	)

	// Start server based on transport choice
	switch transport {
	case "sse":
		baseURL := fmt.Sprintf("http://localhost:%s", port)
		sseServer := server.NewSSEServer(mcpServer, server.WithBaseURL(baseURL))

		// Create ServeMux and wrap endpoints with authentication middleware
		mux := http.NewServeMux()
		mux.Handle("/sse", authMiddleware(apiKey, sseServer.SSEHandler()))
		mux.Handle("/message", authMiddleware(apiKey, sseServer.MessageHandler()))

		log.Printf("Starting Narad MCP SSE Server on port %s (auth enabled: %t)", port, apiKey != "")
		if err := http.ListenAndServe(":"+port, mux); err != nil {
			log.Fatalf("SSE server failed: %v", err)
		}

	case "stdio":
		log.Println("Starting Narad MCP stdio Server...")
		if err := server.ServeStdio(mcpServer); err != nil {
			log.Fatalf("stdio server failed: %v", err)
		}

	default:
		log.Fatalf("Unknown transport layer: %s. Supported: stdio, sse", transport)
	}
}

// Authentication middleware for SSE transport
func authMiddleware(apiKey string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if apiKey == "" {
			next.ServeHTTP(w, r)
			return
		}

		// Check Authorization: Bearer <key>
		authHeader := r.Header.Get("Authorization")
		if authHeader != "" {
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if token == apiKey {
				next.ServeHTTP(w, r)
				return
			}
		}

		// Check X-API-Key: <key>
		xApiKey := r.Header.Get("X-API-Key")
		if xApiKey == apiKey {
			next.ServeHTTP(w, r)
			return
		}

		// Check URL parameters: api_key=<key> or token=<key>
		queryKey := r.URL.Query().Get("api_key")
		if queryKey == apiKey {
			next.ServeHTTP(w, r)
			return
		}
		queryToken := r.URL.Query().Get("token")
		if queryToken == apiKey {
			next.ServeHTTP(w, r)
			return
		}

		http.Error(w, "Unauthorized: Invalid or missing API key", http.StatusUnauthorized)
	})
}
