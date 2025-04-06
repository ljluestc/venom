// executors/sql/sql_test.go
package sql

import (
    "context"
    "strings"
    "testing"

    "github.com/jmoiron/sqlx"
    "github.com/ovh/venom"
    _ "github.com/mattn/go-sqlite3" // Ensure SQLite driver is imported
)

// TestSQLExecutorConstraints tests the SQL executor's ability to handle constraint violations
func TestSQLExecutorConstraints(t *testing.T) {
    // Setup in-memory SQLite database
    db, err := sqlx.Open("sqlite3", ":memory:")
    if err != nil {
        t.Fatalf("Failed to open SQLite: %v", err)
    }
    defer db.Close()

    // Ensure the database connection is working by pinging it
    if err := db.Ping(); err != nil {
        t.Fatalf("Failed to ping SQLite: %v", err)
    }

    // Define test step
    step := venom.TestStep{
        "driver": "sqlite3",
        "dsn":    ":memory:",
        "commands": []string{
            "CREATE TABLE dogs (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            "INSERT INTO dogs (name) VALUES (NULL)",
        },
    }

    // Create a proper context with venom's default logger
    ctx := context.Background()
    ctx = venom.ContextWithLogger(ctx, venom.DefaultLogger())

    executor := Executor{}
    result, err := executor.Run(ctx, step)
    if err != nil {
        t.Fatalf("Executor failed unexpectedly: %v", err)
    }

    // Verify result
    res, ok := result.(Result)
    if !ok {
        t.Fatal("Result is not of type Result")
    }
    if len(res.Queries) != 2 {
        t.Errorf("Expected 2 query results, got %d", len(res.Queries))
    }

    // Check CREATE command
    if res.Queries[0].Error != "" {
        t.Errorf("Expected no error for CREATE, got: %s", res.Queries[0].Error)
    }

    // Check INSERT command (adjust error message for go-sqlite3)
    if !strings.Contains(res.Queries[1].Error, "constraint failed") {
        t.Errorf("Expected NOT NULL constraint error, got: %s", res.Queries[1].Error)
    }
    if len(res.Queries[1].Rows) != 0 {
        t.Errorf("Expected no rows for failed INSERT, got %d", len(res.Queries[1].Rows))
    }
}