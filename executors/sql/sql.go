// executors/sql/sql.go
package sql

import (
    "context"
    "os"
    "path"

    "github.com/jmoiron/sqlx"
    "github.com/mitchellh/mapstructure"
    "github.com/ovh/venom"
    "github.com/pkg/errors"

    // MySQL drivers
    _ "github.com/go-sql-driver/mysql"

    // Postgres driver
    _ "github.com/lib/pq"

    // Oracle
    _ "github.com/sijms/go-ora"

    // Sqlite
    _ "modernc.org/sqlite"
)

// Name of the executor.
const Name = "sql"

// New returns a new executor that can execute SQL queries
func New() venom.Executor {
    return &Executor{}
}

// Executor is a venom executor that can execute SQL queries
type Executor struct {
    File     string   `json:"file,omitempty" yaml:"file,omitempty"`
    Commands []string `json:"commands,omitempty" yaml:"commands,omitempty"`
    Driver   string   `json:"driver" yaml:"driver"`
    DSN      string   `json:"dsn" yaml:"dsn"`
}

// Rows represents an array of Row
type Rows []Row

// Row represents a row returned by a SQL query
type Row map[string]interface{}

// QueryResult represents the result of a single SQL query execution
type QueryResult struct {
    Rows  Rows   `json:"rows,omitempty" yaml:"rows,omitempty"`
    Error string `json:"error,omitempty" yaml:"error,omitempty"` // Added to store error messages
}

// Result represents a step result
type Result struct {
    Queries []QueryResult `json:"queries,omitempty" yaml:"queries,omitempty"`
}

// Run implements the venom.Executor interface for Executor
func (e Executor) Run(ctx context.Context, step venom.TestStep) (interface{}, error) {
    // Transform step to Executor instance
    if err := mapstructure.Decode(step, &e); err != nil {
        return nil, err
    }

    // Connect to the database and ping it
    venom.Debug(ctx, "connecting to database %s, %s\n", e.Driver, e.DSN)
    db, err := sqlx.ConnectContext(ctx, e.Driver, e.DSN)
    if err != nil {
        return nil, errors.Wrapf(err, "failed to connect to database")
    }
    defer db.Close()

    results := []QueryResult{}
    // Execute commands on database if specified
    if len(e.Commands) != 0 {
        for i, s := range e.Commands {
            venom.Debug(ctx, "Executing command number %d: %s\n", i, s)
            rows, err := db.QueryxContext(ctx, s)
            qr := QueryResult{}
            if err != nil {
                // Capture error instead of failing
                qr.Error = err.Error()
            } else {
                // Process rows if no error
                r, err := handleRows(rows)
                if err != nil {
                    qr.Error = err.Error()
                } else {
                    qr.Rows = r
                }
            }
            results = append(results, qr)
        }
    } else if e.File != "" {
        // Execute SQL from file if specified
        workdir := venom.StringVarFromCtx(ctx, "venom.testsuite.workdir")
        file := path.Join(workdir, e.File)
        venom.Debug(ctx, "loading SQL file from %s\n", file)
        sbytes, err := os.ReadFile(file)
        if err != nil {
            return nil, errors.Wrapf(err, "failed to read SQL file %q", file)
        }
        rows, err := db.QueryxContext(ctx, string(sbytes))
        qr := QueryResult{}
        if err != nil {
            qr.Error = err.Error()
        } else {
            r, err := handleRows(rows)
            if err != nil {
                qr.Error = err.Error()
            } else {
                qr.Rows = r
            }
        }
        results = append(results, qr)
    }

    r := Result{Queries: results}
    return r, nil
}

// ZeroValueResult returns an empty implementation of this executor result
func (Executor) ZeroValueResult() interface{} {
    return Result{}
}

// GetDefaultAssertions returns the default assertions of the executor
func (e Executor) GetDefaultAssertions() venom.StepAssertions {
    return venom.StepAssertions{Assertions: []venom.Assertion{}}
}

// handleRows iterates over SQL rows and serializes them into a []Row
func handleRows(rows *sqlx.Rows) ([]Row, error) {
    defer rows.Close()
    res := []Row{}
    for rows.Next() {
        row := make(Row)
        if err := rows.MapScan(row); err != nil {
            return nil, err
        }
        res = append(res, row)
    }
    if err := rows.Err(); err != nil {
        return res, err
    }
    return res, nil
}