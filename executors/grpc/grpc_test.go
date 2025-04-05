package grpc

import (
    "context"
    "encoding/base64"
    "net"
    "testing"
    "time"

    "github.com/golang/protobuf/proto"
    "github.com/ovh/venom"
    "github.com/stretchr/testify/assert"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/metadata"
    "google.golang.org/grpc/status"
    rpb "google.golang.org/genproto/googleapis/rpc/status"
)

// mockEchoServiceServer for testing
type mockEchoServiceServer struct {
    grpc.ServerStream
}

func (s *mockEchoServiceServer) Echo(ctx context.Context, req *proto.Message) (*proto.Message, error) {
    data, _ := json.Marshal(req)
    if string(data) == `{"message":"hello"}` {
        return &proto.Message{Data: `{"response":"hello back"}`}, nil
    }
    // Return error with details
    st := status.New(codes.NotFound, "resource not found")
    detail := &rpb.BadRequest{
        FieldViolations: []*rpb.BadRequest_FieldViolation{
            {Field: "message", Description: "invalid value"},
        },
    }
    detailBytes, _ := proto.Marshal(detail)
    st, _ = st.WithDetails(detail)
    md := metadata.Pairs("grpc-status-details-bin", base64.StdEncoding.EncodeToString(detailBytes))
    grpc.SetTrailer(ctx, md)
    return nil, st.Err()
}

func TestGRPCExecutorSuccess(t *testing.T) {
    // Start mock server
    lis, err := net.Listen("tcp", ":0")
    assert.NoError(t, err)
    s := grpc.NewServer()
    defer s.Stop()
    go s.Serve(lis)

    step := venom.TestStep{
        "URL":     lis.Addr().String(),
        "Service": "echo.EchoService",
        "Method":  "Echo",
        "Data":    `{"message": "hello"}`,
    }

    exec := New()
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    result, err := exec.Run(ctx, step)
    assert.NoError(t, err)
    res, ok := result.(Result)
    assert.True(t, ok)
    assert.Equal(t, 0, res.Code)
    assert.Contains(t, res.SystemOut, "hello back")
}

func TestGRPCExecutorInvalidStep(t *testing.T) {
    step := venom.TestStep{}

    exec := New()
    ctx := context.Background()

    _, err := exec.Run(ctx, step)
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "url, service, and method are mandatory")
}

func TestGetDefaultAssertions(t *testing.T) {
    exec := New()
    assertions := exec.GetDefaultAssertions()
    assert.NotNil(t, assertions)
    assert.Len(t, assertions.Assertions, 1)
    assert.Equal(t, "result.code ShouldEqual 0", assertions.Assertions[0])
}

func TestGRPCExecutorErrorDetails(t *testing.T) {
    // Start mock server
    lis, err := net.Listen("tcp", ":0")
    assert.NoError(t, err)
    s := grpc.NewServer()
    defer s.Stop()
    go s.Serve(lis)

    step := venom.TestStep{
        "URL":     lis.Addr().String(),
        "Service": "echo.EchoService",
        "Method":  "Echo",
        "Data":    `{"message": "nonexistent"}`,
    }

    exec := New()
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    result, err := exec.Run(ctx, step)
    assert.Error(t, err)
    res, ok := result.(Result)
    assert.True(t, ok)
    assert.Equal(t, int(codes.NotFound), res.Code)
    assert.Equal(t, "resource not found", res.Errors["message"])
    assert.Equal(t, int32(codes.NotFound), res.Errors["code"])
    details, ok := res.Errors["details"].([]interface{})
    assert.True(t, ok)
    assert.Len(t, details, 1)
    detail, ok := details[0].(map[string]interface{})
    assert.True(t, ok)
    assert.Equal(t, "google.rpc.BadRequest", detail["type_url"])
}