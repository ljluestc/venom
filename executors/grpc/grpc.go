package grpc

import (
    "context"
    "encoding/base64"
    "fmt"
    "strings"

    "github.com/fullstorydev/grpcurl"
    "github.com/golang/protobuf/proto"
    "github.com/ovh/venom"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/metadata"
    "google.golang.org/grpc/status"
    rpb "google.golang.org/genproto/googleapis/rpc/status"
)

// Name of executor
const Name = "grpc"

// Executor represents the gRPC executor
type Executor struct {
    URL            string            `json:"url" yaml:"url"`
    Service        string            `json:"service" yaml:"service"`
    Method         string            `json:"method" yaml:"method"`
    Data           string            `json:"data,omitempty" yaml:"data,omitempty"`
    Headers        map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
    ConnectTimeout int               `json:"connect_timeout,omitempty" yaml:"connect_timeout,omitempty"`
    TLSRootCA      string            `json:"tls_root_ca,omitempty" yaml:"tls_root_ca,omitempty"`
    TLSClientCert  string            `json:"tls_client_cert,omitempty" yaml:"tls_client_cert,omitempty"`
    TLSClientKey   string            `json:"tls_client_key,omitempty" yaml:"tls_client_key,omitempty"`
    IgnoreVerifySSL bool             `json:"ignore_verify_ssl,omitempty" yaml:"ignore_verify_ssl,omitempty"`
}

// Result represents a step result
type Result struct {
    Code         int                    `json:"code" yaml:"code"`
    SystemOut    string                 `json:"systemout" yaml:"systemout"`
    SystemErr    string                 `json:"systemerr" yaml:"systemerr"`
    Errors       map[string]interface{} `json:"errors" yaml:"errors"`
}

// New returns a new Executor
func New() venom.Executor {
    return &Executor{}
}

// GetDefaultAssertions returns default assertions for this executor
func (Executor) GetDefaultAssertions() venom.StepAssertions {
    return venom.StepAssertions{
        Assertions: []string{"result.code ShouldEqual 0"},
    }
}

// Run executes the gRPC test step
func (e *Executor) Run(ctx context.Context, step venom.TestStep) (interface{}, error) {
    // Decode step using venom’s current method
    if err := venom.JSONUnmarshalStep(step, e); err != nil {
        return nil, fmt.Errorf("failed to decode step: %v", err)
    }

    if e.URL == "" || e.Service == "" || e.Method == "" {
        return nil, fmt.Errorf("url, service, and method are mandatory")
    }

    // Dial gRPC server
    conn, err := grpc.DialContext(ctx, e.URL, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, fmt.Errorf("failed to dial grpc %s: %v", e.URL, err)
    }
    defer conn.Close()

    // Prepare request metadata
    md := make([]string, 0, len(e.Headers)*2)
    for k, v := range e.Headers {
        md = append(md, k, v)
    }

    // Response handlers
    var systemOut, systemErr strings.Builder
    handler := &grpcurlHandler{
        out:    &systemOut,
        errOut: &systemErr,
    }

    // Descriptor source with reflection
    descSource, err := grpcurl.DescriptorSourceFromServer(ctx, conn)
    if err != nil {
        return nil, fmt.Errorf("failed to create descriptor source: %v", err)
    }

    // Request supplier
    rf, formatter, err := grpcurl.RequestParserAndFormatterFor(grpcurl.FormatJSON, descSource, true, true, strings.NewReader(e.Data))
    if err != nil {
        return nil, fmt.Errorf("failed to create request parser: %v", err)
    }

    // Invoke gRPC method
    err = grpcurl.InvokeRPC(ctx, descSource, conn, fmt.Sprintf("%s/%s", e.Service, e.Method), md, handler, rf.Next)
    result := Result{
        SystemOut: systemOut.String(),
        SystemErr: systemErr.String(),
    }

    // Handle gRPC status and error details
    if err != nil {
        if st, ok := status.FromError(err); ok {
            result.Code = int(st.Code())
            if trailers := handler.trailers; len(trailers) > 0 {
                if details := trailers["grpc-status-details-bin"]; len(details) > 0 {
                    detailBytes, err := base64.StdEncoding.DecodeString(details[0])
                    if err == nil {
                        var statusProto rpb.Status
                        if err := proto.Unmarshal(detailBytes, &statusProto); err == nil {
                            detailsMap := make(map[string]interface{})
                            detailsMap["message"] = statusProto.Message
                            detailsMap["code"] = statusProto.Code
                            if len(statusProto.Details) > 0 {
                                detailsList := make([]interface{}, len(statusProto.Details))
                                for i, d := range statusProto.Details {
                                    detailsList[i] = map[string]interface{}{
                                        "type_url": d.TypeUrl,
                                        "value":    base64.StdEncoding.EncodeToString(d.Value),
                                    }
                                }
                                detailsMap["details"] = detailsList
                            }
                            result.Errors = detailsMap
                        }
                    }
                }
            }
        } else {
            return nil, fmt.Errorf("non-gRPC error: %v", err)
        }
    } else {
        result.Code = 0
    }

    return result, nil
}

// grpcurlHandler implements grpcurl.InvocationEventHandler
type grpcurlHandler struct {
    out      *strings.Builder
    errOut   *strings.Builder
    trailers metadata.MD
}

func (h *grpcurlHandler) OnResolveMethod(*grpcurl.DescriptorInfo)                    {}
func (h *grpcurlHandler) OnSendData()                                               {}
func (h *grpcurlHandler) OnReceiveData(s string)                                    { h.out.WriteString(s) }
func (h *grpcurlHandler) OnReceiveHeaders(md metadata.MD)                           {}
func (h *grpcurlHandler) OnReceiveResponse(ctx context.Context, m proto.Message)    { h.out.WriteString(fmt.Sprintf("%v", m)) }
func (h *grpcurlHandler) OnReceiveTrailers(st *status.Status, md metadata.MD) {
    h.trailers = md
    if st != nil && st.Code() != codes.OK {
        h.errOut.WriteString(st.Message())
    }
}