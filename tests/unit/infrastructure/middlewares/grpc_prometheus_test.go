package middlewares

import (
	"broadcasting/internal/infrastructure/middlewares"
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCPrometheusClient_PassesThroughAndCountsOK(t *testing.T) {
	interceptor := middlewares.GRPCPrometheusClient()
	const method = "/auth.v1.AuthService/TestClientOK"
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return nil
	}

	err := interceptor(context.Background(), method, "request", "reply", nil, invoker)

	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		middlewares.GRPCRequestsTotal.WithLabelValues(method, codes.OK.String()),
	))
}

func TestGRPCPrometheusClient_PassesThroughErrorAndCountsCode(t *testing.T) {
	interceptor := middlewares.GRPCPrometheusClient()
	const method = "/auth.v1.AuthService/TestClientUnavailable"
	wantErr := status.Error(codes.Unavailable, "session store unreachable")
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return wantErr
	}

	err := interceptor(context.Background(), method, "request", "reply", nil, invoker)

	assert.Equal(t, wantErr, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		middlewares.GRPCRequestsTotal.WithLabelValues(method, codes.Unavailable.String()),
	))
}
