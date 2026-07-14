package grpc

import (
	"broadcasting/internal/domain/notification/actions"
	"broadcasting/internal/infrastructure/middlewares"
	"context"
	"time"

	authv1 "github.com/guille1988/go-app-shared/rpc/auth/v1"

	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// AuthClient is the gRPC client for auth's AuthService.
type AuthClient struct {
	conn        *googlegrpc.ClientConn
	client      authv1.AuthServiceClient
	callTimeout time.Duration
}

/*
NewAuthClient creates a lazily connecting client: grpc.NewClient does not
dial until the first RPC, so broadcasting starts fine while auth is still
booting. Credentials are insecure because this is container-to-container
traffic on the private network, the same trust zone as the plaintext Kafka
and forward-auth HTTP traffic.
*/
func NewAuthClient(address string, callTimeout time.Duration) (*AuthClient, error) {
	conn, err := googlegrpc.NewClient(
		address,
		googlegrpc.WithTransportCredentials(insecure.NewCredentials()),
		googlegrpc.WithChainUnaryInterceptor(middlewares.GRPCPrometheusClient()),
	)

	if err != nil {
		return nil, err
	}

	return &AuthClient{
		conn:        conn,
		client:      authv1.NewAuthServiceClient(conn),
		callTimeout: callTimeout,
	}, nil
}

/*
ValidateToken asks auth whether the token is still valid. It implements
actions.TokenValidator.
*/
func (authClient *AuthClient) ValidateToken(ctx context.Context, token string) (actions.TokenValidation, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, authClient.callTimeout)
	defer cancel()

	response, err := authClient.client.ValidateToken(ctxWithTimeout, &authv1.ValidateTokenRequest{Token: token})

	if err != nil {
		return actions.TokenValidation{}, err
	}

	return actions.TokenValidation{
		Valid:  response.GetValid(),
		Reason: response.GetReason().String(),
	}, nil
}

// Close releases the underlying connection.
func (authClient *AuthClient) Close() error {
	return authClient.conn.Close()
}
