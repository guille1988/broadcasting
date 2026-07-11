package revalidation

import (
	"broadcasting/internal/domain/notification/actions"
	grpcprovider "broadcasting/internal/infrastructure/providers/grpc"
	"broadcasting/tests/integration"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	authv1 "github.com/guille1988/go-app-shared/rpc/auth/v1"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	googlegrpc "google.golang.org/grpc"
)

/*
fakeAuthService is a hand-written authv1.AuthServiceServer with scripted
verdicts per token. Unknown tokens are reported valid so leftover
connections from other test cases never interfere.
*/
type fakeAuthService struct {
	authv1.UnimplementedAuthServiceServer
	verdicts map[string]*authv1.ValidateTokenResponse
	err      error
}

func (service *fakeAuthService) ValidateToken(_ context.Context, request *authv1.ValidateTokenRequest) (*authv1.ValidateTokenResponse, error) {
	if service.err != nil {
		return nil, service.err
	}

	if verdict, ok := service.verdicts[request.GetToken()]; ok {
		return verdict, nil
	}

	return &authv1.ValidateTokenResponse{Valid: true}, nil
}

/*
newAuthClient serves the fake AuthService on a real TCP listener and
returns the production AuthClient dialed against it, covering the same
wire path used at runtime.
*/
func newAuthClient(test *testing.T, service *fakeAuthService) *grpcprovider.AuthClient {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(test, err)

	server := googlegrpc.NewServer()
	authv1.RegisterAuthServiceServer(server, service)

	go func() {
		_ = server.Serve(listener)
	}()

	var authClient *grpcprovider.AuthClient
	authClient, err = grpcprovider.NewAuthClient(listener.Addr().String(), 2*time.Second)
	assert.NoError(test, err)

	test.Cleanup(func() {
		_ = authClient.Close()
		server.Stop()
	})

	return authClient
}

func dial(test *testing.T, userUUID, token string) *websocket.Conn {
	wsURL := "ws" + strings.TrimPrefix(integration.TestServer.URL, "http") + "/api/ws/" + userUUID

	headers := http.Header{}
	headers.Set("X-User-UUID", userUUID)
	headers.Set("Authorization", "Bearer "+token)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, headers)
	assert.NoError(test, err)

	return conn
}

/*
waitForTokens blocks until the hub's snapshot contains every given token:
registration happens on the hub goroutine after the dial returns, so a test
must not revalidate before the hub has seen its connections.
*/
func waitForTokens(test *testing.T, tokens ...string) {
	deadline := time.Now().Add(3 * time.Second)

	for {
		present := make(map[string]bool)

		for _, client := range integration.TestApp.Container.Hub.Snapshot() {
			present[client.Token] = true
		}

		missing := false

		for _, token := range tokens {
			if !present[token] {
				missing = true
			}
		}

		if !missing {
			return
		}

		if time.Now().After(deadline) {
			test.Fatalf("hub never registered the expected tokens %v", tokens)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

/*
waitForUserMessage asserts the connection still works by sending it a
directed message through the hub and reading it back.
*/
func waitForUserMessage(test *testing.T, conn *websocket.Conn, userUUID string) {
	message := "still connected"
	integration.TestApp.Container.Hub.SendToUser(userUUID, []byte(message))

	assert.NoError(test, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, received, err := conn.ReadMessage()
	assert.NoError(test, err)
	assert.Equal(test, message, string(received))
}

func TestTokenRevalidation(test *testing.T) {
	integration.TestCase(test, "it should close connections with an invalid token and keep the valid ones", func(test *testing.T) {
		authClient := newAuthClient(test, &fakeAuthService{verdicts: map[string]*authv1.ValidateTokenResponse{
			"stale-token": {Valid: false, Reason: authv1.ValidateTokenResponse_EXPIRED},
			"good-token":  {Valid: true},
		}})

		staleConn := dial(test, "stale-user", "stale-token")
		defer func() { _ = staleConn.Close() }()

		goodConn := dial(test, "good-user", "good-token")
		defer func() { _ = goodConn.Close() }()

		waitForTokens(test, "stale-token", "good-token")

		action := actions.NewRevalidateTokens(integration.TestApp.Container.Hub, authClient)
		assert.NoError(test, action.Execute(context.Background()))

		assert.NoError(test, staleConn.SetReadDeadline(time.Now().Add(3*time.Second)))
		_, _, err := staleConn.ReadMessage()

		var closeErr *websocket.CloseError
		assert.ErrorAs(test, err, &closeErr)
		assert.Equal(test, actions.CloseCodeInvalidToken, closeErr.Code)
		assert.Equal(test, authv1.ValidateTokenResponse_EXPIRED.String(), closeErr.Text)

		waitForUserMessage(test, goodConn, "good-user")
	})

	integration.TestCase(test, "it should fail open and keep every connection when auth is unreachable", func(test *testing.T) {
		authClient := newAuthClient(test, &fakeAuthService{err: errors.New("auth is down")})

		conn := dial(test, "some-user", "some-token")
		defer func() { _ = conn.Close() }()

		waitForTokens(test, "some-token")

		action := actions.NewRevalidateTokens(integration.TestApp.Container.Hub, authClient)
		assert.NoError(test, action.Execute(context.Background()))

		waitForUserMessage(test, conn, "some-user")
	})
}
