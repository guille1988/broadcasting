package actions

import (
	"broadcasting/internal/infrastructure/websocket"
	"context"
	"log/slog"
)

/*
CloseCodeInvalidToken is the application close code (4000-4999 range) sent
when a connection's token is no longer valid. Contract with clients:
on 4401, refresh the token and reconnect.
*/
const CloseCodeInvalidToken = 4401

// TokenValidation is the outcome of validating one token against auth.
type TokenValidation struct {
	Valid  bool
	Reason string
}

// TokenValidator asks the auth service whether a token is still valid.
type TokenValidator interface {
	ValidateToken(ctx context.Context, token string) (TokenValidation, error)
}

// ConnectionRegistry is the subset of the Hub this action needs.
type ConnectionRegistry interface {
	Snapshot() []websocket.ClientSnapshot
	Disconnect(client *websocket.Client, code int, reason string)
}

// RevalidateTokens closes WebSocket connections whose token is no longer valid.
type RevalidateTokens struct {
	registry  ConnectionRegistry
	validator TokenValidator
}

// NewRevalidateTokens creates a RevalidateTokens action backed by the given hub and validator.
func NewRevalidateTokens(registry ConnectionRegistry, validator TokenValidator) *RevalidateTokens {
	return &RevalidateTokens{registry: registry, validator: validator}
}

/*
Execute snapshots the open connections and validates each unique token once.
Connections with an invalid token get disconnected with close code 4401.
Connections without a token are disconnected without asking auth (fail
closed: the gateway's forward-auth guarantees proxied connections carry one,
so a token-less connection dialed the service directly). Validator errors
(auth unreachable, timeout) keep the connections (fail open: expiry is
enforced on the next successful run instead of punishing users for an
infra failure).
*/
func (action *RevalidateTokens) Execute(ctx context.Context) error {
	snapshot := action.registry.Snapshot()

	if len(snapshot) == 0 {
		return nil
	}

	clientsByToken := make(map[string][]websocket.ClientSnapshot)

	for _, client := range snapshot {
		clientsByToken[client.Token] = append(clientsByToken[client.Token], client)
	}

	for token, clients := range clientsByToken {
		if err := ctx.Err(); err != nil {
			return err
		}

		if token == "" {
			action.disconnectAll(clients, "token required")
			continue
		}

		validation, err := action.validator.ValidateToken(ctx, token)

		if err != nil {
			slog.Warn("token revalidation skipped, auth unreachable", "error", err, "connections", len(clients))
			continue
		}

		if !validation.Valid {
			action.disconnectAll(clients, validation.Reason)
		}
	}

	return nil
}

func (action *RevalidateTokens) disconnectAll(clients []websocket.ClientSnapshot, reason string) {
	for _, client := range clients {
		slog.Info("closing connection with invalid token", "user_uuid", client.UserUUID, "reason", reason)
		action.registry.Disconnect(client.Client, CloseCodeInvalidToken, reason)
	}
}
