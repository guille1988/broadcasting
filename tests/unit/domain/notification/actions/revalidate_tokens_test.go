package actions

import (
	"broadcasting/internal/domain/notification/actions"
	"broadcasting/internal/infrastructure/websocket"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type disconnection struct {
	client *websocket.Client
	code   int
	reason string
}

type fakeRegistry struct {
	clients        []websocket.ClientSnapshot
	disconnections []disconnection
}

func (registry *fakeRegistry) Snapshot() []websocket.ClientSnapshot {
	return registry.clients
}

func (registry *fakeRegistry) Disconnect(client *websocket.Client, code int, reason string) {
	registry.disconnections = append(registry.disconnections, disconnection{client: client, code: code, reason: reason})
}

type fakeValidator struct {
	validations map[string]actions.TokenValidation
	err         error
	calls       []string
}

func (validator *fakeValidator) ValidateToken(_ context.Context, token string) (actions.TokenValidation, error) {
	validator.calls = append(validator.calls, token)

	if validator.err != nil {
		return actions.TokenValidation{}, validator.err
	}

	return validator.validations[token], nil
}

func snapshotFor(userUUID, token string) websocket.ClientSnapshot {
	return websocket.ClientSnapshot{Client: &websocket.Client{}, UserUUID: userUUID, Token: token}
}

func TestRevalidateTokens(test *testing.T) {
	test.Run("it should disconnect clients whose token is invalid with close code 4401", func(test *testing.T) {
		registry := &fakeRegistry{clients: []websocket.ClientSnapshot{snapshotFor("user-1", "stale-token")}}
		validator := &fakeValidator{validations: map[string]actions.TokenValidation{
			"stale-token": {Valid: false, Reason: "EXPIRED"},
		}}

		err := actions.NewRevalidateTokens(registry, validator).Execute(context.Background())
		assert.NoError(test, err)

		if assert.Len(test, registry.disconnections, 1) {
			assert.Equal(test, registry.clients[0].Client, registry.disconnections[0].client)
			assert.Equal(test, actions.CloseCodeInvalidToken, registry.disconnections[0].code)
			assert.Equal(test, "EXPIRED", registry.disconnections[0].reason)
		}
	})

	test.Run("it should keep clients whose token is valid", func(test *testing.T) {
		registry := &fakeRegistry{clients: []websocket.ClientSnapshot{snapshotFor("user-1", "good-token")}}
		validator := &fakeValidator{validations: map[string]actions.TokenValidation{
			"good-token": {Valid: true},
		}}

		err := actions.NewRevalidateTokens(registry, validator).Execute(context.Background())
		assert.NoError(test, err)
		assert.Empty(test, registry.disconnections)
	})

	test.Run("it should fail open and keep clients when the validator errors", func(test *testing.T) {
		registry := &fakeRegistry{clients: []websocket.ClientSnapshot{snapshotFor("user-1", "some-token")}}
		validator := &fakeValidator{err: errors.New("auth unreachable")}

		err := actions.NewRevalidateTokens(registry, validator).Execute(context.Background())
		assert.NoError(test, err)
		assert.Empty(test, registry.disconnections)
	})

	test.Run("it should validate a token shared by several clients only once and disconnect all of them", func(test *testing.T) {
		registry := &fakeRegistry{clients: []websocket.ClientSnapshot{
			snapshotFor("user-1", "shared-token"),
			snapshotFor("user-1", "shared-token"),
		}}
		validator := &fakeValidator{validations: map[string]actions.TokenValidation{
			"shared-token": {Valid: false, Reason: "EXPIRED"},
		}}

		err := actions.NewRevalidateTokens(registry, validator).Execute(context.Background())
		assert.NoError(test, err)
		assert.Len(test, validator.calls, 1)
		assert.Len(test, registry.disconnections, 2)
	})

	test.Run("it should fail closed on clients without a token, without asking auth", func(test *testing.T) {
		registry := &fakeRegistry{clients: []websocket.ClientSnapshot{snapshotFor("user-1", "")}}
		validator := &fakeValidator{}

		err := actions.NewRevalidateTokens(registry, validator).Execute(context.Background())
		assert.NoError(test, err)
		assert.Empty(test, validator.calls)

		if assert.Len(test, registry.disconnections, 1) {
			assert.Equal(test, actions.CloseCodeInvalidToken, registry.disconnections[0].code)
		}
	})

	test.Run("it should do nothing when there are no connections", func(test *testing.T) {
		registry := &fakeRegistry{}
		validator := &fakeValidator{}

		err := actions.NewRevalidateTokens(registry, validator).Execute(context.Background())
		assert.NoError(test, err)
		assert.Empty(test, validator.calls)
		assert.Empty(test, registry.disconnections)
	})

	test.Run("it should stop and return the context error when canceled", func(test *testing.T) {
		registry := &fakeRegistry{clients: []websocket.ClientSnapshot{snapshotFor("user-1", "some-token")}}
		validator := &fakeValidator{}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := actions.NewRevalidateTokens(registry, validator).Execute(ctx)
		assert.ErrorIs(test, err, context.Canceled)
		assert.Empty(test, validator.calls)
		assert.Empty(test, registry.disconnections)
	})
}
