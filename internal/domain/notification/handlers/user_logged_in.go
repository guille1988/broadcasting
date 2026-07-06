package handlers

import (
	"broadcasting/internal/domain/notification/actions"
	"encoding/json"
	"fmt"

	"github.com/guille1988/go-app-shared/messaging/kafka/dtos"
)

// UserLoggedIn handles the user.logged_in RabbitMQ event.
type UserLoggedIn struct {
	action *actions.BroadcastLogin
}

// NewUserLoggedIn creates a UserLoggedIn handler wired to the given action.
func NewUserLoggedIn(action *actions.BroadcastLogin) *UserLoggedIn {
	return &UserLoggedIn{action: action}
}

// Handle unmarshals the message body and delegates to the broadcast action.
func (handler *UserLoggedIn) Handle(body []byte) error {
	var dto dtos.UserLoggedIn

	if err := json.Unmarshal(body, &dto); err != nil {
		return fmt.Errorf("failed to unmarshal user_logged_in dto: %w", err)
	}

	return handler.action.Execute(dto.UUID, dto.Name)
}
