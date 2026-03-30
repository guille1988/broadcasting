package dtos

// UserLoggedIn is the payload published by the auth service when a user logs in.
type UserLoggedIn struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}
