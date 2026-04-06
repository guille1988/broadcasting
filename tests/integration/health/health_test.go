package health_test

import (
	"broadcasting/tests/integration"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHealthModule(test *testing.T) {
	integration.TestCase(test, "it should return ok", func(test *testing.T) {
		request, _ := http.NewRequest("GET", "/api/health", nil)
		response := integration.ExecuteRequest(request)

		assert.Equal(test, http.StatusOK, response.Code)
	})
}
