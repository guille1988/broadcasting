package integration

import (
	"broadcasting/internal/bootstrap"
	"broadcasting/internal/infrastructure/app"
	"broadcasting/internal/infrastructure/config"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

var TestApp *app.App
var TestHandler http.Handler
var TestConfig *config.Config
var TestServer *httptest.Server

// RunTests handles the integration tests setup, execution, and cleanup.
func RunTests(test *testing.M) {
	var err error

	TestConfig, err = config.New()
	if err != nil {
		panic(err)
	}

	TestApp, err = bootstrap.NewTestingApp(TestConfig)
	if err != nil {
		panic(err)
	}

	TestHandler = bootstrap.NewTestingHandler(TestApp)
	TestServer = httptest.NewServer(TestHandler)
	defer TestServer.Close()

	code := test.Run()
	os.Exit(code)
}

// ExecuteRequest performs an HTTP request against the global test handler and returns the response recorder.
func ExecuteRequest(request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	TestHandler.ServeHTTP(recorder, request)

	return recorder
}

// TestCase is a helper that runs the given function as a named sub-test.
func TestCase(test *testing.T, name string, testFunction func(test *testing.T)) {
	test.Run(name, func(test *testing.T) {
		testFunction(test)
	})
}
