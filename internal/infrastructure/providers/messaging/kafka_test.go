package messaging

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type noopHandler struct{}

func (noopHandler) Handle(_ []byte) error { return nil }

func TestRegisterRejectsMismatchedGroupID(test *testing.T) {
	consumer := NewKafkaConsumer("kafka:9092", 60000, 1)

	assert.NoError(test, consumer.Register("service.a", "", "", "topic.one", noopHandler{}))
	assert.NoError(test, consumer.Register("service.a", "", "", "topic.two", noopHandler{}), "same group id on a second topic must be accepted")

	err := consumer.Register("service.b", "", "", "topic.three", noopHandler{})

	if assert.Error(test, err, "a different group id must be rejected instead of silently ignored") {
		assert.Contains(test, err.Error(), "mismatched group id")
	}

	assert.Len(test, consumer.entries, 2, "the rejected registration must not be added")
}
