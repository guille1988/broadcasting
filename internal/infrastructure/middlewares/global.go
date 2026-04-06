package middlewares

import (
	"github.com/gin-gonic/gin"
)

func RegisterMiddlewares(engine *gin.Engine) {
	engine.Use(gin.Recovery())
	engine.Use(Logger())
}
