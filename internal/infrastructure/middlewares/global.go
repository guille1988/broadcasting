package middlewares

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RegisterMiddlewares(engine *gin.Engine) {
	engine.Use(gin.Recovery())
	engine.Use(Logger())
	engine.GET("/metrics", gin.WrapH(promhttp.Handler()))
}
