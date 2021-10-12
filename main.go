package main

import (
	"gin_framework/global"
	"gin_framework/handler"

	"github.com/fvbock/endless"
	"github.com/gin-gonic/gin"
)

func main() {
	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()

	router.POST("/helloworld", handler.Helloworld)
	router.POST("/search", handler.Search)

	endless.ListenAndServe(":"+global.G_conf.ServerPort, router)
}
