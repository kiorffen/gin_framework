package handler

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

type ProcessFunc func(ctx *gin.Context)

type RootHandler struct{}

func (h *RootHandler) Before(ctx *gin.Context) {
	fmt.Println("before")
}

func (h *RootHandler) After(ctx *gin.Context) {
	fmt.Println("after")
}

func (h *RootHandler) Run(ctx *gin.Context, processFuncs ...ProcessFunc) {
	h.Before(ctx)

	for _, f := range processFuncs {
		f(ctx)
	}

	h.After(ctx)
}

func (h *RootHandler) OutJson(ctx *gin.Context, code int, msg string, data interface{}) {
	res := make(map[string]interface{})
	res["code"] = code
	res["message"] = msg
	res["data"] = data

	ctx.JSON(200, res)
}
