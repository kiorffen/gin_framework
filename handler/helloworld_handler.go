package handler

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

type HelloworldHandler struct {
	RootHandler

	id   string
	name string
}

func (h *HelloworldHandler) Process(ctx *gin.Context) {
	h.id = ctx.Query("id")

	var postData map[string]string
	ctx.BindJSON(&postData)
	h.name = postData["name"]

	fmt.Printf("id: %s, name:%s\n", h.id, h.name)

	res := make(map[string]interface{})
	res["info"] = "helloworld"

	h.OutJson(ctx, 0, "OK", res)
}

func Helloworld(ctx *gin.Context) {
	helloworld := &HelloworldHandler{}
	helloworld.Run(ctx, helloworld.Process)
}
