package cmd

import (
	"embed"
	"net/http"

	"github.com/labstack/echo/v4"
)

//go:embed ui.html
var uiFS embed.FS

func RegisterUI(e *echo.Echo) {
	e.GET("/", func(c echo.Context) error {
		content, err := uiFS.ReadFile("ui.html")
		if err != nil {
			return c.NoContent(http.StatusInternalServerError)
		}
		return c.Blob(http.StatusOK, "text/html; charset=utf-8", content)
	})
}
