package cmd

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func registerCacheRoutes(e *echo.Echo, deps Dependencies) {
	e.POST("/cache/sweep", func(c echo.Context) error {
		if deps.SweepCache == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: errKBUnavailable})
		}
		if err := deps.SweepCache(c.Request().Context()); err != nil {
			return WriteError(c, err, deps.IsBudgetExceeded)
		}
		return c.JSON(http.StatusOK, map[string]any{"status": "ok"})
	})
}
