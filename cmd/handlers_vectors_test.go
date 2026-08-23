package cmd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func TestFetchEmptyKB(t *testing.T) {
	for _, fetchErr := range []error{kb.ErrManifestNotFound, kb.ErrKBUninitialized} {
		e := echo.New()
		registerVectorRoutes(e, Dependencies{
			FetchVectors: func(context.Context, string, []string) ([]kb.VectorRecord, error) {
				return nil, fetchErr
			},
		}, nil)
		req := httptest.NewRequest(
			http.MethodPost, "/v1/vectors/fetch",
			strings.NewReader(`{"kb_id":"empty","ids":["missing"]}`),
		)
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		recorder := httptest.NewRecorder()

		e.ServeHTTP(recorder, req)

		require.Equal(t, http.StatusOK, recorder.Code)
		require.JSONEq(t, `{"records":[]}`, recorder.Body.String())
	}
}
