package cmd

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/mikills/minnow/kb"

	"github.com/labstack/echo/v4"
)

func registerMediaRoutes(e *echo.Echo, deps Dependencies) {
	e.POST("/rag/media/upload", mediaUploadHandler(deps))
	e.GET("/rag/media/list", mediaListHandler(deps))
	e.GET("/rag/media/:id", mediaGetHandler(deps))
}

func mediaUploadHandler(deps Dependencies) echo.HandlerFunc {
	return func(c echo.Context) error { return handleMediaUpload(c, deps) }
}

func mediaListHandler(deps Dependencies) echo.HandlerFunc {
	return func(c echo.Context) error { return handleMediaList(c, deps) }
}

func mediaGetHandler(deps Dependencies) echo.HandlerFunc {
	return func(c echo.Context) error { return handleMediaGet(c, deps) }
}

func handleMediaUpload(c echo.Context, deps Dependencies) error {
	if deps.AppendMediaUpload == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: "media subsystem not configured"})
	}
	if deps.MaxMediaBytes > 0 && c.Request().ContentLength > deps.MaxMediaBytes {
		return c.JSON(
			http.StatusRequestEntityTooLarge,
			map[string]any{errorResponseKey: "upload exceeds maximum allowed size"},
		)
	}
	input, status, err := mediaUploadInputFromRequest(c, deps.MaxMediaBytes)
	if err != nil {
		return c.JSON(status, map[string]any{errorResponseKey: err.Error()})
	}
	_, corr := requestIDs(c)
	evtID, effectiveIdem, err := deps.AppendMediaUpload(
		c.Request().Context(),
		input,
		deps.MaxMediaBytes,
		input.IdempotencyKey,
		corr,
	)
	if err != nil {
		deps.AppMetrics.RecordMediaUpload(input.KBID, input.ContentType, 0, err)
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: err.Error()})
	}
	deps.AppMetrics.RecordMediaUpload(input.KBID, input.ContentType, 0, nil)
	return writeAcceptedOperation(
		c,
		evtID,
		effectiveIdem,
		map[string]any{
			eventIDResponseKey: evtID,
			"status_url":       "/rag/operations/" + evtID,
			kbIDContextKey:     input.KBID,
			"filename":         input.Filename,
		},
	)
}

func mediaUploadInputFromRequest(c echo.Context, maxBytes int64) (kb.MediaUploadInput, int, error) {
	kbID := strings.TrimSpace(c.FormValue(kbIDContextKey))
	if kbID == "" {
		kbID = defaultKBIDValue
	}
	file, err := c.FormFile("file")
	if err != nil {
		return kb.MediaUploadInput{}, http.StatusBadRequest, errors.New("file form field required")
	}
	if maxBytes > 0 && file.Size > maxBytes {
		return kb.MediaUploadInput{}, http.StatusRequestEntityTooLarge, errBodyTooLarge
	}
	src, err := file.Open()
	if err != nil {
		return kb.MediaUploadInput{}, http.StatusBadRequest, errors.New("cannot open uploaded file")
	}
	defer src.Close()
	body, err := readBoundedBody(src, maxBytes)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			return kb.MediaUploadInput{}, http.StatusRequestEntityTooLarge, errBodyTooLarge
		}
		return kb.MediaUploadInput{}, http.StatusBadRequest, errors.New("cannot read uploaded file")
	}
	input := kb.MediaUploadInput{
		KBID:           kbID,
		Filename:       file.Filename,
		ContentType:    strings.TrimSpace(c.FormValue("content_type")),
		Source:         strings.TrimSpace(c.FormValue("source")),
		Title:          strings.TrimSpace(c.FormValue("title")),
		UploadedBy:     strings.TrimSpace(c.FormValue("uploaded_by")),
		IdempotencyKey: strings.TrimSpace(c.Request().Header.Get("Idempotency-Key")),
		Body:           body,
	}
	if input.ContentType == "" {
		input.ContentType = file.Header.Get("Content-Type")
	}
	return input, http.StatusOK, nil
}

func handleMediaList(c echo.Context, deps Dependencies) error {
	if deps.ListMedia == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: "media subsystem not configured"})
	}
	kbID := strings.TrimSpace(c.QueryParam(kbIDContextKey))
	if kbID == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "kb_id required"})
	}
	prefix := strings.TrimSpace(c.QueryParam("prefix"))
	after := strings.TrimSpace(c.QueryParam("after"))
	limit := parsePositiveInt(c.QueryParam("limit"), 500)
	page, err := deps.ListMedia(c.Request().Context(), kbID, prefix, after, limit)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{errorResponseKey: err.Error()})
	}
	return c.JSON(http.StatusOK, map[string]any{"items": page.Items, "limit": limit, "next": page.NextToken})
}

func handleMediaGet(c echo.Context, deps Dependencies) error {
	if deps.GetMedia == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{errorResponseKey: "media subsystem not configured"})
	}
	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{errorResponseKey: "id required"})
	}
	m, err := deps.GetMedia(c.Request().Context(), id)
	if err != nil {
		if errors.Is(err, kb.ErrMediaNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{errorResponseKey: "not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]any{errorResponseKey: err.Error()})
	}
	return c.JSON(http.StatusOK, m)
}

var errBodyTooLarge = errors.New("upload exceeds maximum allowed size")

// readBoundedBody reads from src into memory, refusing to return more than
// maxBytes bytes. A maxBytes<=0 disables the cap. Clients can lie about
// Content-Length or use chunked transfer, so we read maxBytes+1 and fail if
// the extra byte appears.
func readBoundedBody(src io.Reader, maxBytes int64) (io.Reader, error) {
	if maxBytes <= 0 {
		data, err := io.ReadAll(src)
		if err != nil {
			return nil, err
		}
		return bytesReaderFromSlice(data), nil
	}
	limited := io.LimitReader(src, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, errBodyTooLarge
	}
	return bytesReaderFromSlice(data), nil
}

func bytesReaderFromSlice(data []byte) io.Reader {
	return &bytesReader{data: data}
}

type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
