package blobstore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

func TestLocalCopyFix(t *testing.T) {
	t.Run("opposite", func(t *testing.T) {
		ctx := context.Background()
		s := &LocalBlobStore{Root: t.TempDir()}
		a, b := crossoverPair(t, s)
		_, err := s.UploadBytesIfMatch(ctx, a, []byte("A"), "")
		require.NoError(t, err)
		_, err = s.UploadBytesIfMatch(ctx, b, []byte("B"), "")
		require.NoError(t, err)
		errs := make([]error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, errs[0] = s.Copy(ctx, a, b, CopyOptions{}) }()
		go func() { defer wg.Done(); _, errs[1] = s.Copy(ctx, b, a, CopyOptions{}) }()
		wg.Wait()
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
	})

	t.Run("same_stripe", func(t *testing.T) {
		ctx := context.Background()
		s := &LocalBlobStore{Root: t.TempDir()}
		a, b := sameStripePair(t, s)
		_, err := s.UploadBytesIfMatch(ctx, a, []byte("A"), "")
		require.NoError(t, err)
		_, err = s.UploadBytesIfMatch(ctx, b, []byte("B"), "")
		require.NoError(t, err)
		errs := make([]error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, errs[0] = s.Copy(ctx, a, b, CopyOptions{}) }()
		go func() { defer wg.Done(); _, errs[1] = s.Copy(ctx, b, a, CopyOptions{}) }()
		wg.Wait()
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
	})
}

// crossoverPair finds keys whose string order disagrees with stripe order:
// the exact shape that deadlocked when Copy ordered locks by key string.
func crossoverPair(t *testing.T, s *LocalBlobStore) (string, string) {
	t.Helper()
	for i := 0; i < 3000; i++ {
		for j := i + 1; j < 3000; j++ {
			a := fmt.Sprintf("key-%05d", i)
			b := fmt.Sprintf("key-%05d", j)
			ia, ib := s.lockIndexForKey(a), s.lockIndexForKey(b)
			if ia == ib {
				continue
			}
			if (a > b) != (ia > ib) {
				return a, b
			}
		}
	}
	t.Fatal("no stripe/string crossover pair found")
	return "", ""
}

func sameStripePair(t *testing.T, s *LocalBlobStore) (string, string) {
	t.Helper()
	seen := map[uint32]string{}
	for i := 0; i < 5000; i++ {
		a := fmt.Sprintf("stripe-%05d", i)
		idx := s.lockIndexForKey(a)
		if prev, ok := seen[idx]; ok && prev != a {
			return prev, a
		}
		seen[idx] = a
	}
	t.Fatal("no same-stripe pair found")
	return "", ""
}

func TestIsS3NotFoundFix(t *testing.T) {
	t.Run("typed", func(t *testing.T) {
		require.True(t, isS3NotFound(&types.NotFound{Message: strPtr("x")}))
		require.True(t, isS3NotFound(&types.NoSuchKey{Message: strPtr("x")}))
	})

	t.Run("generic_404", func(t *testing.T) {
		err := &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 404}},
			Err:      errors.New("not found"),
		}
		require.True(t, isS3NotFound(err), "untyped CopyObject 404 must map to not-found")
	})

	t.Run("other", func(t *testing.T) {
		err412 := &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 412}},
			Err:      errors.New("precondition failed"),
		}
		require.False(t, isS3NotFound(err412))
		err500 := &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 500}},
			Err:      errors.New("boom"),
		}
		require.False(t, isS3NotFound(err500))
		require.False(t, isS3NotFound(nil))
	})
}

func strPtr(s string) *string { return &s }
