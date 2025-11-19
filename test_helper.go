//go:build test
// +build test

package entitystore

func SetClientForTest(c DatastoreClient) {
	client = c
}
