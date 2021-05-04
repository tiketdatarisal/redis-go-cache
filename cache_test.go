package cache

import "testing"

var c = NewCache("localhost:6379")

func TestCache_Ping(t *testing.T) {
	if err := c.Ping(); err != nil {
		t.Errorf("failed to ping redis")
	}
}

func TestCache_SetAndGet(t *testing.T) {
	stringKey := "string_key"
	want := "Some random string"
	if err := c.Set(stringKey, want); err != nil {
		t.Fatalf("failed to set value")
	}

	if got, err := c.GetString(stringKey); err != nil {
		t.Fatalf("failed to get value")
	} else if got != want {
		t.Errorf("want %q but got %q", want, got)
	}
}

func TestCache_SetAndGetWithNamespace(t *testing.T) {
	namespace := "SECRET-NAMESPACE"
	stringKey := "string_key"
	want := "Another random string"
	if err := c.Set(stringKey, want, namespace); err != nil {
		t.Fatalf("failed to set value")
	}

	if got, err := c.GetString(stringKey, namespace); err != nil {
		t.Fatalf("failed to get value")
	} else if got != want {
		t.Errorf("want %q but got %q", want, got)
	}
}
