package powerplant

import "testing"

func TestLoad(t *testing.T) {
	_, err := Load()
	if err != nil {
		t.Fatal(err)
	}
}
