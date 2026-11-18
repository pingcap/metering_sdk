package utils

import "testing"

func TestValidateSelfID(t *testing.T) {
	valid := []string{"tidbserver", "pd0", "selfID123"}
	for _, v := range valid {
		if err := ValidateSelfID(v); err != nil {
			t.Errorf("ValidateSelfID(%q) unexpected error: %v", v, err)
		}
	}

	invalid := []string{"", "self-id", "a/b", "a\\b", "..", "a..b", "../x"}
	for _, v := range invalid {
		if err := ValidateSelfID(v); err == nil {
			t.Errorf("ValidateSelfID(%q) expected error, got nil", v)
		}
	}
}

func TestValidateSharedPoolID(t *testing.T) {
	if err := ValidateSharedPoolID("pool123"); err != nil {
		t.Errorf("ValidateSharedPoolID(valid) unexpected error: %v", err)
	}
	invalid := []string{"", "a/b", "a\\b", "..", "pool/..", "a..b"}
	for _, v := range invalid {
		if err := ValidateSharedPoolID(v); err == nil {
			t.Errorf("ValidateSharedPoolID(%q) expected error, got nil", v)
		}
	}
}

func TestValidateCategoryRejectsTraversal(t *testing.T) {
	if err := ValidateCategory("storage-size"); err != nil {
		t.Errorf("ValidateCategory(valid) unexpected error: %v", err)
	}
	for _, v := range []string{"", "a/b", "..", "a..b"} {
		if err := ValidateCategory(v); err == nil {
			t.Errorf("ValidateCategory(%q) expected error, got nil", v)
		}
	}
}
