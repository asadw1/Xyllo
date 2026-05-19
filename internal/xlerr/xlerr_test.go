package xlerr

import (
	"errors"
	"strings"
	"testing"
)

var errBase = errors.New("underlying failure")

// ── PipelineError.Error() ────────────────────────────────────────────────────

func TestPipelineError_Error_IncludesStageCodeAndMessage(t *testing.T) {
	pe := New(StageValidation, CodeValidation, "", errBase)
	got := pe.Error()
	if !strings.Contains(got, string(StageValidation)) {
		t.Errorf("Error() should contain stage %q, got: %q", StageValidation, got)
	}
	if !strings.Contains(got, string(CodeValidation)) {
		t.Errorf("Error() should contain code %q, got: %q", CodeValidation, got)
	}
	if !strings.Contains(got, errBase.Error()) {
		t.Errorf("Error() should contain underlying message, got: %q", got)
	}
}

func TestPipelineError_Error_IncludesSourceWhenSet(t *testing.T) {
	pe := New(StageDispatch, CodePanic, "web-sdk", errBase)
	got := pe.Error()
	if !strings.Contains(got, "web-sdk") {
		t.Errorf("Error() should contain source %q, got: %q", "web-sdk", got)
	}
}

func TestPipelineError_Error_OmitsSourceWhenEmpty(t *testing.T) {
	pe := New(StageDLQ, CodeDLQWrite, "", errBase)
	got := pe.Error()
	// Should not contain an empty source= fragment that would confuse log parsers.
	if strings.Contains(got, `source=""`) {
		t.Errorf("Error() should not include empty source, got: %q", got)
	}
}

// ── Unwrap / errors.Is ───────────────────────────────────────────────────────

func TestPipelineError_Unwrap_AllowsErrorsIs(t *testing.T) {
	pe := New(StageValidation, CodeValidation, "src", errBase)
	if !errors.Is(pe, errBase) {
		t.Error("errors.Is should find the wrapped error via Unwrap")
	}
}

func TestPipelineError_Unwrap_AllowsErrorsAs(t *testing.T) {
	inner := &PipelineError{Stage: StageIngest, Code: CodeTranslation, Err: errBase}
	outer := New(StageDispatch, CodeValidation, "src", inner)

	var got *PipelineError
	if !errors.As(outer, &got) {
		t.Fatal("errors.As should find *PipelineError via Unwrap")
	}
	// errors.As returns the outermost match — the outer PipelineError itself.
	if got.Stage != StageDispatch {
		t.Errorf("errors.As Stage: want %q, got %q", StageDispatch, got.Stage)
	}
}

// ── Wrap ─────────────────────────────────────────────────────────────────────

func TestWrap_ReturnsNilForNilError(t *testing.T) {
	if got := Wrap(nil, StageValidation, CodeValidation, "src"); got != nil {
		t.Errorf("Wrap(nil) should return nil, got: %v", got)
	}
}

func TestWrap_WrapsNonNilError(t *testing.T) {
	wrapped := Wrap(errBase, StageDispatch, CodePanic, "mobile")
	if wrapped == nil {
		t.Fatal("Wrap(non-nil) should not return nil")
	}
	if !errors.Is(wrapped, errBase) {
		t.Error("wrapped error should satisfy errors.Is for the original error")
	}
}

// ── Stage and Code constants ─────────────────────────────────────────────────

func TestConstants_StagesAreNonEmpty(t *testing.T) {
	stages := []Stage{StageIngest, StageValidation, StageDispatch, StageDLQ}
	for _, s := range stages {
		if s == "" {
			t.Errorf("Stage constant must not be empty")
		}
	}
}

func TestConstants_CodesAreNonEmpty(t *testing.T) {
	codes := []Code{CodeValidation, CodeTranslation, CodeBufferFull, CodePanic, CodeDLQWrite, CodeSerialization}
	for _, c := range codes {
		if c == "" {
			t.Errorf("Code constant must not be empty")
		}
	}
}
