package main

import (
	"os"

	gwcfg "agy-acp-gateway-go/internal/config"
)

func redactSecretsInString(s string) string { return gwcfg.RedactSecretsInString(s) }

func redactURLSecretsForError(s string) string { return gwcfg.RedactURLSecretsForError(s) }

func redactLogValue(v any) any { return gwcfg.RedactLogValue(v) }

func redactLogJSON(v any) ([]byte, error) { return gwcfg.RedactLogJSON(v) }

func openPrivateLog(path string) (*os.File, error) {
	return gwcfg.OpenPrivateLog(path, ProjectHome)
}
