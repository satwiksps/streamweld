package proxy

import "strings"

// safeLogString keeps untrusted values on a single physical log record while
// preserving line-break information for operators.
func safeLogString(value string) string {
	value = strings.ReplaceAll(value, "\r", `\r`)
	return strings.ReplaceAll(value, "\n", `\n`)
}

func safeLogError(err error) string {
	if err == nil {
		return "<nil>"
	}
	return safeLogString(err.Error())
}
