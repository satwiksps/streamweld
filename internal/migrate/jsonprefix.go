package migrate

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

const maxJSONPrefixDepth = 10_000

// ErrInvalidJSONPrefix indicates that a structured-response prefix cannot be
// extended into one valid JSON value.
var ErrInvalidJSONPrefix = errors.New("migrate: invalid structured JSON prefix")

type jsonPrefixStatus uint8

const (
	jsonPrefixInvalid jsonPrefixStatus = iota
	jsonPrefixIncomplete
	jsonPrefixComplete
)

// ValidateJSONPrefix verifies that prefix is either one complete JSON value or
// can be extended with zero or more bytes into one complete JSON value.
func ValidateJSONPrefix(prefix []byte) error {
	parser := jsonPrefixParser{data: prefix}
	position := parser.skipWhitespace(0)
	if position == len(prefix) {
		return nil
	}
	position, status := parser.parseValue(position, 0)
	switch status {
	case jsonPrefixIncomplete:
		return nil
	case jsonPrefixComplete:
		if parser.skipWhitespace(position) == len(prefix) {
			return nil
		}
	}
	return fmt.Errorf("%w at byte %d", ErrInvalidJSONPrefix, position)
}

// IsValidJSONPrefix reports whether ValidateJSONPrefix accepts prefix.
func IsValidJSONPrefix(prefix []byte) bool {
	return ValidateJSONPrefix(prefix) == nil
}

type jsonPrefixParser struct {
	data []byte
}

func (parser jsonPrefixParser) parseValue(position, depth int) (int, jsonPrefixStatus) {
	if depth > maxJSONPrefixDepth {
		return position, jsonPrefixInvalid
	}
	position = parser.skipWhitespace(position)
	if position == len(parser.data) {
		return position, jsonPrefixIncomplete
	}
	switch parser.data[position] {
	case '{':
		return parser.parseObject(position, depth+1)
	case '[':
		return parser.parseArray(position, depth+1)
	case '"':
		return parser.parseString(position)
	case 't':
		return parser.parseLiteral(position, "true")
	case 'f':
		return parser.parseLiteral(position, "false")
	case 'n':
		return parser.parseLiteral(position, "null")
	case '-':
		return parser.parseNumber(position)
	default:
		if parser.data[position] >= '0' && parser.data[position] <= '9' {
			return parser.parseNumber(position)
		}
		return position, jsonPrefixInvalid
	}
}

func (parser jsonPrefixParser) parseObject(position, depth int) (int, jsonPrefixStatus) {
	position++
	position = parser.skipWhitespace(position)
	if position == len(parser.data) {
		return position, jsonPrefixIncomplete
	}
	if parser.data[position] == '}' {
		return position + 1, jsonPrefixComplete
	}

	for {
		if parser.data[position] != '"' {
			return position, jsonPrefixInvalid
		}
		var status jsonPrefixStatus
		position, status = parser.parseString(position)
		if status != jsonPrefixComplete {
			return position, status
		}
		position = parser.skipWhitespace(position)
		if position == len(parser.data) {
			return position, jsonPrefixIncomplete
		}
		if parser.data[position] != ':' {
			return position, jsonPrefixInvalid
		}
		position++
		position, status = parser.parseValue(position, depth)
		if status != jsonPrefixComplete {
			return position, status
		}
		position = parser.skipWhitespace(position)
		if position == len(parser.data) {
			return position, jsonPrefixIncomplete
		}
		switch parser.data[position] {
		case '}':
			return position + 1, jsonPrefixComplete
		case ',':
			position = parser.skipWhitespace(position + 1)
			if position == len(parser.data) {
				return position, jsonPrefixIncomplete
			}
			if parser.data[position] == '}' {
				return position, jsonPrefixInvalid
			}
		default:
			return position, jsonPrefixInvalid
		}
	}
}

func (parser jsonPrefixParser) parseArray(position, depth int) (int, jsonPrefixStatus) {
	position++
	position = parser.skipWhitespace(position)
	if position == len(parser.data) {
		return position, jsonPrefixIncomplete
	}
	if parser.data[position] == ']' {
		return position + 1, jsonPrefixComplete
	}

	for {
		var status jsonPrefixStatus
		position, status = parser.parseValue(position, depth)
		if status != jsonPrefixComplete {
			return position, status
		}
		position = parser.skipWhitespace(position)
		if position == len(parser.data) {
			return position, jsonPrefixIncomplete
		}
		switch parser.data[position] {
		case ']':
			return position + 1, jsonPrefixComplete
		case ',':
			position = parser.skipWhitespace(position + 1)
			if position == len(parser.data) {
				return position, jsonPrefixIncomplete
			}
			if parser.data[position] == ']' {
				return position, jsonPrefixInvalid
			}
		default:
			return position, jsonPrefixInvalid
		}
	}
}

func (parser jsonPrefixParser) parseString(position int) (int, jsonPrefixStatus) {
	position++
	for position < len(parser.data) {
		value := parser.data[position]
		switch {
		case value == '"':
			return position + 1, jsonPrefixComplete
		case value == '\\':
			position++
			if position == len(parser.data) {
				return position, jsonPrefixIncomplete
			}
			escape := parser.data[position]
			if escape == 'u' {
				for offset := 1; offset <= 4; offset++ {
					if position+offset >= len(parser.data) {
						return len(parser.data), jsonPrefixIncomplete
					}
					if !isHex(parser.data[position+offset]) {
						return position + offset, jsonPrefixInvalid
					}
				}
				position += 5
				continue
			}
			if !isSimpleJSONEscape(escape) {
				return position, jsonPrefixInvalid
			}
			position++
		case value < 0x20:
			return position, jsonPrefixInvalid
		case value < utf8.RuneSelf:
			position++
		default:
			_, size := utf8.DecodeRune(parser.data[position:])
			if size == 1 {
				if !utf8.FullRune(parser.data[position:]) {
					return len(parser.data), jsonPrefixIncomplete
				}
				return position, jsonPrefixInvalid
			}
			position += size
		}
	}
	return position, jsonPrefixIncomplete
}

func (parser jsonPrefixParser) parseLiteral(position int, literal string) (int, jsonPrefixStatus) {
	for offset := 0; offset < len(literal); offset++ {
		if position+offset == len(parser.data) {
			return len(parser.data), jsonPrefixIncomplete
		}
		if parser.data[position+offset] != literal[offset] {
			return position + offset, jsonPrefixInvalid
		}
	}
	return position + len(literal), jsonPrefixComplete
}

func (parser jsonPrefixParser) parseNumber(position int) (int, jsonPrefixStatus) {
	if parser.data[position] == '-' {
		position++
		if position == len(parser.data) {
			return position, jsonPrefixIncomplete
		}
	}
	if position == len(parser.data) {
		return position, jsonPrefixIncomplete
	}
	switch parser.data[position] {
	case '0':
		position++
	default:
		if parser.data[position] < '1' || parser.data[position] > '9' {
			return position, jsonPrefixInvalid
		}
		for position < len(parser.data) && parser.data[position] >= '0' && parser.data[position] <= '9' {
			position++
		}
	}

	if position < len(parser.data) && parser.data[position] == '.' {
		position++
		if position == len(parser.data) {
			return position, jsonPrefixIncomplete
		}
		if parser.data[position] < '0' || parser.data[position] > '9' {
			return position, jsonPrefixInvalid
		}
		for position < len(parser.data) && parser.data[position] >= '0' && parser.data[position] <= '9' {
			position++
		}
	}

	if position < len(parser.data) && (parser.data[position] == 'e' || parser.data[position] == 'E') {
		position++
		if position == len(parser.data) {
			return position, jsonPrefixIncomplete
		}
		if parser.data[position] == '+' || parser.data[position] == '-' {
			position++
			if position == len(parser.data) {
				return position, jsonPrefixIncomplete
			}
		}
		if parser.data[position] < '0' || parser.data[position] > '9' {
			return position, jsonPrefixInvalid
		}
		for position < len(parser.data) && parser.data[position] >= '0' && parser.data[position] <= '9' {
			position++
		}
	}
	return position, jsonPrefixComplete
}

func (parser jsonPrefixParser) skipWhitespace(position int) int {
	for position < len(parser.data) {
		switch parser.data[position] {
		case ' ', '\t', '\r', '\n':
			position++
		default:
			return position
		}
	}
	return position
}

func isHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
}

func isSimpleJSONEscape(value byte) bool {
	switch value {
	case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
		return true
	default:
		return false
	}
}
