package clickhouse

// Escape escapes special symbols
func Escape(line string) string {
	result := ""

	length := len(line)
	for i := 0; i < length; i++ {
		char := line[i: i+1]

		switch char {
		case "\b":
			result += "\\b"
		case "\f":
			result += "\\f"
		case "\r":
			result += "\\r"
		case "\n":
			result += "\\n"
		case "\t":
			result += "\\t"
		case `''`:
			result += `\'`
		case `\`:
			result += `\\`
		case `/`:
			result += `\/`
		case `-`:
			result += `\-`
		default:
			result += string(char)
		}
	}

	return result
}

// Unescape undoes escaping of special symbols
func Unescape(line string) string {
	result := ""

	length := len(line)
	for i := 0; i < length; i += 2 {
		if i >= length-1 {
			result += line[i: i+1]
			break
		}

		pair := line[i: i+2]

		switch pair {
		case "\\b":
			result += "\b"
		case "\\f":
			result += "\f"
		case "\\r":
			result += "\r"
		case "\\n":
			result += "\n"
		case "\\t":
			result += "\t"
		case `\'`:
			result += `'`
		case `\\`:
			result += `\`
		case `\/`:
			result += `/`
		case `\-`:
			result += `-`
		default:
			result += line[i: i+1]
			i--
		}
	}

	return result
}
