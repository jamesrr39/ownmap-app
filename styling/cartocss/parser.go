package styling

import (
	"image/color"
	"regexp"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/styling"
)

var (
	pseudoSelectorName = regexp.MustCompile(`::(\w+)`)
)

func Parse(stylesheet string) (*Style, errorsx.Error) {
	style := &Style{
		variables: make(map[string]string),
	}

	var currentStatement string
	styleSheetLen := len(stylesheet)
	var nextChar rune
	var statementBlockWrappers []string
	for i := 0; i < styleSheetLen; i++ {
		thisChar := rune(stylesheet[i])
		hasNextChar := i != styleSheetLen-1
		if hasNextChar {
			// handle 2 character sequences

			nextChar = rune(stylesheet[i+1])
			next2Chars := string(thisChar) + string(nextChar)
			switch next2Chars {
			case TokenOpenBlockComment:
				// skip to end of comment
				i += strings.Index(stylesheet[i:], TokenCloseBlockComment) + 1
				continue
			case TokenOpenLineComment:
				i += strings.Index(stylesheet[i:], string(TokenNewLine))
				continue
			case TokenPseudoSelector:
				matches := pseudoSelectorName.FindAllStringSubmatch(stylesheet[i:], -1)
				pseudoSelectorName := matches[1]
				println(pseudoSelectorName) // TODO

				i += len(matches[0])

			}
		}

		// now we have dealt with 2-char statements, deal with this char
		switch thisChar {
		case TokenOpenBlock:
			statementBlockWrappers = append(statementBlockWrappers, currentStatement)
			currentStatement = ""
		case TokenCloseBlock:
			statementBlockWrappers = statementBlockWrappers[:len(statementBlockWrappers)-1]
			currentStatement = ""
		case TokenEndStatement:
			// process and reset statement
			processStatement(style, currentStatement)
			currentStatement = ""
		case TokenSpace, TokenTab, TokenNewLine:
		// do nothing
		default:
			currentStatement += string(thisChar)
		}
	}

	return style, nil
}

func processStatement(style *Style, statement string) errorsx.Error {
	if strings.HasPrefix(statement, "@") {
		idxColon := strings.Index(statement, ":")
		if idxColon == -1 {
			return errorsx.Errorf("unprocessable line: %q", statement)
		}
		varName := strings.TrimSpace(statement[:idxColon])
		varVal := strings.TrimSpace(statement[idxColon+1:])
		style.variables[strings.TrimPrefix(varName, "@")] = varVal
		return nil
	}

	return errorsx.Errorf("couldn't process statement: %q", statement)
}

func (s *Style) GetWayStyle(tags []*ownmap.OSMTag, ZoomLevel ownmap.ZoomLevel) (*styling.WayStyle, errorsx.Error) {
	var highwayType string
	for _, tag := range tags {
		switch tag.Key {
		case "highway":
			highwayType = tag.Value
		}
	}

	if highwayType == "" {
		return nil, errorsx.Errorf("not a highway")
	}

	// stub

	lineStyle := new(styling.WayStyle)
	switch highwayType {
	case "motorway":
		lineStyle.FillColor = color.RGBA{0xf3, 0x8d, 0x9e, 0xff}
	case "trunk":
		lineStyle.FillColor = color.RGBA{0xff, 0xae, 0x9b, 0xff}
	case "primary", "primary_link":
		lineStyle.FillColor = color.RGBA{0xff, 0xd4, 0xa5, 0xff}
	case "secondary":
		lineStyle.FillColor = color.RGBA{0xf6, 0xf9, 0xbf, 0xff}
	case "tertiary":
		lineStyle.FillColor = color.RGBA{0xf3, 0x8d, 0x9e, 0xff}
	case "unclassified", "residential", "service", "track":
		lineStyle.FillColor = color.RGBA{0xbc, 0xac, 0xa5, 0xff}
	case "footway", "path", "steps":
		lineStyle.FillColor = color.RGBA{0, 0xff, 0, 0xff}
		lineStyle.LineDashPolicy = []float64{1, 2, 3}
	case "bridleway", "cycleway":
		lineStyle.FillColor = color.RGBA{0, 0xff, 0, 0xff}
		lineStyle.LineDashPolicy = []float64{20, 5}
	default:
		return nil, errorsx.Errorf("unhandled highway type: %q", highwayType)
	}

	return lineStyle, nil
}
