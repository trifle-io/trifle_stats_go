package triflestats

import (
	"fmt"
	"math"
	"strings"
)

type expressionToken struct {
	kind  string
	text  string
	value float64
}

type expressionNode interface{}

type expressionNumber struct {
	value float64
}

type expressionVariable struct {
	name string
}

type expressionUnary struct {
	op   string
	expr expressionNode
}

type expressionBinary struct {
	op    string
	left  expressionNode
	right expressionNode
}

type expressionFunc struct {
	name string
	args []expressionNode
}

type expressionParser struct {
	tokens []expressionToken
	pos    int
	vars   map[string]struct{}
}

func ValidateExpression(paths []string, expression, response string) error {
	_, _, _, err := compileExpression(paths, expression, response)
	return err
}

func (s Series) TransformExpression(paths []string, expression, response string) (Series, error) {
	normalizedPaths, responseSegments, ast, err := compileExpression(paths, expression, response)
	if err != nil {
		return Series{}, err
	}

	values := make([]map[string]any, 0, len(s.Values))
	for _, row := range s.Values {
		if !canCreatePath(row, responseSegments) {
			return Series{}, fmt.Errorf("cannot write to response path %s", strings.Join(responseSegments, "."))
		}

		env := buildExpressionEnv(row, normalizedPaths)
		result, ok, err := evaluateExpression(ast, env)
		if err != nil {
			return Series{}, err
		}

		if ok {
			values = append(values, putPathValue(row, responseSegments, result))
		} else {
			values = append(values, putPathValue(row, responseSegments, nil))
		}
	}

	return Series{At: s.At, Values: values}, nil
}

func compileExpression(paths []string, expression, response string) ([]string, []string, expressionNode, error) {
	normalizedPaths, err := normalizeExpressionPaths(paths)
	if err != nil {
		return nil, nil, nil, err
	}

	trimmedResponse := strings.TrimSpace(response)
	if trimmedResponse == "" {
		return nil, nil, nil, fmt.Errorf("response path is required")
	}

	if containsWildcard(normalizedPaths) || strings.Contains(trimmedResponse, "*") {
		return nil, nil, nil, fmt.Errorf("wildcard paths are not supported yet")
	}

	responseSegments := SplitPath(trimmedResponse)
	if len(responseSegments) == 0 {
		return nil, nil, nil, fmt.Errorf("response path is required")
	}

	tokens, err := tokenizeExpression(expression)
	if err != nil {
		return nil, nil, nil, err
	}

	parser := newExpressionParser(tokens, len(normalizedPaths))
	ast, err := parser.parse()
	if err != nil {
		return nil, nil, nil, err
	}

	return normalizedPaths, responseSegments, ast, nil
}

func normalizeExpressionPaths(paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("at least one path is required")
	}
	if len(paths) > 26 {
		return nil, fmt.Errorf("too many paths. maximum supported is 26")
	}

	normalized := make([]string, 0, len(paths))
	for _, path := range paths {
		trimmed := strings.TrimSpace(path)
		if trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}

	if len(normalized) == 0 {
		return nil, fmt.Errorf("at least one path is required")
	}

	return normalized, nil
}

func containsWildcard(paths []string) bool {
	for _, path := range paths {
		if strings.Contains(path, "*") {
			return true
		}
	}
	return false
}

func tokenizeExpression(expression string) ([]expressionToken, error) {
	input := strings.TrimSpace(expression)
	if input == "" {
		return nil, fmt.Errorf("expression must be text")
	}

	tokens := []expressionToken{}
	for i := 0; i < len(input); {
		ch := input[i]

		switch {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r':
			i++
		case strings.ContainsRune("+-*/(),", rune(ch)):
			tokens = append(tokens, expressionToken{kind: string(ch), text: string(ch)})
			i++
		case ch >= '0' && ch <= '9':
			start := i
			for i < len(input) && input[i] >= '0' && input[i] <= '9' {
				i++
			}
			if i < len(input) && input[i] == '.' {
				i++
				for i < len(input) && input[i] >= '0' && input[i] <= '9' {
					i++
				}
			}
			number, ok := parseNumericString(input[start:i])
			if !ok {
				return nil, fmt.Errorf("invalid token at position %d", start)
			}
			tokens = append(tokens, expressionToken{kind: "number", text: input[start:i], value: number})
		case (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_':
			start := i
			for i < len(input) {
				c := input[i]
				if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
					i++
					continue
				}
				break
			}
			tokens = append(tokens, expressionToken{kind: "ident", text: input[start:i]})
		default:
			return nil, fmt.Errorf("invalid token at position %d", i)
		}
	}

	return tokens, nil
}

func newExpressionParser(tokens []expressionToken, pathCount int) *expressionParser {
	vars := map[string]struct{}{}
	for i := 0; i < pathCount; i++ {
		vars[string(rune('a'+i))] = struct{}{}
	}

	return &expressionParser{tokens: tokens, vars: vars}
}

func (p *expressionParser) parse() (expressionNode, error) {
	node, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	if p.current() != nil {
		return nil, fmt.Errorf("unexpected token %q", p.current().text)
	}
	return node, nil
}

func (p *expressionParser) parseExpression() (expressionNode, error) {
	node, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	for {
		token := p.current()
		if token == nil || (token.kind != "+" && token.kind != "-") {
			return node, nil
		}
		p.pos++
		right, err := p.parseTerm()
		if err != nil {
			return nil, err
		}
		node = expressionBinary{op: token.kind, left: node, right: right}
	}
}

func (p *expressionParser) parseTerm() (expressionNode, error) {
	node, err := p.parseFactor()
	if err != nil {
		return nil, err
	}

	for {
		token := p.current()
		if token == nil || (token.kind != "*" && token.kind != "/") {
			return node, nil
		}
		p.pos++
		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		node = expressionBinary{op: token.kind, left: node, right: right}
	}
}

func (p *expressionParser) parseFactor() (expressionNode, error) {
	token := p.current()
	if token == nil {
		return nil, fmt.Errorf("unexpected end of expression")
	}

	if token.kind == "+" {
		p.pos++
		return p.parseFactor()
	}
	if token.kind == "-" {
		p.pos++
		node, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		return expressionUnary{op: "-", expr: node}, nil
	}
	if token.kind == "(" {
		p.pos++
		node, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if current := p.current(); current == nil || current.kind != ")" {
			return nil, fmt.Errorf("missing closing parenthesis")
		}
		p.pos++
		return node, nil
	}
	if token.kind == "number" {
		p.pos++
		return expressionNumber{value: token.value}, nil
	}
	if token.kind == "ident" {
		p.pos++
		if current := p.current(); current != nil && current.kind == "(" {
			p.pos++
			args, err := p.parseArgs()
			if err != nil {
				return nil, err
			}
			if current := p.current(); current == nil || current.kind != ")" {
				return nil, fmt.Errorf("missing closing parenthesis")
			}
			p.pos++
			return expressionFunc{name: token.text, args: args}, nil
		}
		if _, ok := p.vars[token.text]; !ok {
			return nil, fmt.Errorf("unknown variable %s", token.text)
		}
		return expressionVariable{name: token.text}, nil
	}

	return nil, fmt.Errorf("unexpected token %q", token.text)
}

func (p *expressionParser) parseArgs() ([]expressionNode, error) {
	if current := p.current(); current != nil && current.kind == ")" {
		return []expressionNode{}, nil
	}

	args := []expressionNode{}
	for {
		arg, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		current := p.current()
		if current == nil || current.kind != "," {
			return args, nil
		}
		p.pos++
	}
}

func (p *expressionParser) current() *expressionToken {
	if p.pos >= len(p.tokens) {
		return nil
	}
	return &p.tokens[p.pos]
}

func buildExpressionEnv(row map[string]any, paths []string) map[string]any {
	env := map[string]any{}
	for index, path := range paths {
		env[string(rune('a'+index))] = fetchPath(row, SplitPath(path))
	}
	return env
}

func evaluateExpression(node expressionNode, env map[string]any) (float64, bool, error) {
	switch typed := node.(type) {
	case expressionNumber:
		return typed.value, true, nil
	case expressionVariable:
		value, ok := toFloat(env[typed.name])
		return value, ok, nil
	case expressionUnary:
		value, ok, err := evaluateExpression(typed.expr, env)
		if err != nil || !ok {
			return 0, ok, err
		}
		if typed.op == "-" {
			return -value, true, nil
		}
		return value, true, nil
	case expressionBinary:
		left, leftOK, err := evaluateExpression(typed.left, env)
		if err != nil || !leftOK {
			return 0, leftOK, err
		}
		right, rightOK, err := evaluateExpression(typed.right, env)
		if err != nil || !rightOK {
			return 0, rightOK, err
		}
		switch typed.op {
		case "+":
			return left + right, true, nil
		case "-":
			return left - right, true, nil
		case "*":
			return left * right, true, nil
		case "/":
			if right == 0 {
				return 0, false, nil
			}
			return left / right, true, nil
		default:
			return 0, false, fmt.Errorf("unknown operator %s", typed.op)
		}
	case expressionFunc:
		values := make([]float64, 0, len(typed.args))
		for _, arg := range typed.args {
			value, ok, err := evaluateExpression(arg, env)
			if err != nil {
				return 0, false, err
			}
			if !ok {
				return 0, false, nil
			}
			values = append(values, value)
		}
		return applyExpressionFunction(typed.name, values)
	default:
		return 0, false, fmt.Errorf("unknown expression node %T", node)
	}
}

func applyExpressionFunction(name string, values []float64) (float64, bool, error) {
	switch name {
	case "sum":
		if len(values) == 0 {
			return 0, false, nil
		}
		sum := 0.0
		for _, value := range values {
			sum += value
		}
		return sum, true, nil
	case "mean", "avg":
		if len(values) == 0 {
			return 0, false, nil
		}
		sum := 0.0
		for _, value := range values {
			sum += value
		}
		return sum / float64(len(values)), true, nil
	case "min":
		if len(values) == 0 {
			return 0, false, nil
		}
		min := values[0]
		for _, value := range values[1:] {
			if value < min {
				min = value
			}
		}
		return min, true, nil
	case "max":
		if len(values) == 0 {
			return 0, false, nil
		}
		max := values[0]
		for _, value := range values[1:] {
			if value > max {
				max = value
			}
		}
		return max, true, nil
	case "sqrt":
		if len(values) != 1 {
			return 0, false, fmt.Errorf("function sqrt expects 1 argument")
		}
		if values[0] < 0 {
			return 0, false, nil
		}
		return math.Sqrt(values[0]), true, nil
	default:
		return 0, false, fmt.Errorf("unknown function %s", name)
	}
}

func canCreatePath(row map[string]any, segments []string) bool {
	if len(segments) <= 1 {
		return true
	}

	current := any(row)
	for _, segment := range segments[:len(segments)-1] {
		node, ok := current.(map[string]any)
		if !ok {
			return false
		}
		value, exists := node[segment]
		if !exists || value == nil {
			return true
		}
		current = value
	}

	return true
}

func putPathValue(row map[string]any, segments []string, value any) map[string]any {
	updated := cloneMap(row)
	target := updated

	for _, segment := range segments[:len(segments)-1] {
		current, ok := target[segment]
		if !ok || current == nil {
			next := map[string]any{}
			target[segment] = next
			target = next
			continue
		}

		existing, ok := current.(map[string]any)
		if !ok {
			next := map[string]any{}
			target[segment] = next
			target = next
			continue
		}

		cloned := cloneMap(existing)
		target[segment] = cloned
		target = cloned
	}

	target[segments[len(segments)-1]] = value
	return updated
}
