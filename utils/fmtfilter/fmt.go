package fmtfilter

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

func parseScanfOps(template string) (string, error) {
	readOp := false
	ops := ""
	for i, ch := range template {
		if readOp {
			if ch == 'd' {
				ops += "%d"
			} else if ch == 's' {
				ops += "%s"
			} else if ch == '%' {
			} else if !unicode.IsLetter(ch) {
				continue
			} else {
				return "", fmt.Errorf("unexpected op in position %d for template '%s'", i, template)
			}
		}
		readOp = !readOp && ch == '%'
	}
	if readOp {
		return "", fmt.Errorf("non-closed %% in template '%s'", template)
	}
	return ops, nil
}

func CompileFilter(scanfTemplate, printfTemplate string) (func(req string) (string, error), error) {
	ops, err := parseScanfOps(scanfTemplate)
	if err != nil {
		return nil, err
	}
	printfOps, err := parseScanfOps(printfTemplate)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(ops, printfOps) {
		return nil, fmt.Errorf("template ops for scanf don't match scanf ops: '%s' != '%s'", ops, printfOps)
	}

	if ops == "" {
		return func(req string) (string, error) {
			if req == scanfTemplate {
				return printfTemplate, nil
			}
			return "", errors.New("doesn't match template")
		}, nil
	} else if ops == "%d" {
		return func(req string) (string, error) {
			var v1 int64
			if _, err := fmt.Sscanf(req, scanfTemplate, &v1); err != nil {
				return "", err
			}
			return fmt.Sprintf(printfTemplate, v1), nil
		}, nil
	} else if ops == "%s" {
		return func(req string) (string, error) {
			var v1 string
			if _, err := fmt.Sscanf(req, scanfTemplate, &v1); err != nil {
				return "", err
			}
			return fmt.Sprintf(printfTemplate, v1), nil
		}, nil
	} else if ops == "%d%d" {
		return func(req string) (string, error) {
			var v1 int64
			var v2 int64
			if _, err := fmt.Sscanf(req, scanfTemplate, &v1, &v2); err != nil {
				return "", err
			}
			return fmt.Sprintf(printfTemplate, v1, v2), nil
		}, nil
	} else if ops == "%d%s" {
		return func(req string) (string, error) {
			var v1 int64
			var v2 string
			if _, err := fmt.Sscanf(req, scanfTemplate, &v1, &v2); err != nil {
				return "", err
			}
			return fmt.Sprintf(printfTemplate, v1, v2), nil
		}, nil
	} else if ops == "%s%d" {
		return func(req string) (string, error) {
			var v1 string
			var v2 int64
			if _, err := fmt.Sscanf(req, scanfTemplate, &v1, &v2); err != nil {
				return "", err
			}
			return fmt.Sprintf(printfTemplate, v1, v2), nil
		}, nil
	} else if ops == "%s%s" {
		return func(req string) (string, error) {
			var v1 string
			var v2 string
			if _, err := fmt.Sscanf(req, scanfTemplate, &v1, &v2); err != nil {
				return "", err
			}
			return fmt.Sprintf(printfTemplate, v1, v2), nil
		}, nil
	} else {
		return nil, errors.New("unknown combination of scanf operations")
	}
}
