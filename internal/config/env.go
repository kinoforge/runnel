package config

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const envPrefix = "RUNNEL"

var durationType = reflect.TypeFor[time.Duration]()

func yamlName(f reflect.StructField) string {
	name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
	return name
}

func applyEnv(v reflect.Value, prefix string, getenv func(string) string) error {
	t := v.Type()
	for i := range t.NumField() {
		f, fv := t.Field(i), v.Field(i)
		name := prefix + "_" + strings.ToUpper(yamlName(f))
		if alias := f.Tag.Get("env"); alias != "" {
			name = alias
		}
		if f.Type.Kind() == reflect.Struct {
			if err := applyEnv(fv, name, getenv); err != nil {
				return err
			}
			continue
		}
		raw := getenv(name)
		if raw == "" {
			continue
		}
		if err := set(fv, raw); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

func set(v reflect.Value, raw string) error {
	switch {
	case v.Type() == durationType:
		d, err := time.ParseDuration(raw)
		if err != nil {
			return err
		}
		v.SetInt(int64(d))
	case v.Kind() == reflect.String:
		v.SetString(raw)
	case v.Kind() == reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		v.SetBool(b)
	case v.CanInt():
		n, err := strconv.ParseInt(raw, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetInt(n)
	default:
		return fmt.Errorf("unsupported type %s", v.Type())
	}
	return nil
}

func negatives(v reflect.Value, path string) []error {
	var errs []error
	t := v.Type()
	for i := range t.NumField() {
		f, fv := t.Field(i), v.Field(i)
		key := yamlName(f)
		if path != "" {
			key = path + "." + key
		}
		switch {
		case f.Type.Kind() == reflect.Struct:
			errs = append(errs, negatives(fv, key)...)
		case fv.CanInt() && fv.Int() < 0:
			errs = append(errs, errors.New(key+" must not be negative"))
		}
	}
	return errs
}
