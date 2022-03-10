package query

import "github.com/cnosdb/cnosql"

// castToType will coerce the underlying interface type to another
// interface depending on the type.
func castToType(v interface{}, typ cnosql.DataType) interface{} {
	switch typ {
	case cnosql.Float:
		if val, ok := castToFloat(v); ok {
			v = val
		}
	case cnosql.Integer:
		if val, ok := castToInteger(v); ok {
			v = val
		}
	case cnosql.Unsigned:
		if val, ok := castToUnsigned(v); ok {
			v = val
		}
	case cnosql.String, cnosql.Tag:
		if val, ok := castToString(v); ok {
			v = val
		}
	case cnosql.Boolean:
		if val, ok := castToBoolean(v); ok {
			v = val
		}
	}
	return v
}

func castToFloat(v interface{}) (float64, bool) {
	switch v := v.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return float64(0), false
	}
}

func castToInteger(v interface{}) (int64, bool) {
	switch v := v.(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case uint64:
		return int64(v), true
	default:
		return int64(0), false
	}
}

func castToUnsigned(v interface{}) (uint64, bool) {
	switch v := v.(type) {
	case float64:
		return uint64(v), true
	case uint64:
		return v, true
	case int64:
		return uint64(v), true
	default:
		return uint64(0), false
	}
}

func castToString(v interface{}) (string, bool) {
	switch v := v.(type) {
	case string:
		return v, true
	default:
		return "", false
	}
}

func castToBoolean(v interface{}) (bool, bool) {
	switch v := v.(type) {
	case bool:
		return v, true
	default:
		return false, false
	}
}
