package types

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"

	"github.com/csimplestring/delta-go/errno"
	"github.com/rotisserie/eris"
)

var nonDecimalTypes []DataType = []DataType{
	&BinaryType{}, &BooleanType{}, &ByteType{}, &DateType{}, &DoubleType{}, &LongType{},
	&FloatType{}, &IntegerType{}, &BigIntType{}, &NullType{}, &ShortType{}, &StringType{}, &TimestampType{},
}

var nonDecimalNameToType map[string]DataType = make(map[string]DataType)

var fixedDecimalPattern *regexp.Regexp = regexp.MustCompile(`decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)`)

var defaultDecimal *DecimalType = &DecimalType{Precision: 10, Scale: 0}

func init() {
	for _, t := range nonDecimalTypes {
		nonDecimalNameToType[t.Name()] = t
	}
}

func FromJSON(s string) (DataType, error) {
	var j interface{}
	if err := json.Unmarshal([]byte(s), &j); err != nil {
		return nil, err
	}
	return parseDataType(j)
}

func ToJSON(d DataType) (string, error) {
	b, err := json.Marshal(dataTypeToJSON(d))
	return string(b), err
}

func ForceToJSON(d DataType) string {
	s, err := ToJSON(d)
	if err != nil {
		panic(err)
	}
	return s
}

func nameToType(s string) (DataType, error) {

	if s == "decimal" {
		return &DecimalType{Precision: 10, Scale: 0}, nil
	} else if fixedDecimalPattern.MatchString(s) {
		m := fixedDecimalPattern.FindStringSubmatch(s)
		p, _ := strconv.Atoi(m[1])
		s, _ := strconv.Atoi(m[2])
		return &DecimalType{Precision: p, Scale: s}, nil
	} else if res, ok := nonDecimalNameToType[s]; ok {
		return res, nil
	} else {
		return nil, eris.Wrapf(&errno.IllegalArgError{}, "fail to convert %s to a DataType", s)
	}
}

func dataTypeToJSON(d DataType) interface{} {
	// primitive types except for decimal
	if _, ok := nonDecimalNameToType[d.Name()]; ok {
		return d.Name()
	}

	switch v := d.(type) {
	case *DecimalType:
		return v.JSON()
	case *ArrayType:
		return map[string]interface{}{
			"type":         "array",
			"elementType":  dataTypeToJSON(v.ElementType),
			"containsNull": v.ContainsNull,
		}
	case *MapType:
		return map[string]interface{}{
			"type":              "map",
			"keyType":           dataTypeToJSON(v.KeyType),
			"valueType":         dataTypeToJSON(v.ValueType),
			"valueContainsNull": v.ValueContainsNull,
		}
	case *StructType:
		fields := make([]interface{}, len(v.Fields))
		for i, f := range v.Fields {
			fields[i] = structFieldToJSON(f)
		}
		return map[string]interface{}{
			"type":   "struct",
			"fields": fields,
		}
	default:
		panic(errno.IllegalArgError{Msg: fmt.Sprintf("can not marshal %v to json", v)})
	}

}

func structFieldToJSON(f *StructField) map[string]interface{} {
	return map[string]interface{}{
		"name":     f.Name,
		"type":     dataTypeToJSON(f.DataType),
		"nullable": f.Nullable,
		"metadata": f.Metadata,
	}
}

func parseDataType(s interface{}) (DataType, error) {

	switch v := s.(type) {
	case string:
		return nameToType(v)
	case map[string]interface{}:
		switch v["type"] {
		case "array":
			if elementType, err := parseDataType(v["elementType"]); err == nil {
				return &ArrayType{ElementType: elementType, ContainsNull: v["containsNull"].(bool)}, nil
			} else {
				return nil, err
			}
		case "map":
			keyType, err := parseDataType(v["keyType"])
			if err != nil {
				return nil, err
			}
			valueType, err := parseDataType(v["valueType"])
			if err != nil {
				return nil, err
			}
			valueContainsNull := v["valueContainsNull"].(bool)

			return &MapType{KeyType: keyType, ValueType: valueType, ValueContainsNull: valueContainsNull}, nil
		case "struct":
			rawFields := v["fields"].([]interface{})
			fieldsTypes := make([]*StructField, len(rawFields))
			for i, f := range rawFields {
				if fieldType, err := parseStructField(f.(map[string]interface{})); err != nil {
					return nil, err
				} else {
					fieldsTypes[i] = fieldType
				}
			}
			return NewStructType(fieldsTypes), nil
		default:
			return nil, eris.Wrapf(&errno.IllegalArgError{}, "unsupported type %s", v["type"])
		}

	default:
		return nil, eris.Wrapf(&errno.IllegalArgError{}, "unsupported type %s", v)
	}
}

func parseStructField(v map[string]interface{}) (*StructField, error) {
	fieldType, err := parseDataType(v["type"])
	if err != nil {
		return nil, err
	}

	sf := &StructField{
		Name:     v["name"].(string),
		Nullable: v["nullable"].(bool),
		DataType: fieldType,
	}

	if metaRaw, ok := v["metadata"]; ok && metaRaw != nil {
		m, err := parseStructFieldMetadata(metaRaw.(map[string]interface{}))
		if err != nil {
			return nil, err
		}
		sf.Metadata = m
	}

	return sf, nil
}

func parseStructFieldMetadata(m map[string]interface{}) (map[string]interface{}, error) {
	res := make(map[string]interface{}, len(m))
	for k, v := range m {
		arr, is_slice := v.([]interface{})
		// not array
		if !is_slice {
			res[k] = v
			continue
		}
		// empty array
		if len(arr) == 0 {
			res[k] = []float64{}
			continue
		}
		// iterate array
		var err error
		switch arr[0].(type) {
		case float64:
			res[k], err = asSliceOf[float64](arr, nil)
		case bool:
			res[k], err = asSliceOf[bool](arr, nil)
		case string:
			res[k], err = asSliceOf[string](arr, nil)
		case map[string]interface{}:
			res[k], err = asSliceOf(arr, func(i interface{}) (map[string]interface{}, error) {
				return parseStructFieldMetadata(i.(map[string]interface{}))
			})
		default:
			return nil, eris.Wrapf(&errno.IllegalArgError{}, "unsupported type %s", v)
		}
		if err != nil {
			return nil, err
		}

	}
	return res, nil
}

func asSliceOf[T any](s []interface{}, mapper func(i interface{}) (T, error)) ([]T, error) {
	res := make([]T, len(s))
	for i, item := range s {
		if mapper == nil {
			res[i] = item.(T)
		} else {
			v, err := mapper(item)
			if err != nil {
				return nil, err
			}
			res[i] = v
		}
	}
	return res, nil
}
