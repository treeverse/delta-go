package types

import (
	"encoding/json"
	"testing"

	"github.com/csimplestring/delta-go/errno"
	"github.com/stretchr/testify/assert"
)

func Test_nameToType(t *testing.T) {
	dt, err := nameToType("decimal(16, 5)")
	assert.NoError(t, err)
	assert.Equal(t, &DecimalType{Precision: 16, Scale: 5}, dt)

	dt, err = nameToType("decimal")
	assert.NoError(t, err)
	assert.Equal(t, &DecimalType{Precision: 10, Scale: 0}, dt)

	for name, dataType := range nonDecimalNameToType {
		actual, err := nameToType(name)
		assert.NoError(t, err)
		assert.Equal(t, dataType, actual)
	}

	_, err = nameToType("unknown")
	errIllegalArg := &errno.IllegalArgError{}
	assert.ErrorAs(t, err, &errIllegalArg)
}

func Test_parseDataType(t *testing.T) {
	var v interface{}
	err := json.Unmarshal([]byte("\"double\""), &v)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDataTypeSerde(t *testing.T) {

	check := func(dataType DataType) {
		j, err := ToJSON(dataType)
		assert.NoError(t, err)

		actual, err := FromJSON(j)
		assert.NoError(t, err)
		assert.Equal(t, dataType, actual)

		// inside struct field test
		field1 := NewStructField("foo", dataType, true)
		field2 := NewStructField("bar", dataType, true)
		structType := NewStructType([]*StructField{field1, field2})

		j, err = ToJSON(structType)
		assert.NoError(t, err)

		actual, err = FromJSON(j)
		assert.NoError(t, err)
		assert.Equal(t, structType, actual)
	}

	check(&BooleanType{})
	check(&ByteType{})
	check(&ShortType{})
	check(&IntegerType{})
	check(&BigIntType{})
	check(&LongType{})
	check(&FloatType{})
	check(&DoubleType{})
	check(&DecimalType{Precision: 10, Scale: 5})
	check(defaultDecimal)
	check(&DateType{})
	check(&TimestampType{})
	check(&StringType{})
	check(&BinaryType{})
	check(&ArrayType{ElementType: &DoubleType{}, ContainsNull: true})
	check(&ArrayType{ElementType: &StringType{}, ContainsNull: false})
	check(&MapType{KeyType: &IntegerType{}, ValueType: &StringType{}, ValueContainsNull: false})
	check(&MapType{KeyType: &IntegerType{}, ValueType: &ArrayType{ElementType: &DoubleType{}, ContainsNull: true}, ValueContainsNull: false})
}

func TestDataTypeSerde_fieldMetadata(t *testing.T) {

	emptyMetadata := map[string]interface{}{}
	singleStringMetadata := map[string]interface{}{"test": "test_value"}
	singleBooleanMetadata := map[string]interface{}{"test": true}
	// comment out this, int/int64 are converted to float64 anyway during json marshal
	// singleIntegerMetadata := map[string]interface{}{"test": int64(2)}
	singleDoubleMetadata := map[string]interface{}{"test": 2.0}
	singleMapMetadata := map[string]interface{}{"test_outside": map[string]interface{}{"test_inside": "value"}}
	singleListMetadata := map[string]interface{}{"test": []float64{0, 1, 2}}
	multipleEntriesMetadata := map[string]interface{}{"test": "test_value"}

	structType := NewStructType([]*StructField{
		{Name: "emptyMetadata", DataType: &BooleanType{}, Nullable: true, Metadata: emptyMetadata},
		{Name: "singleStringMetadata", DataType: &BooleanType{}, Nullable: true, Metadata: singleStringMetadata},
		{Name: "singleBooleanMetadata", DataType: &BooleanType{}, Nullable: true, Metadata: singleBooleanMetadata},
		//{Name: "singleIntegerMetadata", DataType: &BooleanType{}, Nullable: true, Metadata: singleIntegerMetadata},
		{Name: "singleDoubleMetadata", DataType: &BooleanType{}, Nullable: true, Metadata: singleDoubleMetadata},
		{Name: "singleMapMetadata", DataType: &BooleanType{}, Nullable: true, Metadata: singleMapMetadata},
		{Name: "singleListMetadata", DataType: &BooleanType{}, Nullable: true, Metadata: singleListMetadata},
		{Name: "multipleEntriesMetadata", DataType: &BooleanType{}, Nullable: true, Metadata: multipleEntriesMetadata},
	})

	s, err := ToJSON(structType)
	assert.NoError(t, err)
	actual, err := FromJSON(s)
	assert.NoError(t, err)

	assert.Equal(t, structType, actual)
}
