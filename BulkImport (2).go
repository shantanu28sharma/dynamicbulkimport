package bulkimport

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var timeType = reflect.TypeOf(time.Time{})

type Schema Object

type Field struct {
	Name        string
	Description string
	Type        FieldType
}

type FieldType interface {
	isField()
}

type PrimitiveType string

type ArrayType struct {
	FieldType
}

type Object struct {
	Name        string
	Description string
	Fields      []Field
}

func (PrimitiveType) isField() {}
func (ArrayType) isField()     {}
func (Object) isField()        {}

const (
	TextPrimitiveType PrimitiveType = "TEXT"
	IntPrimitiveType  PrimitiveType = "INTEGER"
	DecPrimitiveType  PrimitiveType = "DECIMAL"
	TimePrimitiveType PrimitiveType = "TIME"
	BoolPrimitiveType PrimitiveType = "BOOL"
)

var filledTypes = map[reflect.Type]map[string]func([]string, reflect.Value, int, Options, MappingObject) error{}

type Customer struct {
	Name   string
	Id     int
	Budget float64
	Join   time.Time
	Sub    SubCustomer
}

type SubCustomer struct {
	Name string
	Id   int
	Sub  Subs
}

type Subs struct {
	Website string
}

type MappingObject struct {
	Name  string
	Joins []FieldTypeMap
}

type Map struct {
	Field   string
	Header  int
	Options Options
}

type FieldTypeMap interface {
	retMap() Map
	retObj() MappingObject
}

func (m Map) retMap() Map {
	return m
}

func (m Map) retObj() MappingObject {
	return MappingObject{}
}

func (m MappingObject) retObj() MappingObject {
	return m
}

func (m MappingObject) retMap() Map {
	return Map{}
}

type TimeOption struct {
	Format string
}

func (TimeOption) isOption() {}

type Options interface {
	isOption()
}

func main() {
	in := `date,first name,contact,last name,website,budget
2011-04-11,Clementine,8305025573,person,abc.com,10000.53
2011-04-11,Barry,83050255735,hello,abc.com,10000.25234
2011-04-11,Barry,83050255735,hello,abc.com,10000.25234`
	m := MappingObject{Name: "Customer", Joins: []FieldTypeMap{Map{Field: ("Name"), Header: 1},
		Map{Field: ("Id"), Header: 2}, Map{Field: ("Budget"), Header: 5},
		Map{Field: ("Join"), Header: 0, Options: TimeOption{"2006-01-01"}},
		MappingObject{Name: "Sub", Joins: []FieldTypeMap{Map{Field: "Name", Header: 3}, Map{Field: "Id", Header: 2}, MappingObject{Name: "Sub", Joins: []FieldTypeMap{Map{Field: "Website", Header: 4}}}}},
	}}
	// m := MappingObject{Name: "Customer", Joins: []FieldTypeMap{Map{Field: ("Name"), Header: 1},
	// 	Map{Field: ("Id"), Header: 2}, Map{Field: ("Budget"), Header: 5},
	// 	Map{Field: ("Join"), Header: 0, Options: TimeOption{"2006-01-01"}},
	// }}
	Process(in, m)
	fmt.Println(ToSchema(&Customer{}))
}

func Memo(v interface{}) map[string]func([]string, reflect.Value, int, Options, MappingObject) error {
	typ := v.(reflect.Type)
	var Maps = make(map[string]func([]string, reflect.Value, int, Options, MappingObject) error)
	for i := 0; i < typ.NumField(); i++ {
		fa := typ.Field(i)
		f := fa.Name
		switch fa.Type.Kind() {
		case reflect.Bool:
			Maps[f] = func(r []string, V reflect.Value, ind int, opt Options, _ MappingObject) error {
				temp, err := strconv.ParseBool(r[ind])
				if err != nil {
					return errors.New("Unable to parse string as " + fa.Type.Kind().String()) //fmt.Fprintf
				}
				V.FieldByName(f).SetBool(temp)
				return nil
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			Maps[f] = func(r []string, V reflect.Value, ind int, opt Options, _ MappingObject) error {
				temp, err := strconv.ParseInt(r[ind], 10, 64)
				if err != nil {
					return errors.New("Unable to parse string as " + fa.Type.Kind().String())
				}
				V.FieldByName(f).SetInt(temp)
				return nil
			}
		case reflect.String:
			Maps[f] = func(r []string, V reflect.Value, ind int, opt Options, _ MappingObject) error {
				V.FieldByName(f).SetString(r[ind])
				return nil
			}
		case reflect.Float32, reflect.Float64:
			Maps[f] = func(r []string, V reflect.Value, ind int, opt Options, _ MappingObject) error {
				temp, err := strconv.ParseFloat(r[ind], 64)
				if err != nil {
					return errors.New("Unable to parse string as " + fa.Type.Kind().String())
				}
				V.FieldByName(f).SetFloat(temp)
				return nil
			}
		case reflect.Struct:
			if fa.Type == timeType {
				Maps[f] = func(r []string, V reflect.Value, ind int, opt Options, _ MappingObject) error {
					o, ok := opt.(TimeOption)
					if !ok {
						return errors.New("Invalid format for date")
					}
					temp, err := time.Parse(o.Format, r[ind])
					if err != nil {
						return errors.New("Unable to parse string as " + fa.Type.Kind().String())
					}
					V.FieldByName(f).Set(reflect.ValueOf(temp))
					return nil
				}
			} else {
				Maps[f] = func(r []string, V reflect.Value, _ int, _ Options, m MappingObject) error {
					err := FillMemoised(fa.Type, r, m, V.FieldByName(f))
					if err != nil {
						return err
					}
					return nil
				}
			}
		default:
			panic("Unknown Kind")
		}
	}
	return Maps
}

func FillMemoised(typ reflect.Type, r []string, m MappingObject, rv reflect.Value) error {

	val, ok := filledTypes[typ]
	if !ok {
		val = Memo(typ)
		filledTypes[typ] = val
	}

	for _, m_ := range m.Joins {
		j := m_.retMap()
		switch reflect.TypeOf(m_).String() {
		case "main.Map":
			set, ok := val[j.Field]
			if !ok {
				return errors.New("Unknown Field")
			}
			if err := set(r, rv, j.Header, j.Options, m); err != nil {
				return err
			}
		case "main.MappingObject":
			j_ := m_.retObj()
			set, ok := val[j_.Name]
			if !ok {
				return errors.New("Unknown Field")
			}
			if err := set(r, rv, 0, j.Options, j_); err != nil {
				return err
			}
		}

	}

	return nil
}

func Process(r string, m MappingObject) {
	csv := csv.NewReader(strings.NewReader(r))
	var i int
	c := Customer{}
	for {
		record, err := csv.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		} else {
			if i == 0 {
				i = 1
			} else {
				err := FillMemoised(reflect.TypeOf(&c).Elem(), record, m, reflect.ValueOf(&c).Elem())
				if err == nil {
					fmt.Println(c)
				}
			}
		}
	}
}

func ToSchema(v interface{}) Schema {
	typ := reflect.TypeOf(v)
	switch typ.Kind() {
	case reflect.Ptr, reflect.Uintptr:
		switch typ.Elem().Kind() {
		case reflect.Struct:
			return Schema(toSchema(typ.Elem()))
		default:
			panic("Input not a Struct")
		}
	case reflect.Struct:
		return Schema(toSchema(typ))
	default:
		panic("Input not a Struct")
	}
}

func toSchema(typ reflect.Type) Object {
	var fields []Field
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		fields = append(fields, Field{
			Name:        f.Name,
			Description: typ.Name() + " " + f.Name,
			Type:        TypeInfo(f.Type),
		})
	}

	return Object{
		Name:        typ.Name(),
		Description: typ.Name() + " Schema",
		Fields:      fields,
	}
}

func TypeInfo(v reflect.Type) FieldType {
	switch v.Kind() {
	case reflect.Bool:
		return BoolPrimitiveType
	case reflect.String:
		return TextPrimitiveType
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return IntPrimitiveType
	case reflect.Float32, reflect.Float64:
		return DecPrimitiveType
	case reflect.Slice, reflect.Array:
		return ArrayType{TypeInfo(v.Elem())}
	case reflect.Ptr, reflect.Uintptr:
		return TypeInfo(v.Elem())
	case reflect.Struct:
		if v == timeType {
			return DatePrimitiveType
		} else {
			return toSchema(v)
		}
	default:
		panic("Unknown Kind")
	}
}
