package rtstream

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

// Convert a redis.XMessage to a Message[T]
func toMessage[T any](rm redis.XMessage) Message[T] {
	var data T
	mapToStruct(&data, rm.Values)
	return Message[T]{
		ID:   rm.ID,
		Data: data,
	}
}

// Parse streams of form ["key id"] to Streams
func parseStreams(streams ...string) Streams {
	ids := make(map[string]string, len(streams))
	for _, stream := range streams {
		parts := strings.Split(stream, " ")
		if len(parts) == 1 {
			ids[parts[0]] = "$"
		} else if len(parts) == 2 {
			ids[parts[0]] = parts[1]
		}
	}
	return ids
}

// Parse value from string
// TODO: find a better solution. Maybe there is a library for this.
func fromString(kd reflect.Kind, st string) interface{} {
	switch kd {
	case reflect.String:
		return st
	case reflect.Int:
		i, err := strconv.Atoi(st)
		if err != nil {
			return int(0)
		}
		return i
	case reflect.Float32:
		i, err := strconv.ParseFloat(st, 32)
		if err != nil {
			return float32(0)
		}
		return i
	}
	return nil
}

// Convert struct to map
// TODO: support embedded structs
func structToMap(st any) map[string]any {
	rv := reflect.ValueOf(st)
	rt := reflect.TypeOf(st)
	out := make(map[string]interface{}, rv.NumField())

	for i := 0; i < rv.NumField(); i++ {
		fieldValue := rv.Field(i)
		fieldType := rt.Field(i)
		out[fieldType.Name] = fieldValue.Interface()
	}
	return out
}

// Convert map to struct
// TODO: support embedded structs
func mapToStruct(st any, vals map[string]any) {
	rv := reflect.ValueOf(st).Elem()
	for k, v := range vals {
		field := rv.FieldByName(k)
		if !field.IsValid() {
			continue
		}
		stval, ok := v.(string)
		if !ok {
			continue
		}
		val := fromString(field.Type().Kind(), stval)
		field.Set(reflect.ValueOf(val))
	}
}
