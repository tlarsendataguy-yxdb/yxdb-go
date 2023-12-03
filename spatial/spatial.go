package spatial

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
)

const bytesPerPoint = 16

// ToGeoJSON translates SpatialObj fields into GeoJSON.
//
// Alteryx stores spatial objects in a binary format.
// This function reads the binary format and converts it to a GeoJSON string.
func ToGeoJSON(value []byte) (string, error) {
	if value == nil {
		return ``, nil
	}
	if len(value) < 20 {
		return ``, errors.New(`bytes are not a spatial object`)
	}
	objType := int(binary.LittleEndian.Uint32(value[0:4]))
	switch objType {
	case 8:
		return parsePoints(value)
	case 3:
		return parseLines(value)
	case 5:
		return parsePoly(value)
	}
	return ``, errors.New(`bytes are not a spatial object`)
}

func parsePoints(value []byte) (string, error) {
	totalPoints := int(binary.LittleEndian.Uint32(value[36:40]))
	if totalPoints == 1 {
		return parseSinglePoint(value)
	}
	return parseMultiPoint(totalPoints, value)
}

func parseSinglePoint(value []byte) (string, error) {
	return geoJSON(`Point`, getCoordAt(value, 40))
}

func parseMultiPoint(totalPoints int, value []byte) (string, error) {
	points := make([][2]float64, 0, totalPoints)
	i := 40
	for i < len(value) {
		points = append(points, getCoordAt(value, i))
		i += bytesPerPoint
	}
	return geoJSON(`MultiPoint`, points)
}

func parseLines(value []byte) (string, error) {
	lines := parseMultiPointObject(value)

	if len(lines) == 1 {
		return geoJSON(`LineString`, lines[0])
	}
	return geoJSON(`MultiLineString`, lines)
}

func parsePoly(value []byte) (string, error) {
	poly := parseMultiPointObject(value)

	if len(poly) == 1 {
		return geoJSON(`Polygon`, poly)
	}
	return geoJSON(`MultiPolygon`, []any{poly})
}

func parseMultiPointObject(value []byte) [][][2]float64 {
	endingIndices := getEndingIndices(value)

	i := 48 + (len(endingIndices) * 4) - 4
	objects := make([][][2]float64, len(endingIndices))
	for objIndex, endingIndex := range endingIndices {
		line := make([][2]float64, 0, (endingIndex-i)/bytesPerPoint)
		for i < endingIndex {
			line = append(line, getCoordAt(value, i))
			i += bytesPerPoint
		}
		objects[objIndex] = line
	}
	return objects
}

func getEndingIndices(value []byte) []int {
	totalObjects := int(binary.LittleEndian.Uint32(value[36:40]))
	totalPoints := int(binary.LittleEndian.Uint64(value[40:48]))
	endingIndices := make([]int, 0, totalObjects)

	i := 48
	startAt := 48 + ((totalObjects - 1) * 4)
	for j := 1; j < totalObjects; j++ {
		endingPoint := int(binary.LittleEndian.Uint32(value[i : i+4]))
		endingIndex := (endingPoint * bytesPerPoint) + startAt
		endingIndices = append(endingIndices, endingIndex)
		i += 4
	}
	endingIndices = append(endingIndices, (totalPoints*bytesPerPoint)+startAt)
	return endingIndices
}

func getCoordAt(value []byte, i int) [2]float64 {
	lng := math.Float64frombits(binary.LittleEndian.Uint64(value[i : i+8]))
	lat := math.Float64frombits(binary.LittleEndian.Uint64(value[i+8 : i+bytesPerPoint]))
	return [2]float64{lng, lat}
}

type returnObj struct {
	Type        string `json:"type"`
	Coordinates any    `json:"coordinates"`
}

func geoJSON(objType string, data any) (string, error) {
	raw, err := json.Marshal(returnObj{
		Type:        objType,
		Coordinates: data,
	})
	return string(raw), err
}
