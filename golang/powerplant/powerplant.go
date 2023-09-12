package powerplant

import (
	"fmt"
	"github.com/goccy/go-json"
	"os"
	"strings"
)

type PowerPlant struct {
	Enterprise string `json:"enterprise"`
	Sites      []Site `json:"sites"`
}

type Site struct {
	Site  string `json:"site"`
	Areas []Area `json:"areas"`
}

type Area struct {
	Area            string           `json:"area"`
	ProductionLines []ProductionLine `json:"productionLines"`
}

type ProductionLine struct {
	ProductionLine string     `json:"productionLine"`
	WorkCells      []WorkCell `json:"workCells"`
}

type WorkCell struct {
	WorkCell string `json:"workCell"`
	TagGroup string `json:"tagGroup"`
	Tags     []Tag  `json:"tags"`
}

type Tag struct {
	Name string `json:"name"`
	Unit Unit   `json:"unit"`
	Type Type   `json:"type"`
}

type Unit string

const (
	UnitNone               Unit = ""
	UnitDegreeC            Unit = "°C"
	UnitPercent            Unit = "%"
	UnitPascal             Unit = "Pa"
	UnitCubicMetersPerHour Unit = "m3/h"
	UnitVolt               Unit = "V"
	UnitAmpere             Unit = "A"
	UnitSivertPerHour      Unit = "Sv/h"
	UnitRotationsPerMinute Unit = "rpm"
	UnitWatt               Unit = "W"
	UnitSpeed              Unit = "m/s"
)

func (u *Unit) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), "\"")
	switch s {
	case "", "°C", "%", "Pa", "m3/h", "V", "A", "Sv/h", "rpm", "W", "m/s":
		*u = Unit(s)
		return nil
	default:
		return fmt.Errorf("invalid unit value: %s", s)
	}
}

type Type string

const (
	Boolean Type = "boolean"
	Float   Type = "float"
	Int     Type = "int"
)

func Load() (*PowerPlant, error) {
	// load powerplant.json and return a PowerPlant struct

	file, err := os.ReadFile("powerplant/powerplant.json")
	if err != nil {
		return nil, err
	}

	var powerplant []PowerPlant
	err = json.Unmarshal(file, &powerplant)
	if err != nil {
		return nil, err
	}
	return &powerplant[0], nil

}
