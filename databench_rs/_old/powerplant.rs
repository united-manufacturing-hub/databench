// Extend your existing imports:
use serde::{Deserialize, Serialize};

pub type PowerPlant = Vec<PowerplantElement>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PowerplantElement {
    pub(crate) enterprise: String,
    pub(crate) sites: Vec<Site>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Site {
    pub(crate) site: String,
    pub(crate) areas: Vec<Area>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Area {
    pub(crate) area: String,
    pub(crate) production_lines: Vec<ProductionLine>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProductionLine {
    pub(crate) production_line: String,
    pub(crate) work_cells: Vec<WorkCell>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkCell {
    pub(crate) work_cell: String,
    pub(crate) tag_group: String,
    pub(crate) tags: Vec<Tag>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tag {
    pub(crate) name: String,
    pub(crate) unit: Unit,
    #[serde(rename = "type")]
    pub(crate) tag_type: Type,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Boolean,
    Float,
    Int,
}

#[derive(Debug, Clone, PartialEq, Serialize, Copy)]
pub enum Unit {
    None,
    DegreeC,
    Percent,
    Pascal,
    CubicMetersPerHour,
    Volt,
    Ampere,
    SievertPerHour,
    RotationsPerMinute,
    Watt,
    Speed,
}

impl<'de> serde::de::Deserialize<'de> for Unit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        match s.as_str() {
            "" => Ok(Unit::None),
            "°C" => Ok(Unit::DegreeC),
            "%" => Ok(Unit::Percent),
            "Pa" => Ok(Unit::Pascal),
            "m3/h" => Ok(Unit::CubicMetersPerHour),
            "V" => Ok(Unit::Volt),
            "A" => Ok(Unit::Ampere),
            "Sv/h" => Ok(Unit::SievertPerHour),
            "rpm" => Ok(Unit::RotationsPerMinute),
            "W" => Ok(Unit::Watt),
            "m/s" => Ok(Unit::Speed),
            _ => Err(serde::de::Error::custom(format!(
                "invalid unit value: {}",
                s
            ))),
        }
    }
}

pub(crate) fn load() -> PowerPlant {
    serde_json::from_str(include_str!("../src/generator/chernobyl/powerplant.json")).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load() {
        let powerplant = load();
        assert_eq!(powerplant.len(), 1);
    }
}
