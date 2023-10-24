pub mod json_struct;

use crate::generator::chernobyl::json_struct::{Type, Unit};
use crate::generator::{Generator, Message};
use crate::helper::rand_entry;
use rand::Rng;
use std::collections::HashMap;
use std::time::SystemTime;

#[derive(Clone)]
pub struct TopicInternal {
    pub name: String,
    unit: Unit,
    _type: Type,
}

pub struct Chernobyl {
    split_point: usize,
    topics: Vec<TopicInternal>,
}

impl Generator for Chernobyl {
    fn new(split_point: usize, number_of_topics: usize) -> anyhow::Result<Self> {
        let mut c = Self {
            split_point,
            topics: Vec::with_capacity(number_of_topics),
        };
        c.generate_topics(number_of_topics)?;
        Ok(c)
    }

    fn get_message(&self) -> anyhow::Result<Message> {
        let topic = rand_entry(&self.topics);
        let topic_name_split: Vec<&str> = topic.name.split('.').collect();
        let mut data: HashMap<String, String> = HashMap::new();
        let mut rng = rand::thread_rng();

        let topic_name = topic_name_split
            .iter()
            .take(self.split_point)
            .cloned()
            .collect::<Vec<_>>()
            .join(".");
        let mut key = topic_name_split
            .iter()
            .skip(self.split_point)
            .cloned()
            .collect::<Vec<_>>()
            .join(".");

        let nano_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_nanos();

        data.insert(
            "timestamp_ms".to_owned(),
            format!("{}", nano_time / 1_000_000),
        );

        // Match on topic.unit
        match topic.unit {
            Unit::None => match topic._type {
                Type::Boolean => {
                    data.insert("value".to_owned(), format!("{}", rng.gen_bool(0.5)));
                }
                Type::Float => {
                    data.insert("value".to_owned(), format!("{}", rng.gen_range(0.0..1.0)));
                }
                Type::Int => {
                    data.insert("value".to_owned(), format!("{}", rng.gen_range(0..1)));
                }
            },
            Unit::DegreeC => match topic._type {
                Type::Boolean => {
                    unreachable!("Boolean type not supported for unit DegreeC");
                }
                Type::Float => {
                    data.insert(
                        "degreeC".to_owned(),
                        format!("{}", rng.gen_range(0.0..1000.0)),
                    );
                }
                Type::Int => {
                    data.insert("degreeC".to_owned(), format!("{}", rng.gen_range(0..1000)));
                }
            },
            Unit::Percent => match topic._type {
                Type::Boolean => {
                    unreachable!("Boolean type not supported for unit Percent");
                }
                Type::Float => {
                    data.insert(
                        "percent".to_owned(),
                        format!("{}", rng.gen_range(0.0..100.0)),
                    );
                }
                Type::Int => {
                    data.insert("percent".to_owned(), format!("{}", rng.gen_range(0..100)));
                }
            },
            Unit::Pascal => match topic._type {
                Type::Boolean => {
                    unreachable!("Boolean type not supported for unit Pascal");
                }
                Type::Float => {
                    data.insert(
                        "pascal".to_owned(),
                        format!("{}", rng.gen_range(100.0..10000000.0)),
                    );
                }
                Type::Int => {
                    data.insert(
                        "pascal".to_owned(),
                        format!("{}", rng.gen_range(100..10000000)),
                    );
                }
            },
            Unit::CubicMetersPerHour => match topic._type {
                Type::Boolean => {
                    unreachable!("Boolean type not supported for unit CubicMetersPerHour");
                }
                Type::Float => {
                    data.insert(
                        "cubicMetersPerHour".to_owned(),
                        format!("{}", rng.gen_range(0.0..100.0)),
                    );
                }
                Type::Int => {
                    data.insert(
                        "cubicMetersPerHour".to_owned(),
                        format!("{}", rng.gen_range(0..100)),
                    );
                }
            },
            Unit::Volt => match topic._type {
                Type::Boolean => {
                    unreachable!("Boolean type not supported for unit Volt");
                }
                Type::Float => {
                    data.insert("volt".to_owned(), format!("{}", rng.gen_range(0.0..1000.0)));
                }
                Type::Int => {
                    data.insert("volt".to_owned(), format!("{}", rng.gen_range(0..1000)));
                }
            },
            Unit::Ampere => match topic._type {
                Type::Boolean => {
                    unreachable!("Boolean type not supported for unit Ampere");
                }
                Type::Float => {
                    data.insert(
                        "ampere".to_owned(),
                        format!("{}", rng.gen_range(0.0..1000.0)),
                    );
                }
                Type::Int => {
                    data.insert("ampere".to_owned(), format!("{}", rng.gen_range(0..1000)));
                }
            },
            Unit::SievertPerHour => {
                match topic._type {
                    Type::Boolean => {
                        unreachable!("Boolean type not supported for unit SievertPerHour");
                    }
                    Type::Float => {
                        let mut sievert = rng.gen_range(0.0..1_000_000_000.0);
                        // Divide to get nano sievert
                        sievert /= 1_000_000_000.0;
                        data.insert("sievertPerHour".to_owned(), format!("{}", sievert));
                    }
                    Type::Int => {
                        data.insert(
                            "sievertPerHour".to_owned(),
                            format!("{}", rng.gen_range(0..1)),
                        );
                        // If this is ever 1, this is a bad day
                    }
                }
            }
            Unit::RotationsPerMinute => match topic._type {
                Type::Boolean => {
                    unreachable!("Boolean type not supported for unit RotationsPerMinute");
                }
                Type::Float => {
                    data.insert(
                        "rotationsPerMinute".to_owned(),
                        format!("{}", rng.gen_range(0.0..1000.0)),
                    );
                }
                Type::Int => {
                    data.insert(
                        "rotationsPerMinute".to_owned(),
                        format!("{}", rng.gen_range(0..1000)),
                    );
                }
            },
            Unit::Watt => match topic._type {
                Type::Boolean => {
                    unreachable!("Boolean type not supported for unit Watt");
                }
                Type::Float => {
                    data.insert(
                        "watt".to_owned(),
                        format!("{}", rng.gen_range(0.0..1_000_000.0)),
                    );
                }
                Type::Int => {
                    data.insert(
                        "watt".to_owned(),
                        format!("{}", rng.gen_range(0..1_000_000)),
                    );
                }
            },
            Unit::MetersPerSecond => match topic._type {
                Type::Boolean => {
                    unreachable!("Boolean type not supported for unit Speed");
                }
                Type::Float => {
                    data.insert(
                        "metersPerSecond".to_owned(),
                        format!("{}", rng.gen_range(0.0..1000.0)),
                    );
                }
                Type::Int => {
                    data.insert(
                        "metersPerSecond".to_owned(),
                        format!("{}", rng.gen_range(0..1000)),
                    );
                }
            },
        }

        // HashMap to json bytes
        let data_as_json = serde_json::to_vec(&data)?;

        Ok(Message {
            topic: topic_name,
            value: data_as_json,
            key,
        })
    }

    fn generate_topics(&mut self, number_of_topics: usize) -> anyhow::Result<()> {
        let mut rng = rand::thread_rng();
        let mut topic: Vec<String> = vec![];

        let pp = json_struct::load()?;
        let chernobyl = pp
            .get(0)
            .map_or(Err(anyhow::anyhow!("No powerplant found")), Ok)?;
        let site = chernobyl
            .sites
            .get(0)
            .map_or(Err(anyhow::anyhow!("No site found")), Ok)?;

        for _ in 0..number_of_topics {
            topic.push("umh.v1.".to_owned());
            topic.push(chernobyl.enterprise.clone());
            topic.push(".".to_owned());
            topic.push(site.site.to_owned());

            // push number from 1-4 using rng
            let rand_number = rng.gen_range(1..5).to_string();
            topic.push(rand_number);
            topic.push(".".to_owned());

            let area = rand_entry(&site.areas);
            topic.push(area.area.to_owned());
            topic.push(".".to_owned());

            let production_line = rand_entry(&area.production_lines);
            topic.push(production_line.production_line.to_owned());
            topic.push(".".to_owned());

            let work_cell = rand_entry(&production_line.work_cells);
            topic.push(work_cell.work_cell.to_owned());
            topic.push(".".to_owned());

            topic.push(work_cell.tag_group.to_owned());
            topic.push(".".to_owned());

            let tag = rand_entry(&work_cell.tags);
            topic.push(tag.name.to_owned());

            let t: TopicInternal = TopicInternal {
                name: topic.join(""),
                unit: tag.unit,
                _type: tag.tag_type,
            };

            self.topics.push(t);
            topic.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::generator::chernobyl::Chernobyl;
    use crate::generator::Generator;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_chernobyl() {
        let c = Chernobyl::new(3, 100).unwrap();
        for _ in 0..100 {
            let m = c.get_message().unwrap();

            // Assert that the topic is correct
            assert_eq!(m.topic, "umh.v1.chernobylnuclearpowerplant");

            // Key must be in this form: ^(?:\w+\.){6}\d+$
            let re = regex::Regex::new(r"^(?:\w+\.){6}\d+$").unwrap();

            assert!(re.is_match(&m.key));

            // Assert that the value is valid json
            let v: serde_json::Value = serde_json::from_slice(&m.value).unwrap();
            assert!(v.is_object());
        }
    }
}
