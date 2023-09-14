use crate::powerplant;
use crate::powerplant::Unit;
use rand::{thread_rng, Rng};
use std::collections::{HashMap};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;

const THREADS: usize = 16;

#[derive(Clone)]
pub(crate) struct TopicInternal {
    pub(crate) name: String,
    unit: Unit,
    _type: powerplant::Type,
}

#[derive(Clone)]
pub(crate) struct ChannelTopic {
    pub(crate) topic: String,
    pub(crate) message: Vec<u8>,
    pub(crate) key: String,
}

pub(crate) struct Generator {
    topics: Arc<Vec<TopicInternal>>,
    channel: (SyncSender<ChannelTopic>, Receiver<ChannelTopic>),
    messages: Vec<ChannelTopic>,
}

impl Generator {
    pub(crate) fn new(topic_amount: usize, split_point: usize, messages: usize) -> Generator {
        log::info!("Generating {} topics", topic_amount);
        let mut now = std::time::Instant::now();
        let mut topics = Vec::with_capacity(topic_amount);

        let mut rng = thread_rng();

        let pp = powerplant::load();
        let chernobyl = pp.get(0).expect("Chernobyl not found");

        let mut topic: Vec<String> = vec![];
        for _ in 0..topic_amount {
            topic.push("umh.v1.".to_owned());
            topic.push(chernobyl.enterprise.clone());
            topic.push(".".to_owned());

            let site = chernobyl.sites.get(0).expect("Site not found");
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

            topic.push("_".to_owned());

            // Generate 6 digit random hex number
            let hex = format!("{:06x}", rng.gen_range(0..0xffffff));
            topic.push(hex.to_owned());

            let t: TopicInternal = TopicInternal {
                name: topic.join(""),
                unit: tag.unit,
                _type: tag.tag_type,
            };

            topics.push(t);
            topic.clear();
        }
        log::info!(
            "Generated {} topics in {} ms",
            topic_amount,
            now.elapsed().as_millis()
        );

        now = std::time::Instant::now();
        let channel: (SyncSender<ChannelTopic>, Receiver<ChannelTopic>) =
            std::sync::mpsc::sync_channel(messages);
        let mut gen = Self {
            topics: Arc::new(topics),
            channel,
            messages: Vec::with_capacity(messages),
        };

        let messages_per_thread = messages / THREADS;
        let diff = messages - (messages_per_thread * THREADS);
        for _ in 0..THREADS {
            let xtopics = gen.topics.clone();
            let xsend = gen.channel.0.clone();
            thread::spawn(move || {
                begin_generate(xsend, xtopics, messages_per_thread, split_point);
            });
        }
        begin_generate(gen.channel.0.clone(), gen.topics.clone(), diff, split_point);

        while gen.messages.len() < messages {
            gen.messages
                .push(gen.channel.1.recv().expect("Failed to receive message"));
        }

        log::info!(
            "Generated {} messages in {} ms",
            messages,
            now.elapsed().as_millis()
        );

        return gen;
    }

    pub(crate) fn get_topics(&self) -> Arc<Vec<TopicInternal>> {
        self.topics.clone()
    }

    pub(crate) fn get_message(&mut self) -> Vec<ChannelTopic> {
        self.messages.clone()
    }
}

fn begin_generate(
    channel: SyncSender<ChannelTopic>,
    topics: Arc<Vec<TopicInternal>>,
    to_generate: usize,
    split_point: usize,
) {
    let mut rng = thread_rng();
    let mut data: HashMap<String, String> = HashMap::new();

    for _i in 0..to_generate {
        data.clear();
        let topic = rand_entry(&topics);
        let topic_name_split: Vec<&str> = topic.name.split(".").collect();

        let topic_name = topic_name_split
            .iter()
            .take(split_point)
            .cloned()
            .collect::<Vec<_>>()
            .join(".");
        let mut key = topic_name_split
            .iter()
            .skip(split_point)
            .cloned()
            .collect::<Vec<_>>()
            .join(".");

        let nano_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Failed to get time")
            .as_nanos();
        key.push_str(&format!(".{}", nano_time));

        data.insert(
            "timestamp_ms".to_owned(),
            format!("{}", nano_time / 1_000_000),
        );

        // Match on topic.unit
        match topic.unit {
            Unit::None => match topic._type {
                powerplant::Type::Boolean => {
                    data.insert("value".to_owned(), format!("{}", rng.gen_bool(0.5)));
                }
                powerplant::Type::Float => {
                    data.insert("value".to_owned(), format!("{}", rng.gen_range(0.0..100.0)));
                }
                powerplant::Type::Int => {
                    data.insert("value".to_owned(), format!("{}", rng.gen_range(0..100)));
                }
            },
            Unit::DegreeC => match topic._type {
                powerplant::Type::Boolean => {
                    log::error!("Boolean type not supported for unit DegreeC");
                    continue;
                }
                powerplant::Type::Float => {
                    data.insert(
                        "degreeC".to_owned(),
                        format!("{}", rng.gen_range(0.0..100.0)),
                    );
                }
                powerplant::Type::Int => {
                    data.insert("degreeC".to_owned(), format!("{}", rng.gen_range(0..100)));
                }
            },
            Unit::Percent => match topic._type {
                powerplant::Type::Boolean => {
                    log::error!("Boolean type not supported for unit Percent");
                    continue;
                }
                powerplant::Type::Float => {
                    data.insert(
                        "percent".to_owned(),
                        format!("{}", rng.gen_range(0.0..100.0)),
                    );
                }
                powerplant::Type::Int => {
                    data.insert("percent".to_owned(), format!("{}", rng.gen_range(0..100)));
                }
            },
            Unit::Pascal => match topic._type {
                powerplant::Type::Boolean => {
                    log::error!("Boolean type not supported for unit Pascal");
                    continue;
                }
                powerplant::Type::Float => {
                    data.insert(
                        "pascal".to_owned(),
                        format!("{}", rng.gen_range(0.0..100.0)),
                    );
                }
                powerplant::Type::Int => {
                    data.insert("pascal".to_owned(), format!("{}", rng.gen_range(0..100)));
                }
            },
            Unit::CubicMetersPerHour => match topic._type {
                powerplant::Type::Boolean => {
                    log::error!("Boolean type not supported for unit CubicMetersPerHour");
                    continue;
                }
                powerplant::Type::Float => {
                    data.insert(
                        "cubicMetersPerHour".to_owned(),
                        format!("{}", rng.gen_range(0.0..100.0)),
                    );
                }
                powerplant::Type::Int => {
                    data.insert(
                        "cubicMetersPerHour".to_owned(),
                        format!("{}", rng.gen_range(0..100)),
                    );
                }
            },
            Unit::Volt => match topic._type {
                powerplant::Type::Boolean => {
                    log::error!("Boolean type not supported for unit Volt");
                    continue;
                }
                powerplant::Type::Float => {
                    data.insert("volt".to_owned(), format!("{}", rng.gen_range(0.0..100.0)));
                }
                powerplant::Type::Int => {
                    data.insert("volt".to_owned(), format!("{}", rng.gen_range(0..100)));
                }
            },
            Unit::Ampere => match topic._type {
                powerplant::Type::Boolean => {
                    log::error!("Boolean type not supported for unit Ampere");
                    continue;
                }
                powerplant::Type::Float => {
                    data.insert(
                        "ampere".to_owned(),
                        format!("{}", rng.gen_range(0.0..100.0)),
                    );
                }
                powerplant::Type::Int => {
                    data.insert("ampere".to_owned(), format!("{}", rng.gen_range(0..100)));
                }
            },
            Unit::SievertPerHour => {
                match topic._type {
                    powerplant::Type::Boolean => {
                        log::error!("Boolean type not supported for unit SievertPerHour");
                        continue;
                    }
                    powerplant::Type::Float => {
                        let mut sievert = rng.gen_range(0.0..100.0);
                        // Divide to get nano sievert
                        sievert /= 1_000_000_000.0;
                        data.insert("sievertPerHour".to_owned(), format!("{}", sievert));
                    }
                    powerplant::Type::Int => {
                        data.insert(
                            "sievertPerHour".to_owned(),
                            format!("{}", rng.gen_range(0..1)),
                        );
                        // If this is ever 1, this is a bad day
                    }
                }
            }
            Unit::RotationsPerMinute => match topic._type {
                powerplant::Type::Boolean => {
                    log::error!("Boolean type not supported for unit RotationsPerMinute");
                    continue;
                }
                powerplant::Type::Float => {
                    data.insert(
                        "rotationsPerMinute".to_owned(),
                        format!("{}", rng.gen_range(0.0..100.0)),
                    );
                }
                powerplant::Type::Int => {
                    data.insert(
                        "rotationsPerMinute".to_owned(),
                        format!("{}", rng.gen_range(0..100)),
                    );
                }
            },
            Unit::Watt => match topic._type {
                powerplant::Type::Boolean => {
                    log::error!("Boolean type not supported for unit Watt");
                    continue;
                }
                powerplant::Type::Float => {
                    data.insert("watt".to_owned(), format!("{}", rng.gen_range(0.0..100.0)));
                }
                powerplant::Type::Int => {
                    data.insert("watt".to_owned(), format!("{}", rng.gen_range(0..100)));
                }
            },
            Unit::Speed => match topic._type {
                powerplant::Type::Boolean => {
                    log::error!("Boolean type not supported for unit Speed");
                    continue;
                }
                powerplant::Type::Float => {
                    data.insert("speed".to_owned(), format!("{}", rng.gen_range(0.0..100.0)));
                }
                powerplant::Type::Int => {
                    data.insert("speed".to_owned(), format!("{}", rng.gen_range(0..100)));
                }
            },
        }

        // HashMap to json bytes
        let data_as_json = serde_json::to_vec(&data).expect("Failed to serialize data");

        // Ignore errors
        let _ = channel.send(ChannelTopic {
            topic: topic_name,
            message: data_as_json,
            key,
        });
    }
}

fn rand_entry<T>(entries: &[T]) -> &T {
    let mut rng = thread_rng();
    &entries[rng.gen_range(0..entries.len())]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_generator() {
        let topic_amount = 1000;
        let mut g = Generator::new(topic_amount, 4, 1000);
        assert_eq!(g.topics.len(), topic_amount);
        // display first 10 topics
        let topics = g.get_topics();
        for i in 0..10 {
            println!("{}", topics[i].name);
        }

        let messages = g.get_message();
        // Get 5 messages
        for _ in 0..5 {
            let msg = messages.get(0).expect("Failed to get message");
            let original_topic = msg.topic.clone() + "." + &msg.key;
            println!("{}: {:?}", original_topic, msg.message);
        }
    }
}
