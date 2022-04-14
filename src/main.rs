use std::time::Duration;
use kafka::client::FetchOffset;
use kafka::consumer::{Consumer, GroupOffsetStorage};
use kafka::producer::{Producer, Record, RequiredAcks};

fn main() {
    println!("Hello, world!");

    let mut producer = Producer::from_hosts(vec!("localhost:9092".to_owned()))
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap();
    let record = Record::from_value("topic", "value");
    producer.send(&record).unwrap();
    println!("Wrote dummy value to producer.");

    let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("topic".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("group".to_owned())
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap();

    println!("Reading messages from consumer:");
    for ms in consumer.poll().unwrap().iter() {
        for m in ms.messages() {
            println!("{:?}", m);
        }
        consumer.consume_messageset(ms);
    }
    consumer.commit_consumed().unwrap();
    println!("Consumer is finished.");
}
