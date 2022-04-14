use std::io::Write;
use kafka::client::FetchOffset;
use kafka::consumer::{Consumer, GroupOffsetStorage, Message, MessageSet};
use kafka::producer::{Producer, Record, RequiredAcks};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;
use tokio_stream::StreamExt;

const LOG_FILENAME: &str = "consumer.log";

#[tokio::main]
async fn main() {
    println!("---------------------------------------------------------------------------------------");
    println!("--- Enter a value into the console, it will be sent to kafka.                       ---");
    println!("--- Supported: <value> (no key) | <key:value> (key + value)                         ---");
    println!("--- Enter <exit> to leave.                                                          ---");
    println!("--- Enter <show> to output log.                                                     ---");
    println!("--- Every 10 seconds a consumer runs and writes kafka logs to a file (consumer.log) ---");
    println!("---------------------------------------------------------------------------------------");

    let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap();

    let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("topic".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("group".to_owned())
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap();

    let consumer_handle = tokio::spawn(async move {
        loop {
            let polled = consumer.poll().unwrap();
            let message_sets: Vec<MessageSet<'_>> = polled.iter().collect();
            let mut stream = tokio_stream::iter(message_sets);
            while let Some(ms) = stream.next().await {
                for m in ms.messages() {
                    log_consumed(m.clone()).await;
                }
                consumer.consume_messageset(ms).unwrap();
            };
            consumer.commit_consumed().unwrap();
            sleep(Duration::from_secs(10)).await;
        }
    });

    let terminal_handle = tokio::spawn(async move {
        loop {
            print!("> "); // prompt
            tokio::io::stdout().flush().await.unwrap();
            std::io::stdout().flush().unwrap();
            let read = read_from_terminal().await;
            // println!("Read from terminal: {:?}", read);
            if read == "exit" {
                println!("Goodbye.");
                return;
            }
            if read == "show" {
                show_log().await;
                continue;
            }
            if read.contains(":") {
                let split: Vec<&str> = read.split(":").collect();
                let record = Record::from_key_value("topic", split[0], split[1]);
                producer.send(&record).unwrap();
            } else {
                let record = Record::from_value("topic", read);
                producer.send(&record).unwrap();
            };
            println!("Wrote value to producer.");
            println!("------------------------");
        }
    });

    tokio::select! {
        _ = consumer_handle => {
            return;
        },
        _ = terminal_handle => {
            return;
        },
    };
}

async fn read_from_terminal() -> String {
    let mut buf: [u8; 1024] = [0; 1024];
    let bytes_read = tokio::io::stdin().read(&mut buf).await.unwrap();
    let read = &buf[..bytes_read];
    let read_text = std::str::from_utf8(read).unwrap();
    let without_break = &read_text[..(read_text.len() - 1)]; // remove line break
    return without_break.to_owned();
}

async fn log_consumed(msg: &Message<'_>) {
    let mut file = OpenOptions::new().create(true).append(true).truncate(false).open(LOG_FILENAME).await.expect("File can't be opened");
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let log = format!("[{}] {{key={}, offset={:?},value={}}}\n", now, std::str::from_utf8(msg.key).unwrap(), msg.offset, std::str::from_utf8(msg.value).unwrap());
    let formatted = format!("{}", log);
    file.write(&*formatted.into_bytes()).await.expect("File can't be written to");
}

async fn show_log() {
    let file_res = OpenOptions::new().read(true).open(LOG_FILENAME).await;
    if let Err(e) = file_res {
        println!("Unable to read log file: {:?}", e);
        return;
    }
    let mut file = file_res.unwrap();
    let mut buf = [0u8; 1024];
    let bytes_read = file.read(&mut buf).await.expect("Could not read file content");
    let content = std::str::from_utf8(&buf[..bytes_read]).unwrap();
    println!("{}", content);
}
