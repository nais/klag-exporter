use thiserror::Error;

#[derive(Error, Debug)]
pub enum KlagError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Regex error: {0}")]
    Regex(#[from] regex::Error),

    #[error("HTTP server error: {0}")]
    Http(String),

    #[error("OpenTelemetry error: {0}")]
    Otel(String),
}

pub type Result<T> = std::result::Result<T, KlagError>;
