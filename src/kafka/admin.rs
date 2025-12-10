use crate::kafka::client::TopicPartition;
use rdkafka::TopicPartitionList;

#[allow(dead_code)]
pub fn build_topic_partition_list(partitions: &[TopicPartition]) -> TopicPartitionList {
    let mut tpl = TopicPartitionList::new();
    for tp in partitions {
        tpl.add_partition(&tp.topic, tp.partition);
    }
    tpl
}
