"""
================================================================
Baggage Service — نظام تتبع الأمتعة
================================================================

يطبق المفاهيم الآتية:
  - Consumer Group مستقل تماماً: group_id='baggage-team'
    (منفصل عن checkin-system، عشان نوضح إن كل Group بيشوف
    الرسايل بشكل مستقل، مفيش تعارض بين الاتنين)
  - قراءة من Topic له 4 Partitions
  - حساب Consumer Lag يدوياً لكل Partition (مؤشر مهم في الإنتاج)
"""

import json
from kafka import KafkaConsumer, TopicPartition

BROKERS = ["localhost:9092", "localhost:9093", "localhost:9094"]


def print_consumer_lag(consumer, topic):
    """
    يحسب الفرق بين آخر رسالة موجودة فعلياً في كل Partition،
    وآخر رسالة الـ Consumer وصلها (Consumer Lag).
    """
    partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
    end_offsets = consumer.end_offsets(partitions)

    for tp in partitions:
        current_position = consumer.position(tp)
        lag = end_offsets[tp] - current_position
        print(f"    [LAG] partition={tp.partition} lag={lag} messages")


def main():
    consumer = KafkaConsumer(
        "baggage_events",
        bootstrap_servers=BROKERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="baggage-team",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    print("=== Baggage Service شغال، عضو في Consumer Group: baggage-team ===")

    message_count = 0
    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            for partition, messages in records.items():
                for message in messages:
                    bag = message.value
                    print(
                        f"[BAG UPDATE] partition={message.partition} "
                        f"bag_id={bag['bag_id']} event={bag['event_type']}"
                    )
                    message_count += 1

            if records:
                consumer.commit()

                # كل 5 رسايل نطبع الـ Lag، عشان نراقب صحة الـ Consumer
                if message_count % 5 == 0:
                    print_consumer_lag(consumer, "baggage_events")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
