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

ملحوظة عن الاستقرار:
  مكتبة kafka-python-ng عندها Bug معروف (مش متعلق بمشروعنا) بيظهر
  كـ "ValueError: Invalid file descriptor: -1" لو الاتصال بالـ Broker
  انقطع فجأة (شائع فوق WSL2/Docker Desktop). عشان كده الكود هنا
  بيعيد إنشاء الـ Consumer تلقائياً لو الخطأ ده حصل، بدل ما يقف.
"""

import json
import time
from kafka import KafkaConsumer, TopicPartition

BROKERS = ["localhost:9092", "localhost:9095", "localhost:9094"]


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


def make_consumer():
    return KafkaConsumer(
        "baggage_events",
        bootstrap_servers=BROKERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="baggage-team",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )


def main():
    consumer = make_consumer()
    print("=== Baggage Service شغال، عضو في Consumer Group: baggage-team ===")

    message_count = 0

    while True:  # حلقة خارجية: لو حصل انقطاع، بنعيد الاتصال من هنا
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

        except KeyboardInterrupt:
            consumer.close()
            break

        except Exception as e:
            # هنا بنمسك أي مشكلة اتصال عابرة (زي Bug الـ selector المعروف)
            # ونعيد إنشاء الـ Consumer من جديد بدل ما البرنامج يقف كله
            print(f"[CONNECTION ISSUE] {type(e).__name__}: {e}")
            print("[RECOVERING] بنعيد الاتصال بعد 3 ثواني...")
            try:
                consumer.close()
            except Exception:
                pass
            time.sleep(3)
            consumer = make_consumer()


if __name__ == "__main__":
    main()
