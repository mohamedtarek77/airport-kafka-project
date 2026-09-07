"""
================================================================
Check-in Service — نظام تسجيل الوصول
================================================================

يطبق المفاهيم الآتية:

- Consumer Group
  group_id = 'checkin-system'

  شغّل الملف ده كذا مرة
  (كذا Process)

  وهتلاقي Kafka بيوزع الـ 3 Partitions
  بتاعة Topic: flight_status عليهم أوتوماتيك

  ده اسمه:
  Rebalancing


- Manual Commit

  عشان نضمن معالجة الرسائل بطريقة
  At-least-once


- Idempotency

  بنتحقق قبل ما نعالج الرسالة

  عشان لو الرسالة اتكررت
  (Duplicate Processing)

  متعملش أي أثر جانبي إضافي


شغّله بالأمر:

    python consumers/checkin_service.py


وافتح كذا Terminal
وشغّل البرنامج فيهم كلهم بنفس الكود

هتشوف إن Kafka بيوزع الـ Partitions
على الـ Consumers تلقائيًا.
"""

import json
from kafka import KafkaConsumer

BROKERS = ["localhost:9092", "localhost:9093", "localhost:9094"]

# تتبع بسيط في الذاكرة
# لمحاكاة حالة "already_processed"
#
# الهدف هو تطبيق مفهوم:
# Idempotency
#
# في نظام حقيقي
# هيكون التتبع ده موجود في جدول داخل قاعدة البيانات
_processed_events = set()


def already_processed(flight_number, timestamp):
    key = f"{flight_number}:{timestamp}"
    if key in _processed_events:
        return True
    _processed_events.add(key)
    return False


def update_gate_display(flight):
    """محاكاة تحديث شاشة العرض في صالة المطار."""
    print(
        f"  [DISPLAY UPDATED] {flight['flight_number']} | "
        f"status={flight['status']} | gate={flight['gate']}"
    )


def main():
    consumer = KafkaConsumer(
        "flight_status",
        bootstrap_servers=BROKERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="checkin-system",
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # Manual Commit للتحكم الكامل
        max_poll_records=50,
    )

    print(
    "=== Check-in Service شغال ==="
    " | "
    "عضو في Consumer Group: checkin-system"
)
    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            for partition, messages in records.items():
                for message in messages:
                    flight = message.value

                    print(
                        f"[RECEIVED] partition={message.partition} "
                        f"offset={message.offset} flight={flight['flight_number']}"
                    )

                    # فحص Idempotency قبل أي معالجة فعلية
                    if already_processed(
                        flight["flight_number"], flight["message_timestamp"]
                    ):
                        print("  [SKIPPED] تم معالجة هذا الحدث من قبل")
                        continue

                    update_gate_display(flight)

            # Commit بعد ما كل الدفعة اتعالجت بنجاح (At-least-once)
            if records:
                consumer.commit()

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
