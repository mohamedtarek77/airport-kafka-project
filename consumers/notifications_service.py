"""
================================================================
Notifications Service — نظام إشعارات الركاب
================================================================

الهدف من الملف ده تحديداً:
إثبات مفهوم مهم

نفس الرسالة
(flight_status)

بتتقرأ مرتين بالظبط:

المرة الأولى:
checkin_service.py
Consumer Group: checkin-system

المرة الثانية:
notifications_service.py
Consumer Group: notification-system


كل Consumer Group
ليه Offsets خاصة به تماماً

ومحدش بيأثر على التاني.


شغّل الملف ده بالتوازي مع:

    checkin_service.py

وهتلاقي الاتنين بيستقبلوا
نفس تحديثات الرحلات

في نفس الوقت تقريباً.
"""

import json
from kafka import KafkaConsumer

BROKERS = ["localhost:9092", "localhost:9093", "localhost:9094"]


def send_sms_notification(flight):
    """محاكاة بعت SMS للركاب عند تأخير أو إلغاء الرحلة."""
    if flight["status"] in ("delayed", "cancelled"):
        print(
            f"  [SMS SENT] إشعار للركاب: رحلة {flight['flight_number']} "
            f"حالتها: {flight['status']}"
        )


def main():
    consumer = KafkaConsumer(
        "flight_status",
        bootstrap_servers=BROKERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="notification-system",  # Group مختلف تماماً عن checkin-system
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    print("=== Notifications Service شغال، عضو في Consumer Group: notification-system ===")

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            for partition, messages in records.items():
                for message in messages:
                    flight = message.value
                    print(
                        f"[NOTIFICATION CHECK] partition={message.partition} "
                        f"flight={flight['flight_number']} status={flight['status']}"
                    )
                    send_sms_notification(flight)

            if records:
                consumer.commit()

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
