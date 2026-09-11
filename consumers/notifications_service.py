"""
================================================================
Notifications Service — نظام إشعارات الركاب
================================================================

الهدف من الملف ده تحديداً: إثبات عملي لمفهوم مهم:

    نفس الرسالة (flight_status) بتتقرأ مرتين بالظبط:
    مرة من checkin_service.py (Group: checkin-system)
    ومرة من هنا (Group: notification-system)

    كل Group ليه Offsets خاصة به تماماً، ومحدش بيأثر على التاني.

ملحوظة عن الاستقرار:
  مكتبة kafka-python-ng عندها Bug معروف (مش متعلق بمشروعنا) بيظهر
  كـ "ValueError: Invalid file descriptor: -1" لو الاتصال بالـ Broker
  انقطع فجأة (شائع فوق WSL2/Docker Desktop). عشان كده الكود هنا
  بيعيد إنشاء الـ Consumer تلقائياً لو الخطأ ده حصل، بدل ما يقف.

شغّل الملف ده بالتوازي مع checkin_service.py وهتلاقي الاتنين
بيستقبلوا نفس تحديثات الرحلات، في نفس الوقت تقريباً.
"""

import json
import time
from kafka import KafkaConsumer

BROKERS = ["localhost:9092", "localhost:9095", "localhost:9094"]


def send_sms_notification(flight):
    """محاكاة بعت SMS للركاب عند تأخير أو إلغاء الرحلة."""
    if flight["status"] in ("delayed", "cancelled"):
        print(
            f"  [SMS SENT] إشعار للركاب: رحلة {flight['flight_number']} "
            f"حالتها: {flight['status']}"
        )


def make_consumer():
    return KafkaConsumer(
        "flight_status",
        bootstrap_servers=BROKERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="notification-system",  # Group مختلف تماماً عن checkin-system
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )


def main():
    consumer = make_consumer()
    print("=== Notifications Service شغال، عضو في Consumer Group: notification-system ===")

    while True:  # حلقة خارجية: لو حصل انقطاع، بنعيد الاتصال من هنا
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

        except KeyboardInterrupt:
            consumer.close()
            break

        except Exception as e:
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
