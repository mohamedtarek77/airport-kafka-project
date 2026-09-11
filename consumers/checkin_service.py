"""
================================================================
Check-in Service — نظام تسجيل الوصول
================================================================

يطبق المفاهيم الآتية:
  - Consumer Group: group_id='checkin-system'
    شغّل الملف ده كذا مرة (كذا process) وهتلاقي Kafka بيوزع
    الـ 3 Partitions بتاعة flight_status عليهم أوتوماتيك (Rebalancing)
  - Manual Commit: عشان نضمن At-least-once معالجة صح
  - Idempotency: بنتحقق قبل ما نعالج، عشان لو الرسالة اتكررت
    (Duplicate Processing) متعملش أي أثر جانبي إضافي

ملحوظة عن الاستقرار:
  مكتبة kafka-python-ng عندها Bug معروف (مش متعلق بمشروعنا) بيظهر
  كـ "ValueError: Invalid file descriptor: -1" لو الاتصال بالـ Broker
  انقطع فجأة (شائع فوق WSL2/Docker Desktop). عشان كده الكود هنا
  بيعيد إنشاء الـ Consumer تلقائياً لو الخطأ ده حصل، بدل ما يقف.

شغّله بالأمر:
    python consumers/checkin_service.py
وافتح كذا Terminal وشغله فيهم كلهم بنفس الكود، هتشوف كل Consumer
بياخد Partitions مختلفة.
"""

import json
import time
from kafka import KafkaConsumer

BROKERS = ["localhost:9092", "localhost:9095", "localhost:9094"]

# تتبع بسيط في الذاكرة لمحاكاة "already_processed" (Idempotency)
# في نظام حقيقي، ده هيكون جدول في قاعدة بيانات
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


def make_consumer():
    return KafkaConsumer(
        "flight_status",
        bootstrap_servers=BROKERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="checkin-system",
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # Manual Commit للتحكم الكامل
        max_poll_records=50,
    )


def main():
    consumer = make_consumer()
    print("=== Check-in Service شغال، عضو في Consumer Group: checkin-system ===")

    while True:  # حلقة خارجية: لو حصل انقطاع، بنعيد الاتصال من هنا
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
