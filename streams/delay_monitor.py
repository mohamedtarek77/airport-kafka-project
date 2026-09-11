"""
================================================================
Delay Monitor — تطبيق Kafka Streams (باستخدام مكتبة Faust)
================================================================

الفرق الجوهري عن الـ Consumers العادية اللي كتبناها:
هنا مش بس بنقرأ ونعالج رسالة رسالة، إحنا بنعمل:
    1. Filter: نختار بس الرحلات اللي status='delayed'
    2. Table (KTable): بنحتفظ بعداد مستمر لكل رحلة متأخرة
    3. بنكتب النتيجة النهائية في Topic تاني: delayed_flights_count

شغّله بالأمر:
    faust -A streams.delay_monitor worker -l info
"""

import faust

app = faust.App(
    "airport-delay-monitor",
    broker="kafka://localhost:9092",
    value_serializer="json",
)

# Topic المدخل: نفس flight_status اللي بنيناه من البداية
# ملحوظة: من غير value_type، Faust هيفك تشفير JSON تلقائياً
# ويرجع dict عادي (بفضل value_serializer="json" المحدد فوق في الـ App)
flight_topic = app.topic("flight_status")

# Topic المخرج: نتيجة المعالجة (Kafka Streams بيكتب هنا)
delayed_count_topic = app.topic("delayed_flights_count")

# KTable بسيط: بيحتفظ بعدد الرحلات المتأخرة لكل شركة طيران
# ملحوظة: partitions لازم يطابق بالظبط عدد partitions بتاعة
# الـ Topic المصدر (flight_status له 3 partitions)، عشان Kafka
# Streams يقدر يربط كل partition بالـ partition المقابلة لها
# في الـ changelog topic الداخلي
delayed_counts = app.Table(
    "delayed-counts-by-airline",
    default=int,
    partitions=3,
)


@app.agent(flight_topic)
async def monitor_delays(flights):
    """
    المعالج الأساسي: بيشتغل تلقائياً على كل رسالة جديدة توصل
    لـ flight_status، ويطبق منطق الـ Filter + Aggregation.
    """
    async for flight in flights:
        # خطوة الـ Filter
        if flight.get("status") != "delayed":
            continue

        airline = flight.get("airline", "unknown")

        # خطوة الـ Aggregation: زيادة العداد لهذه الشركة
        delayed_counts[airline] += 1

        # بعت النتيجة المحدثة لـ Topic المخرج
        await delayed_count_topic.send(
            value={
                "airline": airline,
                "flight_number": flight["flight_number"],
                "total_delayed_count": delayed_counts[airline],
            }
        )

        print(
            f"[DELAY DETECTED] airline={airline} "
            f"flight={flight['flight_number']} "
            f"total_delayed_so_far={delayed_counts[airline]}"
        )


if __name__ == "__main__":
    app.main()
