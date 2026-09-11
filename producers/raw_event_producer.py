"""
================================================================
Raw Event Producer — نظام حالة الرحلات المركزي في المطار
================================================================

هذا الملف بيطبق مفهوم "Message Splitting" اللي اتعلمناه:
حدث واحد كبير (flight_arrival_complete) بيوصل من نظام المطار المركزي،
والـ Producer هو المسؤول عن تفكيكه وتوزيعه على 3 Topics منفصلة:

    flight_status      -> حالة الرحلة نفسها
    baggage_events      -> كل شنطة (ممكن تكون أكتر من شنطة، فبتتبعت رسايل منفصلة)
    customs_clearance   -> بيانات الراكب مع الجمارك

يطبق أيضاً مفهوم acks:
    - flight_status و customs_clearance: acks='all' (بيانات حساسة)
    - baggage_events: acks=1 (بيانات بتتكرر، فقدان رسالة واحدة أقل خطورة)
"""

import json
import time
import random
from kafka import KafkaProducer

BROKERS = ["localhost:9092", "localhost:9095", "localhost:9094"]


def make_producer(acks_level):
    """ينشئ Producer بمستوى acks محدد، حسب حساسية البيانات."""
    return KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=acks_level,
    )


# Producer لكل نوع بيانات، بمستوى acks يناسب حساسيتها
critical_producer = make_producer("all")   # flight_status + customs_clearance
frequent_producer = make_producer(1)        # baggage_events


def process_and_split(raw_event):
    """
    الدالة الأساسية: بتاخد الحدث الكبير وتفككه لثلاث رسايل منفصلة،
    كل واحدة برسالتها الخاصة وبالـ Key المناسب لها.
    """

    # 1) جزء حالة الرحلة -> Topic: flight_status
    #    الـ Key = رقم الرحلة، عشان كل تحديثات نفس الرحلة تروح لنفس الـ Partition
    #    وبالتالي يحافظوا على ترتيبهم الزمني الصحيح
    flight_data = raw_event["flight"]
    critical_producer.send(
        topic="flight_status",
        key=flight_data["flight_number"],
        value=flight_data,
    )

    # 2) جزء الأمتعة -> Topic: baggage_events
    #    ممكن يكون فيه أكتر من شنطة، فبنعمل loop ونبعت رسالة منفصلة لكل شنطة
    #    الـ Key = bag_id، عشان تاريخ كل شنطة يفضل مترتب في نفس الـ Partition
    for bag in raw_event["baggage"]:
        frequent_producer.send(
            topic="baggage_events",
            key=bag["bag_id"],
            value=bag,
        )

    # 3) جزء الجمارك -> Topic: customs_clearance
    customs_data = raw_event["customs"]
    critical_producer.send(
        topic="customs_clearance",
        key=customs_data["passenger_id"],
        value=customs_data,
    )

    # 4) تحديث آخر بوابة للرحلة -> Topic: gate_assignment (Log Compaction)
    #    هنا بنستخدم Producer عادي (مش critical) لأن ده مجرد "آخر حالة"
    frequent_producer.send(
        topic="gate_assignment",
        key=flight_data["flight_number"],
        value={"gate": flight_data["gate"], "terminal": flight_data["terminal"]},
    )

    # نضمن إن الرسايل اتبعتت فعلياً قبل ما نكمل
    critical_producer.flush()
    frequent_producer.flush()


def generate_sample_event(flight_number, airline, status, gate):
    """بيبني حدث كبير واحد، زي اللي بيوصل من نظام المطار المركزي."""
    now = time.strftime("%Y-%m-%dT%H:%M:%S") + f".{random.randint(100000, 999999)}Z"
    return {
        "event_id": f"EVT-{random.randint(10000, 99999)}",
        "event_type": "flight_arrival_complete",
        "flight": {
            "flight_number": flight_number,
            "airline": airline,
            "status": status,
            "gate": gate,
            "terminal": str(random.randint(1, 3)),
            "message_timestamp": now,
        },
        "baggage": [
            {
                "bag_id": f"BG{random.randint(1000, 9999)}",
                "flight_number": flight_number,
                "passenger_id": f"P{random.randint(1000, 9999)}",
                # نولد نوع حدث عشوائي، بنفس الأنواع اللي اتفقنا عليها
                # في مثال المطار: تحميل، تنزيل، على السير، أو فقدان نادر
                "event_type": random.choices(
                    ["loaded_on_plane", "unloaded", "on_belt", "bag_lost"],
                    weights=[30, 30, 35, 5],  # bag_lost نادر عن قصد
                    k=1,
                )[0],
                "message_timestamp": now,
            }
            for _ in range(random.randint(1, 3))
        ],
        "customs": {
            "passenger_id": f"P{random.randint(1000, 9999)}",
            "flight_number": flight_number,
            "event_type": "arrived_customs",
            "message_timestamp": now,
        },
    }


if __name__ == "__main__":
    flights = [
        ("EK1", "Emirates", "landed", "A12"),
        ("QR2", "Qatar Airways", "delayed", "B04"),
        ("EY3", "Etihad", "boarding", "C21"),
    ]

    print("=== Raw Event Producer شغال، هيبعت أحداث للمطار كل 2 ثانية ===")
    while True:
        flight_number, airline, status, gate = random.choice(flights)
        event = generate_sample_event(flight_number, airline, status, gate)

        process_and_split(event)

        print(
            f"[SENT] flight={flight_number} status={status} "
            f"bags={len(event['baggage'])} -> flight_status, baggage_events, "
            f"customs_clearance, gate_assignment"
        )
        time.sleep(2)
