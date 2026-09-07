# Airport Kafka Project — مشروع مطار بيستخدم Kafka بالكامل

مشروع تطبيقي كامل بيجمع كل مفاهيم Kafka اللي اتعلمناها، باستخدام نفس
مثال المطار من أول الرحلة لحد آخرها.

## خريطة المشروع — كل ملف بيمثل إيه من اللي اتعلمناه؟

| الملف | المفهوم المطبق |
|---|---|
| `docker-compose.yml` | Broker, Cluster, KRaft (3 Brokers بدل ZooKeeper) |
| `setup/create_topics.sh` | Topic, Partition, Replication Factor, Retention Policy, Log Compaction |
| `producers/raw_event_producer.py` | Producer, Message Splitting, Key & Partitioning, acks |
| `consumers/checkin_service.py` | Consumer, Consumer Group, Manual Commit, Idempotency |
| `consumers/baggage_service.py` | Consumer Group مستقل, Consumer Lag |
| `consumers/notifications_service.py` | Consumer Group مختلف يقرأ نفس الـ Topic بشكل مستقل |
| `streams/delay_monitor.py` | Kafka Streams (Faust): Filter + Aggregation (KTable) |
| `connect/*.json` | Kafka Connect: Source Connector و Sink Connector |

---

## 1. تشغيل الـ Kafka Cluster

```bash
docker compose up -d
```

ده هيشغل 3 Brokers (زي ما اتعلمنا في Broker & Cluster)، بالإضافة لواجهة
`kafka-ui` على المتصفح تقدر من خلالها تتفرج بصرياً على الـ Topics
والـ Partitions والـ Consumer Groups:

```
http://localhost:8080
```

انتظر حوالي 30 ثانية لحد ما الـ 3 Brokers يخلصوا الـ Leader Election
بينهم (مفهوم KRaft Controller Quorum).

---

## 2. إنشاء الـ Topics

```bash
cd setup
chmod +x create_topics.sh
./create_topics.sh
```

هيتعمل 5 Topics بإعدادات مختلفة تماماً حسب طبيعة كل بيانات:

- **flight_status**: 3 Partitions، Replication Factor = 3، Retention أسبوعين
  (بيانات حساسة، محتاجة حماية قوية)
- **baggage_events**: 4 Partitions (حجم بيانات أكبر)
- **customs_clearance**: 2 Partitions
- **gate_assignment**: بيستخدم `cleanup.policy=compact` بدل المسح بالوقت
  (عايزين بس آخر بوابة لكل رحلة، مش تاريخ كل التغييرات)
- **delayed_flights_count**: مخرجات تطبيق الـ Streams

---

## 3. تثبيت المكتبات

```bash

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt --break-system-packages
```

---

## 4. تشغيل الـ Producer

```bash
python producers/raw_event_producer.py
```

هيبدأ يبعت "حدث كبير" كل ثانيتين، ويقسمه لـ 4 Topics مختلفة، بالظبط
زي ما اتفقنا في مفهوم **Message Splitting**:

```
حدث flight_arrival_complete
        │
        ├──► flight_status      (acks='all')
        ├──► baggage_events     (acks=1, رسالة لكل شنطة)
        ├──► customs_clearance  (acks='all')
        └──► gate_assignment    (Log Compaction)
```

---

## 5. تشغيل الـ Consumers (في Terminals منفصلة)

```bash
# Terminal 1
python consumers/checkin_service.py

# Terminal 2 — نفس الكود، Kafka هيوزع الـ Partitions أوتوماتيك
python consumers/checkin_service.py

# Terminal 3
python consumers/baggage_service.py

# Terminal 4
python consumers/notifications_service.py
```

### جرب بنفسك — Consumer Group Rebalancing

1. شغّل `checkin_service.py` مرتين في Terminals مختلفة (نفس `group_id`).
2. لاحظ في اللوج إن كل Consumer بياخد Partitions مختلفة.
3. اقفل واحد منهم (Ctrl+C).
4. لاحظ Kafka بيعيد توزيع الـ Partitions على الـ Consumer الباقي
   (مفهوم **Rebalancing** اللي اتعلمناه).

### جرب بنفسك — استقلالية الـ Consumer Groups

شغّل `checkin_service.py` و `notifications_service.py` مع بعض،
هتلاحظ الاتنين بيستقبلوا **نفس** رسايل `flight_status`، لأن كل واحد
منهم في `group_id` مختلف تماماً.

---

## 6. تشغيل تطبيق Kafka Streams

```bash
cd streams
faust -A delay_monitor worker -l info
```

هيراقب `flight_status`، ويفلتر بس الرحلات المتأخرة، ويحسب عداد
مستمر لكل شركة طيران، ويكتب النتيجة في `delayed_flights_count`.

---

## 7. Kafka Connect (اختياري، محتاج Kafka Connect worker شغال)

الملفات في `connect/` هي إعدادات جاهزة (مش كود) توضح إزاي تربط
Kafka بأنظمة خارجية من غير ما تكتب Producer/Consumer يدوي:

- `flight-db-source-connector.json`: يسحب صفوف جديدة من قاعدة بيانات
  ويحطها أوتوماتيك في `flight_status` (**Source Connector**).
- `baggage-elasticsearch-sink-connector.json`: ياخد رسايل من
  `baggage_events` ويحطها في Elasticsearch للأرشفة (**Sink Connector**).

لو عندك Kafka Connect worker شغال على `localhost:8083`:

```bash
cd connect
chmod +x register_connectors.sh
./register_connectors.sh
```

---

## مراجعة سريعة — كل المفاهيم المطبقة في المشروع

```
✅ Topic, Partition, Offset
✅ Producer, Message Splitting, Key-based Partitioning
✅ Consumer, Pull Model, Manual Commit, Idempotency
✅ Consumer Group, Rebalancing, Consumer Lag
✅ Broker, Cluster, KRaft (Controller Quorum)
✅ Replication Factor, Leader/Follower, ISR
✅ acks (0/1/all), min.insync.replicas
✅ Retention Policy (by time) + Log Compaction
✅ Kafka Connect (Source + Sink)
✅ Kafka Streams (Filter + Aggregation/KTable)
```

مبروك — المشروع ده بيغطي كل خطوة من الرحلة اللي مشيناها من أول
"ليه أصلاً نحتاج Kafka؟" لحد معالجة البيانات لحظياً بـ Kafka Streams.
