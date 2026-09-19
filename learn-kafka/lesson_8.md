<div dir="rtl">

# الدرس 8: Retention Policies

## الخطوة 0: سؤال منطقي — الرسالة بتفضل موجودة لحد إمتى؟

من درس الـ `Consumers` فهمنا إن الـ `Consumer` بيقرأ الرسالة، وممكن يعمل commit عليها. لكن هل الرسالة بعد كده بتتمسح فورًا؟ الإجابة: **لأ**. الرسائل في Kafka بتفضل محفوظة لفترة معينة، سواء اتقرأت أو لأ — ده عكس تمامًا بعض أنظمة الـ `Message Queues` التقليدية اللي بتمسح الرسالة بمجرد ما حد يقراها.

## الخطوة 1: ليه القرار ده مهم؟

بما إن الرسالة مش بتتمسح بمجرد القراءة، بيبقى ممكن:

- أكتر من `Consumer Group` (فاكر درس Consumer Groups؟) يقرأوا نفس الـ `Topic` في أوقات مختلفة تمامًا.
- `Consumer Group` جديد ينضم بعد أسابيع ويقدر يرجع يقرأ من البداية لو عايز.
- لو حصل bug في نظام المعالجة، تقدر ترجع تعالج نفس البيانات تاني من تاريخ معين.

لكن ده معناه كمان إن البيانات هتفضل تكبر باستمرار على القرص — فمحتاجين سياسة تحدد "لحد إمتى بالظبط نحتفظ بالرسالة؟" — ده هو الـ `Retention`.

## الخطوة 2: أول سياسة — Time-based Retention

أبسط سياسة: الرسالة بتتمسح تلقائيًا بعد ما تعدي مدة زمنية معينة من وقت كتابتها، بغض النظر هل اتقرأت أو لأ.

```
retention.ms = 604800000  (7 أيام بالمللي ثانية)

رسالة اتكتبت يوم 1 → هتتمسح تلقائيًا يوم 8
```

## الخطوة 3: مشكلة الوقت لوحده — لو الحجم كبر أوي فجأة؟

تخيل يوم فيه حمل ضخم جدًا من الرسائل (مثلًا كل رحلات المطار اتأخرت في يوم واحد وبقى فيه رسائل كتير جدًا). لو اعتمدنا على الوقت بس، القرص ممكن يمتلئ قبل ما الـ 7 أيام يعدوا. محتاجين حماية إضافية بناءً على الحجم.

## الخطوة 4: ثاني سياسة — Size-based Retention

بمجرد ما حجم الـ `Partition` يتخطى حد معين، أقدم الرسائل بتتمسح عشان تفضي مكان للجديدة، حتى لو مدة الـ `retention.ms` لسه ماخلصتش.

```
retention.bytes = 1073741824  (1 GB)

لو الـ partition وصل لـ 1GB → أقدم رسائل هتتمسح لحد ما يرجع تحت الحد
```

**ملحوظة مهمة**: لو الاتنين (`retention.ms` و`retention.bytes`) مضبوطين مع بعض على نفس الـ `Topic`، Kafka بيطبق أنهي حد يوصله الأول (`whichever comes first`) — يعني الاتنين بيشتغلوا كحماية مزدوجة، مش بديل عن بعض.

## الخطوة 5: مشكلة تالتة — لو عايزين "آخر حالة" بس مش كل التاريخ؟

فيه حالة استخدام مختلفة تمامًا: تخيل `Topic` بيمثل "آخر حالة معروفة لكل رحلة" (`key = flight_id`، القيمة = الحالة الحالية). هنا مش منطقي نمسح بناءً على وقت أو حجم بس — عايزين نحتفظ *دايمًا* بآخر قيمة لكل `flight_id`، ونمسح بس النسخ القديمة المكررة لنفس المفتاح.

## الخطوة 6: الحل — Log Compaction

`Log Compaction` سياسة مختلفة تمامًا عن الاتنين اللي فاتوا: بدل ما تمسح حسب الوقت أو الحجم، بيحتفظ *بآخر قيمة* لكل `key` بس، ويمسح النسخ الأقدم لنفس الـ key.

```
قبل الـ Compaction:
[key=A,v=1] [key=B,v=1] [key=A,v=2] [key=A,v=3] [key=B,v=2]

بعد الـ Compaction:
[key=A,v=3] [key=B,v=2]   ← بس آخر قيمة لكل key
```

ده مفيد جدًا للحالات اللي محتاج فيها "آخر حالة معروفة" بس، مش تاريخ كامل من الأحداث — تمامًا زي مثال "آخر موقع/حالة لكل رحلة" اللي فتحنا بيه.

## الخطوة 7: تطبيق عملي من المشروع

في مشروع المطار، طبقنا الفرق ده بشكل واقعي:

- بيانات الأحداث اللحظية زي `boarding_events` أو `gate_announcements` → `Time-based Retention` قصيرة نسبيًا، لأننا مش محتاجين نحتفظ بيها فترة طويلة بعد ما تتعالج فعليًا.
- الـ `changelog topics` الخاصة بتطبيق الـ `Faust` (اللي بتخزن الحالة الداخلية للـ `KTable` — هنشرح ده بالتفصيل في درس Kafka Streams) → استخدمنا معاها `cleanup.policy=compact`، لأنها بالظبط حالة "آخر قيمة لكل key".

## مثال بكود Python (`kafka-python-ng`)

إنشاء `Topic` بسياسة `Time-based` و`Size-based` مع بعض (خطوة 2 و4):

```python
from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(bootstrap_servers="localhost:9092")

topic = NewTopic(
    name="flights.boarding",
    num_partitions=3,
    replication_factor=3,
    topic_configs={
        "retention.ms": "604800000",     # 7 أيام
        "retention.bytes": "1073741824", # 1 GB
    },
)

admin.create_topics([topic])
admin.close()
```

إنشاء `Topic` بسياسة `Log Compaction` (خطوة 6) — مطابق لإعداد الـ changelog topics في مشروع الـ Faust:

```python
compacted_topic = NewTopic(
    name="flights.current-status-changelog",
    num_partitions=3,
    replication_factor=3,
    topic_configs={
        "cleanup.policy": "compact",   # الاحتفاظ بآخر قيمة لكل key بس
    },
)

admin.create_topics([compacted_topic])
```

تعديل سياسة الـ `Retention` لـ `Topic` موجود بالفعل:

```python
from kafka.admin import ConfigResource, ConfigResourceType

config = ConfigResource(ConfigResourceType.TOPIC, "flights.boarding")
admin.alter_configs([config])  # مع تمرير القيم الجديدة المطلوبة لـ retention.ms مثلاً
```

## تمرين صغير

- لو عندك `Topic` بيسجل "آخر موقع معروف لكل طائرة" (`key = tail_number`)، أي سياسة `Retention` الأنسب وليه؟
- لو غيرت `retention.ms` لقيمة أقل من الوقت اللي فات بالفعل على بعض الرسائل، هيحصل إيه فورًا؟

---
◀ [الدرس السابق: ZooKeeper vs. KRaft](./07-zookeeper-vs-kraft.md) | [الفهرس](./00-index.md) | التالي ▶ [الدرس 9: Kafka Connect](./09-kafka-connect.md)

</div>
