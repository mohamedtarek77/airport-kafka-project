# الدرس 1: Topics و Partitions و Offsets

## الخطوة 0: ليه أصلاً محتاجين Kafka؟

قبل أي مصطلح، لازم نفهم المشكلة اللي Kafka جاي يحلها.

تخيل عندك أنظمة كتير في المطار: نظام تسجيل الوصول، نظام بوابات الصعود، نظام تتبع الأمتعة، نظام الإعلانات الصوتية. كل نظام من دول بيحصل فيه "أحداث" (`events`) طول الوقت: راكب سجل وصوله، حقيبة اتحملت، رحلة اتأخرت.

المشكلة: لو كل نظام عايز يبلغ الأنظمة التانية بحدث حصل، هتحتاج كل نظام يتكلم مباشرة مع كل نظام تاني (اتصال مباشر لكل زوج أنظمة) — ده تعقيد كبير وبيكبر بسرعة كل ما زاد عدد الأنظمة.

الحل: نظام وسيط واحد، كل الأنظمة تكتب فيه الأحداث بتاعتها، وكل نظام محتاج يعرف بحدث معين "يشترك" ويقرأ منه. ده بالظبط دور Kafka — نظام لتبادل الأحداث (`event streaming platform`) بين الأنظمة المختلفة، بدل الاتصال المباشر.

```
من غير Kafka (تعقيد N×N):          مع Kafka (نظام وسيط واحد):

  CheckIn ──▶ Boarding                 CheckIn ──┐
  CheckIn ──▶ Baggage                  Boarding ─┤
  Boarding ──▶ Baggage                 Baggage ──┼──▶  Kafka  ──▶  أي نظام يشترك
  Boarding ──▶ Announcements           Announce ─┘
  ... (اتصالات كتير متشابكة)
```

## الخطوة 1: إيه هي الرسالة (Message)؟

أبسط وحدة في Kafka هي الـ `Message` (أو `Record`) — قطعة بيانات واحدة بتمثل حدث حصل، زي: "الراكب أحمد سجل وصوله على رحلة 101 الساعة 10:00".

الرسالة دي، بمجرد ما تتكتب في Kafka، **ثابتة ومتغيرش (`immutable`)** — مافيش تعديل عليها بعد الكتابة، بس ممكن تتمسح بعد فترة (هنشرح ده بالتفصيل في درس الـ Retention لاحقًا).

## الخطوة 2: فين بتتخزن الرسائل؟ الـ Topic

مش منطقي كل الرسائل (بتاعة تسجيل الوصول، وبتاعة الأمتعة، وبتاعة الإعلانات) تتخزن في مكان واحد مخلوط ببعض. عشان كده Kafka بيديك `Topic` — قناة منطقية منفصلة، زي جدول أو فئة، لكل نوع بيانات.

مثلاً:
- `Topic` اسمه `flights.checkin` لأحداث تسجيل الوصول.
- `Topic` اسمه `flights.baggage` لأحداث الأمتعة.

فكر في الـ `Topic` كأنه **سجل مفتوح (`log`)**: الرسائل بتتضاف في الآخر بالترتيب، وما بتتمسحش ولا بتتغيرش فورًا، وأي عدد من الأنظمة يقدر "يقرأ" منه من غير ما يأثر على غيره.

```
Topic: flights.checkin
[msg: "Ahmed checked in"] → [msg: "Sara checked in"] → [msg: "Omar checked in"] → ...
(كل رسالة بتتضاف في الآخر، والترتيب ده ثابت)
```

## الخطوة 3: مشكلة الـ Topic الواحد لو كبر الحمل

لو كل الرسائل بتاعة `Topic` واحد بتتخزن في مكان واحد فيزيائي، وييجي عليه حمل ضخم (ملايين الرسائل)، هتقابل مشكلتين:

1. **مافيش تنافس (`parallelism`)**: نظام واحد بس ممكن يقرأ بسرعة معينة، مايقدرش يتوزع.
2. **مافيش قابلية توسع (`scalability`)**: الـ `Topic` محبوس على جهاز واحد، مايقدرش يكبر أكتر من طاقة الجهاز ده.

## الخطوة 4: الحل — تقسيم الـ Topic لـ Partitions

عشان نحل المشكلة دي، Kafka بيقسم الـ `Topic` الواحد لعدد من الأجزاء المستقلة اسمها `Partitions`. كل `Partition` هي فعليًا "سجل" (`log`) منفصل بيقدر يتخزن على جهاز مختلف، ويتقرأ باستقلالية عن باقي الـ `Partitions`.

```
Topic: flights.checkin  (مقسم لـ 3 partitions)

Partition 0: [msg][msg][msg]...
Partition 1: [msg][msg]...
Partition 2: [msg][msg][msg][msg]...
```

الفايدة المباشرة:
- **التوازي**: كل `Partition` ممكن تتقرأ بواسطة نظام/عملية مختلفة في نفس الوقت.
- **التوزيع**: الـ `Partitions` ممكن تتوزع على أجهزة مختلفة (`Brokers` — هنشرحها بالتفصيل في درس لاحق).

## الخطوة 5: إزاي نحدد مكان رسالة معينة جوه الـ Partition؟ الـ Offset

بما إن كل `Partition` هي سجل بيتضاف فيه بالترتيب، Kafka بيدي كل رسالة جوه الـ `Partition` رقم تسلسلي ثابت — ده هو الـ `Offset`. زي رقم الصف في جدول: أول رسالة رقمها 0، اللي بعدها 1، وهكذا.

```
Partition 0:
┌────────┬────────┬────────┬────────┐
│offset:0│offset:1│offset:2│offset:3│
└────────┴────────┴────────┴────────┘
```

**نقطة مهمة جدًا**: الـ `Offset` رقمه مستقل لكل `Partition` لوحدها. يعني `Partition 0` ليها `offset:0`، و`Partition 1` كمان ليها `offset:0` — ده رقمين مختلفين تمامًا لرسالتين مختلفتين، مش نفس الرسالة.

```
Partition 0: offset:0 → "Ahmed checked in"
Partition 1: offset:0 → "Sara checked in"   ← offset مختلف تمامًا، مجرد نفس الرقم بالصدفة
```

## الخطوة 6: ترتيب الرسائل — مضمون فين بالظبط؟

نقطة دقيقة لازم نوضحها: Kafka بيضمن الترتيب **جوه نفس الـ Partition بس**. يعني لو رسالتين اتكتبوا في نفس الـ `Partition`، هتتقروا بنفس الترتيب اللي اتكتبوا بيه بالظبط. لكن لو الرسالتين في partitions مختلفة، **مافيش ضمان** إن واحدة هتتقرأ قبل التانية.

```
Partition 0: [event A] → [event B]   ← الترتيب A ثم B مضمون 100%

Partition 0: [event A]        Partition 1: [event C]
لا يوجد ضمان إن A هتتقرأ قبل C أو بعدها — كل partition مستقلة زمنيًا عن التانية
```

ده هيبقى مهم جدًا لما نتكلم عن اختيار الـ `Partition` المناسبة للرسالة في الدرس الجاي (Producers)، لأنه هو اللي بيحدد هل رسائل معينة هيكون بينها ترتيب مضمون ولا لأ.

## ملخص الخطوات

```
Message (حدث واحد)
    │
    ▼
Topic (قناة منطقية لنوع بيانات معين — "سجل" منطقي)
    │
    ▼  (بينقسم لـ)
Partitions (أجزاء مستقلة قابلة للتوزيع والتوازي)
    │
    ▼  (كل رسالة جواها ليها)
Offset (رقم تسلسلي ثابت يحدد مكانها بالظبط جوه الـ partition)
```

> **ملاحظة**: الـ `Offset` اللي شرحناه هنا هو المفهوم الأساسي البسيط. لما نوصل لدرس الـ `Consumers`، هنكتشف إن فيه أكتر من "منظور" لرقم الـ offset (فين آخر رسالة اتكتبت، وفين وصل القارئ، وفين آخر نقطة سجلها) — لكن ده مبني فوق نفس الأساس اللي فهمناه هنا، مش مفهوم مختلف.

## مثال بكود Python (`kafka-python-ng`)

نقدر ننشئ `Topic` بعدد partitions محدد باستخدام `KafkaAdminClient`:

```python
from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(bootstrap_servers="localhost:9092")

topic = NewTopic(
    name="flights.checkin",
    num_partitions=3,       # هنا بنحدد عدد الـ Partitions
    replication_factor=1,   # هنشرح الرقم ده بالتفصيل في درس الـ Replication
)

admin.create_topics([topic])
admin.close()
```

ونقدر نشوف عدد الـ partitions وحالة كل واحدة فيهم (بما فيها آخر offset متاح) باستخدام `KafkaConsumer`:

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers="localhost:9092")

partitions = consumer.partitions_for_topic("flights.checkin")
print(f"عدد الـ Partitions: {len(partitions)}")  # {0, 1, 2}

for p in partitions:
    from kafka import TopicPartition
    tp = TopicPartition("flights.checkin", p)
    end_offset = consumer.end_offsets([tp])[tp]
    print(f"Partition {p} → آخر offset متاح (LEO): {end_offset}")

consumer.close()
```

## تمرين صغير

لو عندك `Topic` اسمه `flights.boarding` وعليه 3 partitions فاضية تمامًا (يعني لسه مافيهاش أي رسالة):

- لو وصلت أول رسالة على `Partition 1`، هي هتاخد offset رقم كام؟
- لو بعد كده وصلت رسالة على `Partition 0`، هي هتاخد offset رقم كام؟ ليه؟
- هل ترتيب وصول الرسالتين دول (اللي على Partition 1 واللي على Partition 0) مضمون بينهم، ولا لأ؟

---
◀ [الفهرس](./00-index.md) | التالي ▶ [الدرس 2: Producers](./02-producers.md)
