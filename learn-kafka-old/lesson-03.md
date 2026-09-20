<div dir="rtl" align="right">

# Kafka Producer و Partition — الشرح الكامل

**محتويات الملف:**

1. الجزء الأول: Producer — شكل الرسالة الحقيقية والكود اللي بيبعتها
2. الجزء التاني: بيانات تجريبية لكل Topic في مثال المطار
3. الجزء التالت: تقسيم الرسالة الكبيرة على أكتر من Topic (Message Splitting)
4. الجزء الرابع: Partition — جزء مستقل من الـ Topic

---

# الجزء الأول: Producer — شكل الرسالة الحقيقية

تمام! هنقف عند مفهوم Producer وهنوريك بالظبط شكل البيانات الحقيقية (Real Data) اللي بتتبعت للـ Kafka Broker، باستخدام مثال شركة الطيران EK1.

## مثال حقيقي: شركة طيران بتبعت حدث "الرحلة وصلت"

### 1. شكل الرسالة الكاملة اللي بتوصل لـ Kafka Broker

الرسالة في Kafka مش مجرد "نص عشوائي" — ليها بنية محددة (Structure) فيها أجزاء أساسية:

<div dir="ltr" align="left">

```text
┌─────────────────────────────────────────────────┐
│                  Kafka Message                    │
├─────────────────────────────────────────────────┤
│  Topic:      flight_status                        │
│  Key:        "EK1"                                 │
│  Value:      { ...flight data... }                 │
│  Headers:    { source: "ek-ops-system" }           │
│  Timestamp:  1725184500000                         │
│  Partition:  (Kafka هيحددها، هنشرح ده بعدين)        │
│  Offset:     (Kafka هيحددها أوتوماتيك)              │
└─────────────────────────────────────────────────┘
```

</div>

**شرح كل جزء بالعربي:**

- <code dir="ltr">Topic: flight_status</code> — ده اسم القناة اللي الرسالة رايحة لها.
- <code dir="ltr">Key: "EK1"</code> — الـ Key ده مهم جداً، هو رقم الرحلة. Kafka بيستخدمه عشان يحدد الرسالة دي هتروح لأنهي Partition (هنشرحها بالتفصيل بعدين). المهم دلوقتي: كل رسايل ليها نفس الـ Key بتروح لنفس الـ Partition دايماً.
- <code dir="ltr">Value: { ...flight data... }</code> — ده المحتوى الفعلي، البيانات الحقيقية اللي عايزين نوصلها.
- **Headers** — معلومات إضافية اختيارية، زي "مين اللي بعت الرسالة دي". مش جزء أساسي من البيانات نفسها، لكنها بتساعد في التتبع (Tracing).
- **Timestamp** — الوقت اللي اترسلت فيه الرسالة (بصيغة Unix time بالميلي ثانية).
- **Offset** — Kafka نفسه هو اللي بيحدد الرقم ده أوتوماتيك، مش الـ Producer. هنشرحه بالتفصيل في مفهوم قادم.

### 2. شكل الـ Value الحقيقي (JSON)

ده أهم جزء — البيانات الفعلية اللي جوه الرسالة:

<div dir="ltr" align="left">

```json
{
  "flight_number": "EK1",
  "airline": "Emirates",
  "origin": "DXB",
  "destination": "JFK",
  "status": "landed",
  "scheduled_time": "2026-09-01T10:00:00Z",
  "actual_time": "2026-09-01T10:15:00Z",
  "gate": "A12",
  "terminal": "3"
}
```

</div>

### 3. الكود الحقيقي اللي بيبعت الرسالة دي (Python)

<div dir="ltr" align="left">

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

flight_event = {
    "flight_number": "EK1",
    "airline": "Emirates",
    "origin": "DXB",
    "destination": "JFK",
    "status": "landed",
    "scheduled_time": "2026-09-01T10:00:00Z",
    "actual_time": "2026-09-01T10:15:00Z",
    "gate": "A12",
    "terminal": "3"
}

producer.send(
    topic='flight_status',
    key=b'EK1',
    value=flight_event
)

producer.flush()
```

</div>

**شرح الكود سطر بسطر بالعربي:**

<div dir="ltr" align="left">

```python
from kafka import KafkaProducer
```

</div>

بنستورد المكتبة اللي بتخلينا نتصل بـ Kafka كـ Producer.

<div dir="ltr" align="left">

```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    ...
)
```

</div>

بننشئ Producer ونديله عنوان الـ Kafka Broker اللي هيتصل بيه (زي رقم تليفون تتصل بيه).

<div dir="ltr" align="left">

```python
value_serializer=lambda v: json.dumps(v).encode('utf-8')
```

</div>

Kafka مش بيفهم Python Dictionary مباشرة — لازم نحول البيانات لـ Bytes. السطر ده بيحول الـ dictionary لـ JSON نص، وبعدين لـ bytes، أوتوماتيك مع كل رسالة.

<div dir="ltr" align="left">

```python
flight_event = { ... }
```

</div>

ده الـ Dictionary اللي فيه بيانات الرحلة الحقيقية.

<div dir="ltr" align="left">

```python
producer.send(
    topic='flight_status',
    key=b'EK1',
    value=flight_event
)
```

</div>

السطر الأهم! بنقول لـ Kafka:

- ابعت الرسالة دي للـ Topic اسمه `flight_status`
- استخدم `EK1` كـ Key (لازم يبقى bytes، عشان كده حطينا `b` قبلها)
- المحتوى (Value) هو الـ dictionary اللي عملناه

<div dir="ltr" align="left">

```python
producer.flush()
```

</div>

ده بيتأكد إن الرسالة فعلاً اترسلت للـ Broker قبل ما نكمل الكود، مش لسه في الطريق (Kafka عادةً بيبعت الرسايل في دفعات/Batches لأسباب أداء، فـ `flush()` بيجبرها تتبعت فوراً).

### 4. ماذا يحدث داخل Kafka بعد استقبال الرسالة دي؟

1. Kafka Broker يستقبل الرسالة من Producer
2. Kafka يشوف الـ Key = `"EK1"`
3. Kafka يحسب Hash للـ Key ده
4. بناءً على الـ Hash، يحدد الرسالة تروح لأنهي Partition
5. الرسالة تتكتب في آخر الـ Log بتاع الـ Partition ده
6. Kafka يدي الرسالة رقم Offset جديد (تسلسلي)
7. Kafka يرجع تأكيد (Acknowledgment) للـ Producer إن الرسالة اتخزنت بنجاح

### 5. الخطأ الشائع

المبتدئين غالباً بينسوا إن الـ Value لازم يتحول لـ bytes قبل ما يتبعت — Kafka مش بيفهم Python objects أو Dictionaries مباشرة، عشان كده استخدمنا `value_serializer`.

كمان بينسوا `producer.flush()`، فبيفتكروا الرسالة اترسلت فوراً وهي لسه في طريقها.

### تمرين صغير

جرب تكتب لوحدك رسالة (Value) لحدث "شنطة اتحملت على السير رقم 5" عشان تتبعت لـ `Topic: baggage_events`. اكتب الـ JSON بس (زي مثال الـ `flight_event`)، وأنا هراجعها معاك.

---

# الجزء التاني: بيانات تجريبية لكل Topic في مثال المطار

تمام! هنعمل نفس الأسلوب بالظبط — يعني كل Topic هيكون ليه مجموعة رسايل (array) بنفس شكل الـ JSON اللي شفناه في مثال السفن، لكن بمحتوى مناسب لكل موضوع.

## Topic: `flight_status` ✈️

<div dir="ltr" align="left">

```json
[
  {"flight_number":"EK1","airline":"Emirates","origin":"DXB","destination":"JFK","status":"landed","scheduled_time":"2026-09-01T10:00:00Z","actual_time":"2026-09-01T10:15:00Z","gate":"A12","terminal":"3","message_timestamp":"2026-09-01T10:15:02.417684Z"},
  {"flight_number":"QR2","airline":"Qatar Airways","origin":"DOH","destination":"LHR","status":"delayed","scheduled_time":"2026-09-01T09:30:00Z","actual_time":null,"gate":"B04","terminal":"1","message_timestamp":"2026-09-01T10:15:11.208390Z"},
  {"flight_number":"EY3","airline":"Etihad","origin":"AUH","destination":"CDG","status":"boarding","scheduled_time":"2026-09-01T10:45:00Z","actual_time":null,"gate":"C21","terminal":"2","message_timestamp":"2026-09-01T10:15:18.903512Z"},
  {"flight_number":"EK1","airline":"Emirates","origin":"DXB","destination":"JFK","status":"taxiing","scheduled_time":"2026-09-01T10:00:00Z","actual_time":"2026-09-01T10:18:00Z","gate":"A12","terminal":"3","message_timestamp":"2026-09-01T10:18:34.117290Z"}
]
```

</div>

## Topic: `baggage_events` 🧳

<div dir="ltr" align="left">

```json
[
  {"bag_id":"SV4521","flight_number":"EK1","passenger_id":"P8921","event_type":"loaded_on_plane","belt_number":null,"terminal":"3","message_timestamp":"2026-09-01T08:45:12.302119Z"},
  {"bag_id":"SV4522","flight_number":"EK1","passenger_id":"P8922","event_type":"loaded_on_plane","belt_number":null,"terminal":"3","message_timestamp":"2026-09-01T08:45:14.887601Z"},
  {"bag_id":"SV4521","flight_number":"EK1","passenger_id":"P8921","event_type":"unloaded","belt_number":null,"terminal":"3","message_timestamp":"2026-09-01T10:16:02.554873Z"},
  {"bag_id":"SV4521","flight_number":"EK1","passenger_id":"P8921","event_type":"on_belt","belt_number":3,"terminal":"3","message_timestamp":"2026-09-01T10:19:47.221904Z"},
  {"bag_id":"QR9981","flight_number":"QR2","passenger_id":"P4410","event_type":"bag_lost","belt_number":null,"terminal":"1","message_timestamp":"2026-09-01T10:21:03.660015Z"}
]
```

</div>

## Topic: `customs_clearance` 🛂

<div dir="ltr" align="left">

```json
[
  {"passenger_id":"P8921","flight_number":"EK1","event_type":"arrived_customs","officer_id":"OFF203","gate":"C12","message_timestamp":"2026-09-01T10:20:11.442038Z"},
  {"passenger_id":"P8921","flight_number":"EK1","event_type":"customs_cleared","officer_id":"OFF203","gate":"C12","message_timestamp":"2026-09-01T10:22:45.890120Z"},
  {"passenger_id":"P8922","flight_number":"EK1","event_type":"flagged_for_inspection","officer_id":"OFF117","gate":"C09","message_timestamp":"2026-09-01T10:23:02.301887Z"},
  {"passenger_id":"P4410","flight_number":"QR2","event_type":"arrived_customs","officer_id":"OFF088","gate":"C15","message_timestamp":"2026-09-01T10:24:19.550772Z"}
]
```

</div>

## ملاحظة مهمة على الشكل ده

زي ما اتفقنا قبل كده، الـ Topic نفسه مش مكتوب جوه الـ JSON. يعني الرسايل اللي فوق دي هي بس محتوى الـ **Value**.

لو حبينا نكتب الكود الحقيقي اللي بيبعت الرسايل الأربعة بتوع `flight_status` مثلاً، هيكون شكله:

<div dir="ltr" align="left">

```python
flight_events = [
    {"flight_number":"EK1", "status":"landed", ...},
    {"flight_number":"QR2", "status":"delayed", ...},
    {"flight_number":"EY3", "status":"boarding", ...},
    {"flight_number":"EK1", "status":"taxiing", ...}
]

for event in flight_events:
    producer.send(
        topic='flight_status',        # ← نفس الـ Topic لكل الرسايل الأربعة
        key=event['flight_number'].encode('utf-8'),
        value=event
    )

producer.flush()
```

</div>

**شرح مهم:**

<div dir="ltr" align="left">

```python
key=event['flight_number'].encode('utf-8')
```

</div>

هنا استخدمنا رقم الرحلة كـ Key. لاحظ إن `EK1` ظهر مرتين في الرسايل (landed, taxiing) — الاتنين هيروحوا لنفس الـ Partition بالظبط، عشان ليهم نفس الـ Key.

## سؤال صغير قبل ما ندخل رسمياً في Partition

بص على بيانات `flight_status` اللي فوق — عندنا 4 رسايل، لكن بس 3 قيم مختلفة لـ `flight_number` (`EK1` ظهر مرتين). لو الـ Topic ده كان عنده 3 Partitions، إيه توقعك: هل الرسالتين بتوع `EK1` هيروحوا لنفس الـ Partition ولا ممكن يتفرقوا؟

---

# الجزء التالت: تقسيم الرسالة الكبيرة على أكتر من Topic

سؤال ممتاز جداً، وده فعلاً نمط تصميم حقيقي وشائع في أنظمة الإنتاج (Production Systems)! خليني أشرحلك الفكرة بالتفصيل.

## الإجابة: أيوه، ده ممكن ومنتشر جداً

الفكرة اسمها **Message Splitting** أو **Event Fan-out**، وبتحصل لما يكون عندك حدث واحد كبير فيه معلومات متعلقة بأكتر من نطاق (Domain)، فالـ Producer بيقسمه ويوزعه على Topics مختلفة.

## مثال حقيقي بمثال المطار

تخيل نظام مركزي في المطار بيستقبل "حدث وصول رحلة كامل" فيه كل التفاصيل مخلوطة سوا:

### 1. الرسالة الكبيرة الأصلية (Raw Event)

<div dir="ltr" align="left">

```json
{
  "event_id": "EVT-88213",
  "event_type": "flight_arrival_complete",
  "flight": {
    "flight_number": "EK1",
    "status": "landed",
    "gate": "A12",
    "actual_time": "2026-09-01T10:15:00Z"
  },
  "baggage": [
    {"bag_id": "SV4521", "passenger_id": "P8921", "event_type": "unloaded"},
    {"bag_id": "SV4522", "passenger_id": "P8922", "event_type": "unloaded"}
  ],
  "customs": {
    "passenger_id": "P8921",
    "event_type": "arrived_customs",
    "gate": "C12"
  }
}
```

</div>

### 2. الرسم التوضيحي للفكرة

<div dir="ltr" align="left">

```text
                    Raw Event (حدث كبير واحد)
                    "flight_arrival_complete"
                              │
                              ▼
                    ┌──────────────────┐
                    │   Producer Logic   │   ← هنا بتحصل عملية التقسيم
                    │   (Splitting)       │
                    └──────────────────┘
                     │        │        │
         ┌───────────┘        │        └───────────┐
         ▼                    ▼                    ▼
┌─────────────────┐ ┌──────────────────┐ ┌──────────────────────┐
│ Topic:            │ │ Topic:             │ │ Topic:                 │
│ flight_status ✈️  │ │ baggage_events 🧳  │ │ customs_clearance 🛂  │
└─────────────────┘ └──────────────────┘ └──────────────────────┘
```

</div>

### 3. الكود الحقيقي (Python)

<div dir="ltr" align="left">

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def process_and_split(raw_event):
    # 1. استخرج جزء الرحلة وابعته لـ Topic الرحلات
    flight_data = raw_event['flight']
    producer.send(
        topic='flight_status',
        key=flight_data['flight_number'].encode('utf-8'),
        value=flight_data
    )

    # 2. استخرج كل عناصر الأمتعة (ممكن تكون أكتر من شنطة) وابعتهم واحدة واحدة
    for bag in raw_event['baggage']:
        producer.send(
            topic='baggage_events',
            key=bag['bag_id'].encode('utf-8'),
            value=bag
        )

    # 3. استخرج جزء الجمارك وابعته لـ Topic الجمارك
    customs_data = raw_event['customs']
    producer.send(
        topic='customs_clearance',
        key=customs_data['passenger_id'].encode('utf-8'),
        value=customs_data
    )

    producer.flush()
```

</div>

**شرح الكود سطر بسطر:**

<div dir="ltr" align="left">

```python
def process_and_split(raw_event):
```

</div>

دالة (function) بتاخد الحدث الكبير الواحد وتبدأ تفككه.

<div dir="ltr" align="left">

```python
flight_data = raw_event['flight']
producer.send(topic='flight_status', ...)
```

</div>

بناخد بس الجزء الخاص بالرحلة من الرسالة الكبيرة، ونبعته لوحده لـ `flight_status`.

<div dir="ltr" align="left">

```python
for bag in raw_event['baggage']:
    producer.send(topic='baggage_events', ...)
```

</div>

**نقطة مهمة:** جزء الأمتعة كان array فيه أكتر من شنطة، فبنعمل `for` loop ونبعت رسالة منفصلة لكل شنطة. يعني لو فيه 2 شنطة، هيتبعتوا رسالتين منفصلتين لـ `baggage_events`، مش رسالة واحدة فيها الاتنين.

<div dir="ltr" align="left">

```python
customs_data = raw_event['customs']
producer.send(topic='customs_clearance', ...)
```

</div>

نفس الفكرة بالظبط لجزء الجمارك.

### 4. ليه الشركات بتعمل كده أصلاً؟

**✅ الفايدة:**

- كل Consumer (نظام) بيفضل يقرأ من Topic واحد بسيط ومخصص ليه
- الأنظمة بتفضل منفصلة عن بعض (Decoupling)
- تقدر تتحكم في صلاحيات كل Topic لوحده (مين يقدر يقرأ إيه)

**⚠️ التحدي:**

- الـ Producer بيحتاج "منطق" (Business Logic) عشان يعرف يفكك الرسالة صح
- لازم تتأكد إن كل الرسايل الفرعية اتبعتت بنجاح (لو واحدة فشلت والباقي نجح، ممكن تحصل مشكلة عدم اتساق - Data Inconsistency)

### 5. الخطأ الشائع

المبتدئين أحياناً بيفتكروا إن العملية دي بتحصل تلقائياً جوه Kafka نفسه — يعني يبعتوا رسالة واحدة لـ Topic واحد ويتوقعوا Kafka "يقسمها" لهم على Topics تانية.

**الحقيقة:** Kafka نفسه مالوش أي منطق تقسيم تلقائي. عملية التقسيم دي مسؤولية الـ Producer بالكامل — يعني الكود بتاعك إنت هو اللي بيقرر يعمل `send()` كام مرة ولأنهي Topics.

> (فيه أداة اسمها **Kafka Streams** بتقدر تعمل تقسيم وإعادة توجيه تلقائي بعد ما البيانات تكون جوه Kafka بالفعل، لكن ده موضوع متقدم هنوصله بعدين لما نخلص الأساسيات).

### تمرين صغير

لو الرسالة الكبيرة كانت شايلة كمان جزء اسمه `security_check` (نتيجة فحص أمني للراكب)، واحنا معندناش Topic مخصص له لسه:

هل هتنصح تعمل Topic رابع جديد `security_events`، ولا تحط البيانات دي جوه `customs_clearance` مع إضافة حقل `event_category` يوضح النوع؟ ايه رأيك وليه؟

---

# الجزء الرابع: Partition — جزء مستقل من الـ Topic

تمام! دلوقتي هنكمل الرحلة بعد ما الـ Producer قسّم الرسالة وبعتها للـ Broker — يعني هنفهم إيه اللي بيحصل جوه Kafka بالظبط لما الرسالة توصل. وده هيوصلنا لأهم مفهوم في Kafka كله: **Partition**.

## 1. الاسم بالإنجليزية

**Partition** — جزء مستقل من الـ Topic

## 2. شرح بسيط جداً بالعربية

قلنا قبل كده إن الـ Topic هو "اسم القناة" بس — مش هو اللي بيخزن البيانات فعلياً.

البيانات الحقيقية بتتخزن جوه أجزاء أصغر اسمها **Partitions**. كل Topic بينقسم لعدد معين من الـ Partitions (إنت بتحدد العدد ده وقت إنشاء الـ Topic، فاكر لما كتبنا <code dir="ltr">--partitions 3</code>؟).

**ليه أصلاً محتاجين نقسم؟ عشان نقدر:**

- نوزع البيانات على أكتر من سيرفر (Broker) في نفس الوقت
- نخلي أكتر من Consumer يشتغلوا بالتوازي (Parallel) على نفس الـ Topic

## 3. مثال المطار — Airport

فاكر لما اتكلمنا عن `Topic: flight_status`؟ خلينا نتخيل إنه اتقسم لـ 3 Partitions.

تخيلهم زي 3 شاشات عرض منفصلة في صالة المطار، بدل ما تكون شاشة واحدة بس بتعرض كل الرحلات:

<div dir="ltr" align="left">

```text
قبل التقسيم (شاشة واحدة كبيرة):
┌─────────────────────────────────────┐
│  شاشة العرض الوحيدة                    │
│  EK1, QR2, EY3, EK1, EK1, QR2...     │  ← كل الرحلات مخلوطة سوا
└─────────────────────────────────────┘

بعد التقسيم (3 شاشات، كل واحدة مسؤولة عن مجموعة رحلات):
┌────────────────┐ ┌────────────────┐ ┌────────────────┐
│  شاشة رقم 0       │ │  شاشة رقم 1       │ │  شاشة رقم 2       │
│  EK1 events       │ │  QR2 events       │ │  EY3 events       │
└────────────────┘ └────────────────┘ └────────────────┘
```

</div>

## 4. الرسم التوضيحي (تحديث كامل لمثال المطار)

<div dir="ltr" align="left">

```text
Producer                              Topic: flight_status ✈️
(نظام حالة الرحلات)                    (مقسوم لـ 3 Partitions)

                              ┌─────────────────────────────┐
                              │  Partition 0                  │
send(key="EK1") ─────────────►│  ┌────┬────┬────┬────┐       │
send(key="EK1") ─────────────►│  │EK1 │EK1 │EK1 │... │       │
                              │  └────┴────┴────┴────┘       │
                              │  Offset: 0    1    2          │
                              ├─────────────────────────────┤
                              │  Partition 1                  │
send(key="QR2") ─────────────►│  ┌────┬────┐                 │
                              │  │QR2 │QR2 │                 │
                              │  └────┴────┘                 │
                              │  Offset: 0    1                │
                              ├─────────────────────────────┤
                              │  Partition 2                  │
send(key="EY3") ─────────────►│  ┌────┐                       │
                              │  │EY3 │                       │
                              │  └────┘                       │
                              │  Offset: 0                     │
                              └─────────────────────────────┘
```

</div>

> **ملاحظة أهم حاجة في الرسم ده:** كل رسايل فيها نفس الـ Key (زي `EK1`) بتروح دايماً لنفس الـ Partition. عشان كده كل رسايل رحلة `EK1` اتجمعت في Partition 0 بس، من غير ما تتفرق.

## 5. مثال Kafka حقيقي — إزاي Kafka بيقرر أنهي Partition؟

Kafka بيستخدم معادلة بسيطة (تقريباً):

<div dir="ltr" align="left">

```text
Partition Number = hash(Key) % Number of Partitions
```

</div>

يعني:

1. بياخد الـ Key (مثلاً `"EK1"`)
2. بيحسبله رقم (Hash) — تخيله زي "بصمة رقمية" للنص
3. بيقسم الرقم ده على عدد الـ Partitions (هنا 3) وياخد الباقي (Remainder)
4. الناتج هو رقم الـ Partition

> **النتيجة المهمة:** طول ما الـ Key ثابت (`"EK1"`) وعدد الـ Partitions ثابت (3)، الرسالة هتروح لنفس الـ Partition دايماً. ده مضمون ومش عشوائي.

## 6. ماذا يحدث داخل Kafka؟ (خطوة بخطوة)

1. الـ Producer بيبعت الرسالة مع Key = `"EK1"`
2. Kafka Broker يستقبلها
3. Kafka يحسب: `hash("EK1") % 3` = مثلاً 0
4. Kafka يحط الرسالة في آخر الـ Log بتاع Partition 0
5. Kafka يدي الرسالة Offset جديد (رقم تسلسلي جوه Partition 0 بس)
6. Kafka يرجع تأكيد (ack) للـ Producer

> **نقطة مهمة جداً:** الـ Offset (رقم ترتيب الرسالة) بيبقى مستقل لكل Partition لوحده. يعني ممكن تلاقي Offset رقم 0 في Partition 0، وكمان Offset رقم 0 في Partition 1 — دول رسالتين مختلفتين تماماً، مش نفس الرقم.

## 7. Command أو Python Code

### أ. لو عايز تحدد الـ Key عشان تتحكم في الـ Partition

<div dir="ltr" align="left">

```python
producer.send(
    topic='flight_status',
    key=b'EK1',
    value=flight_event
)
```

</div>

**شرح النقطة الجديدة هنا:**

<div dir="ltr" align="left">

```python
key=b'EK1'
```

</div>

ده اللي بيتحكم في أي Partition الرسالة هتروحله. لو ماحددتش Key (يعني `key=None`)، Kafka هيوزع الرسايل بالتساوي بين الـ Partitions بشكل عشوائي تقريباً (Round-robin) — مفيش ضمان إن رسايل `EK1` كلها تتجمع مع بعض.

### ب. أمر عشان تشوف تفاصيل الـ Partitions بتاعة Topic معين

<div dir="ltr" align="left">

```bash
kafka-topics.sh --describe \
  --topic flight_status \
  --bootstrap-server localhost:9092
```

</div>

الناتج هيكون شكله كده:

<div dir="ltr" align="left">

```text
Topic: flight_status   PartitionCount: 3
    Partition: 0   Leader: 1   Replicas: 1
    Partition: 1   Leader: 2   Replicas: 2
    Partition: 2   Leader: 3   Replicas: 3
```

</div>

(متقلقش من `Leader` و`Replicas` دلوقتي — دول هيتشرحوا في مفهوم Replication بعدين).

## 8. الخطأ الشائع

غلطة كبيرة بيقع فيها المبتدئين: بيفتكروا إن Kafka بيحافظ على ترتيب الرسايل عبر الـ Topic كله.

**الحقيقة:** Kafka بيضمن الترتيب جوه نفس الـ Partition بس، مش عبر الـ Topic كله.

- ✅ **مضمون:** كل رسايل `EK1` (في Partition 0) هتفضل بترتيبها الصحيح
- ❌ **مش مضمون:** إن رسالة `QR2` (Partition 1) تيجي قبل أو بعد رسالة `EK1` (Partition 0) بترتيب زمني دقيق عبر الـ Topic كله

عشان كده اختيار الـ Key قرار مهم جداً — لو محتاج كل أحداث رحلة معينة تتحافظ على ترتيبها، لازم تستخدم رقم الرحلة كـ Key، بالظبط زي ما عملنا.

## 9. سؤال أو تمرين صغير

فاكر الرسايل بتاعة `baggage_events` اللي عملناها قبل كده؟ فيها شنطتين بنفس رقم الرحلة `EK1` (`SV4521` و`SV4522`) لكن بـ `bag_id` مختلف.

**سؤالي ليك:**

لو الـ Producer استخدم `bag_id` كـ Key (مش `flight_number`)، هل الشنطتين دول (`SV4521` و`SV4522`) مضمون إنهم يروحوا لنفس الـ Partition؟ ولا ممكن يتفرقوا؟ وليه ده ممكن يبقى مشكلة لو نظام تتبع الأمتعة محتاج يشوف كل شنط رحلة معينة بترتيبها مع بعض؟

</div>
