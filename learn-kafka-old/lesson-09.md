<div dir="rtl" align="right">

# Kafka Connect و Kafka Streams — والخلاصة الشاملة

**محتويات الملف:**

1. الجزء الأول: Kafka Connect — نقل البيانات من وإلى أنظمة خارجية
2. الجزء التاني: Kafka Streams — معالجة البيانات لحظياً
3. الخلاصة الشاملة — رحلة Kafka كاملة

---

# الجزء الأول: Kafka Connect

## 1. الاسم بالإنجليزية

**المصطلح:** Kafka Connect

**الترجمة:** أداة جاهزة لتوصيل Kafka بأنظمة خارجية من غير ما تكتب كود Producer أو Consumer يدوي

## 2. شرح بسيط جداً بالعربية

**السؤال الأساسي:**

فاكر كل الكود اللي كتبناه لحد دلوقتي؟ كل مرة كنا محتاجين نوصل بيانات من أو إلى نظام خارجي (زي قاعدة بيانات)، كنا هنضطر نكتب Producer أو Consumer بالكامل بنفسنا. السؤال: هل فيه طريقة أسهل من كتابة الكود ده كل مرة؟

**الإجابة:**

أيوه، Kafka Connect هو أداة جاهزة بتعمل الشغل ده لوحدها، إنت بس بتحدد الإعدادات (Configuration) من غير ما تكتب كود.

**نقطة جوهرية:**

Kafka Connect بيشتغل بمفهومين بس:

<div dir="ltr" align="left">

```text
Source Connector  → بيسحب بيانات من نظام خارجي ويحطها في Kafka
Sink Connector    → بياخد بيانات من Kafka ويحطها في نظام خارجي
```

</div>

## 3. مثال المطار — Airport

**السياق:**

تخيل قاعدة بيانات قديمة (Database) بتسجل فيها شركة الطيران كل تحديثات الرحلات، من غير ما تعرف حاجة عن Kafka خالص. عايزين البيانات دي توصل لـ `Topic: flight_status` أوتوماتيك، من غير ما نكتب كود Producer يدوي بيتصل بقاعدة البيانات دي.

<div dir="ltr" align="left">

```text
قاعدة بيانات شركة الطيران (Database)
          │
          │  Source Connector (بيسحب البيانات أوتوماتيك)
          ▼
   Topic: flight_status ✈️
          │
          │  Sink Connector (بيصدر البيانات لمكان تاني)
          ▼
   Elasticsearch (لأرشفة والبحث في السجلات القديمة)
```

</div>

## 4. الرسم التوضيحي الكامل

<div dir="ltr" align="left">

```text
┌──────────────────┐
│  Airline Database   │
│  (قاعدة بيانات        │
│   شركة الطيران)       │
└─────────┬────────┘
          │
          ▼
┌──────────────────┐
│  Source Connector   │  ← بيراقب الجدول ويسحب أي صف جديد
│  (JDBC Connector)    │
└─────────┬────────┘
          ▼
┌──────────────────┐
│  Topic:              │
│  flight_status ✈️     │
└─────────┬────────┘
          ▼
┌──────────────────┐
│  Sink Connector       │  ← بياخد كل رسالة جديدة ويحطها هناك
│  (Elasticsearch Sink) │
└─────────┬────────┘
          ▼
┌──────────────────┐
│  Elasticsearch        │
│  (للبحث والأرشفة)      │
└──────────────────┘
```

</div>

## 5. ماذا يحدث داخل Kafka Connect؟

**خطوة بخطوة:**

1. تشغل Kafka Connect كـ عملية منفصلة (Worker)
2. تحدد له إعدادات الـ Connector (اسم الجدول، عنوان قاعدة البيانات، اسم الـ Topic)
3. الـ Source Connector يبدأ يراقب الجدول باستمرار
4. أي صف جديد يتضاف في الجدول، الـ Connector يسحبه ويحوله لرسالة Kafka
5. الرسالة تتبعت لـ Topic المحدد، بنفس آلية أي Producer عادي

> **نقطة مهمة:** Kafka Connect مش نظام مختلف عن Producer/Consumer من ناحية Kafka نفسه — هو فعلياً بيستخدم نفس مبادئ الـ Producer والـ Consumer اللي اتعلمناها، بس بشكل جاهز ومُعد مسبقاً (Pre-built)، من غير ما تكتب الكود بنفسك.

## 6. مثال Configuration حقيقي

إعداد Source Connector بيسحب من قاعدة بيانات:

<div dir="ltr" align="left">

```json
{
  "name": "flight-db-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://airline-db:5432/flights",
    "table.whitelist": "flight_updates",
    "topic.prefix": "flight_status",
    "mode": "incrementing",
    "incrementing.column.name": "id"
  }
}
```

</div>

**شرح الإعدادات المهمة:**

- <code dir="ltr">"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector"</code> — بيحدد نوع الـ Connector المستخدم، هنا JDBC، يعني بيتصل بقواعد بيانات SQL عادية.
- <code dir="ltr">"table.whitelist": "flight_updates"</code> — اسم الجدول اللي عايزين نراقبه في قاعدة البيانات.
- <code dir="ltr">"mode": "incrementing"</code> — بيقول للـ Connector: "راقب عمود مُعرِّف تلقائي الزيادة (زي ID)، وأي صف جديد ليه رقم أكبر من آخر رقم شفته، اسحبه".
- <code dir="ltr">"topic.prefix": "flight_status"</code> — اسم الـ Topic اللي هتتحط فيه البيانات المسحوبة.

## 7. الخطأ الشائع

**الخطأ:**

الاعتقاد إن Kafka Connect بيعمل معالجة أو تحويل معقد للبيانات (زي فلترة، تجميع، حسابات).

**التصحيح:**

Kafka Connect وظيفته نقل البيانات بس (Data Movement)، مش معالجتها. لو محتاج تعالج البيانات أثناء انتقالها (زي فلترة رسالة معينة أو حساب متوسط)، محتاج أداة تانية اسمها Kafka Streams، وده بالظبط موضوعنا الجاي.

## تمرين صغير

**سؤال:**

لو عايز تاخد بيانات `Topic: baggage_events` وتحطها في نظام تخزين طويل المدى زي Amazon S3 عشان الأرشفة، هل ده هيحتاج Source Connector ولا Sink Connector؟ وليه؟

دلوقتي هنكمل بآخر مفهوم أساسي في القائمة.

---

# الجزء التاني: Kafka Streams

## 1. الاسم بالإنجليزية

**المصطلح:** Kafka Streams

**الترجمة:** مكتبة لمعالجة البيانات وهي جايه من Kafka، بشكل مستمر ولحظي

## 2. شرح بسيط جداً بالعربية

**السؤال الأساسي:**

فاكرين إن Kafka Connect بينقل البيانات بس من غير معالجة؟ السؤال دلوقتي: طيب لو عايز أعالج البيانات وهي بتتحرك — زي إني أحسب عدد الرحلات المتأخرة كل ساعة، أو أفلتر رسايل معينة، أو أدمج بيانات من Topic مع Topic تاني؟

**الإجابة:**

Kafka Streams هي المكتبة اللي بتخليك تكتب كود بيقرأ من Topic، يعالج البيانات فوراً وهي جايه، ويكتب النتيجة في Topic تاني — كله بشكل مستمر ولحظي (Real-time).

**نقطة جوهرية:**

ده مختلف تماماً عن الـ Consumer العادي اللي اتعلمناه. الـ Consumer العادي بيقرأ ويعالج رسالة رسالة بشكل منفصل. Kafka Streams بيقدر يعمل عمليات أعقد زي: تجميع بيانات (Aggregation)، ربط Topics ببعض (Joins)، حساب على فترات زمنية (Windowing).

## 3. مثال المطار — Airport

**السياق:**

تخيل عايز تحسب عدد الرحلات المتأخرة كل 10 دقايق، من غير ما تنتظر لحد آخر اليوم تعمل تقرير يدوي. عايز الرقم ده يتحدث لحظياً كل ما رحلة جديدة تتأخر.

<div dir="ltr" align="left">

```text
Topic: flight_status ✈️
          │
          ▼
┌──────────────────┐
│  Kafka Streams        │
│  Application          │
│                        │
│  - يفلتر: status=delayed │
│  - يجمع كل 10 دقايق      │
│  - يحسب العدد           │
└─────────┬────────┘
          ▼
Topic: delayed_flights_count_per_10min
```

</div>

## 4. الرسم التوضيحي — عملية Filter بسيطة

<div dir="ltr" align="left">

```text
Input Topic: flight_status

┌────┬────┬────┬────┬────┐
│EK1  │QR2  │EY3  │EK1  │QR2  │
│landed│delayed│boarding│taxiing│delayed│
└────┴────┴────┴────┴────┘
       │              │
       ▼              ▼
   ┌──────────────────────┐
   │   Kafka Streams          │
   │   Filter: status=delayed  │
   └──────────────────────┘
       │              │
       ▼              ▼
Output Topic: delayed_flights

┌────┐        ┌────┐
│QR2  │        │QR2  │
│delayed│      │delayed│
└────┘        └────┘
```

</div>

## 5. مفهوم مهم: Stream مقابل Table

### المفهوم الأول: KStream

بيمثل تدفق مستمر من الأحداث المستقلة عن بعض — كل حدث حاجة منفصلة (زي كل تحديث حالة رحلة).

### المفهوم التاني: KTable

بيمثل آخر حالة معروفة لكل Key — شبيه بمفهوم Log Compaction اللي اتكلمنا عنه قبل كده.

**مثال يوضح الفرق:**

<div dir="ltr" align="left">

```text
KStream (كل حدث منفصل):
   EK1 → landed
   EK1 → taxiing
   EK1 → at_gate
   (3 أحداث منفصلة، كلهم موجودين)

KTable (آخر حالة بس):
   EK1 → at_gate
   (الحالة الحالية بس، القديم اتستبدل)
```

</div>

## 6. مثال Python Code — استخدام مكتبة Faust (مكافئ Kafka Streams بلغة Python)

> **ملحوظة مهمة:** Kafka Streams أصلها مكتبة Java/Scala رسمية من Kafka نفسه. في Python، بنستخدم مكتبة مشابهة في الفكرة اسمها **Faust**.

<div dir="ltr" align="left">

```python
import faust

app = faust.App('flight-delay-counter', broker='kafka://localhost:9092')

flight_topic = app.topic('flight_status', value_type=dict)
delayed_count_topic = app.topic('delayed_flights_count')

@app.agent(flight_topic)
async def process_flights(flights):
    async for flight in flights:
        if flight['status'] == 'delayed':
            await delayed_count_topic.send(value={
                'flight_number': flight['flight_number'],
                'delayed_at': flight['message_timestamp']
            })
```

</div>

**شرح الكود سطر بسطر:**

<div dir="ltr" align="left">

```python
app = faust.App('flight-delay-counter', broker='kafka://localhost:9092')
```

</div>

بننشئ تطبيق Streams، ونحدد اسمه وعنوان الـ Kafka Cluster.

<div dir="ltr" align="left">

```python
flight_topic = app.topic('flight_status', value_type=dict)
```

</div>

بنحدد الـ Topic المصدر اللي هنعالج بياناته.

<div dir="ltr" align="left">

```python
@app.agent(flight_topic)
async def process_flights(flights):
```

</div>

ده تعريف "معالج" (Agent) هيشتغل تلقائياً على كل رسالة جديدة توصل لـ `flight_topic`.

<div dir="ltr" align="left">

```python
if flight['status'] == 'delayed':
    await delayed_count_topic.send(value={...})
```

</div>

المنطق الفعلي: لو حالة الرحلة "متأخرة"، ابعت رسالة جديدة لـ Topic تاني اسمه `delayed_flights_count`.

## 7. الخطأ الشائع

**الخطأ الأول:**

الاعتقاد إن Kafka Streams نظام منفصل عن Kafka، محتاج سيرفرات خاصة بيه زي Kafka Connect.

**التصحيح:**

Kafka Streams هي مكتبة بس (Library)، مدمجة جوه تطبيقك أنت. مفيش Cluster منفصل ليها — هي بتشتغل كجزء من تطبيقك العادي، وبتقرأ وتكتب من/لـ Kafka زي أي Producer/Consumer عادي.

**الخطأ التاني:**

الخلط بين Kafka Connect و Kafka Streams.

**الفرق الجوهري:**

| الأداة | وظيفتها |
|---|---|
| **Kafka Connect** | نقل بيانات بس، من غير معالجة (Data Movement) |
| **Kafka Streams** | معالجة وتحويل البيانات فعلياً (Data Processing) |

## 8. تمرين صغير

**سؤال:**

لو عايز تعمل تطبيق يحسب متوسط سرعة كل سفينة (فاكر بيانات السفن اللي شفناها قبل كده) على مدار كل 5 دقايق، وتبعت النتيجة لـ Topic جديد اسمه `vessel_avg_speed`:

هل ده استخدام مناسب لـ Kafka Connect ولا Kafka Streams؟ وليه؟

---

# الخلاصة الشاملة — رحلة Kafka كاملة

بكده خلصنا كل المفاهيم الأساسية والمتقدمة المهمة:

<div dir="ltr" align="left">

```text
✅ Motivation
✅ Topic → Partition → Offset
✅ Producer → Consumer → Consumer Group
✅ Broker → Cluster → Replication
✅ ZooKeeper / KRaft
✅ acks → Retention Policy
✅ Kafka Connect → Kafka Streams
```

</div>

**المتبقي** (مستوى متقدم جداً، تتعلمه حسب الحاجة الفعلية في الشغل):

<div dir="ltr" align="left">

```text
- Schema Registry + Avro/Protobuf
- Security (Authentication, Authorization, Encryption)
- Monitoring (JMX Metrics, Lag Monitoring)
- Multi-Cluster Replication (MirrorMaker)
```

</div>

مبروك — كملت رحلة كاملة من الصفر لحد فهم عميق لكل مكونات Kafka الأساسية والمتقدمة، كل ده بمثال المطار اللي فضل معانا من أول يوم! 🎉

</div>
