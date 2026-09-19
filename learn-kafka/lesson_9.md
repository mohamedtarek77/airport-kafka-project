<div dir="rtl">

# الدرس 9: Kafka Connect

## الخطوة 0: مشكلة — كل تكامل مع نظام خارجي هنكتبه بإيدينا؟

في كل الدروس اللي فاتت، اتكلمنا عن `Producers` و`Consumers` بيتكتبوا بلغة برمجة (Python في حالتنا) عشان يبعتوا/يقروا رسائل. لكن تخيل عايز تنقل بيانات من قاعدة بيانات (`Database`) لـ Kafka، أو من Kafka لنظام تخزين سحابي زي `S3`. هل لازم تكتب `Producer` أو `Consumer` مخصوص بإيدك لكل حالة من دول، وتتعامل بنفسك مع كل تفاصيل إعادة المحاولة والأخطاء؟

## الخطوة 1: الحل — Kafka Connect

الإجابة: لأ، مش لازم. Kafka بيوفر إطار عمل جاهز اسمه `Kafka Connect`، مصمم خصيصًا عشان ينقل البيانات بين Kafka والأنظمة الخارجية، من غير ما تكتب كود تكامل من الصفر. فكر فيه زي "شبكة النقالات والسيور" في المطار — نظام جاهز بينقل الحقائب من الطائرة للسير الناقل وبالعكس، من غير ما موظف يحمل كل حقيبة بإيده.

## الخطوة 2: اتجاه أول — من نظام خارجي لـ Kafka (Source Connector)

لو عايز تسحب بيانات من نظام خارجي (زي قاعدة بيانات) وتحطها في Kafka، بتستخدم `Source Connector`.

```
[Database]  ──Source Connector──▶  [Kafka Topic]

مثال: كل صف جديد يتضاف في جدول "flights" في قاعدة البيانات
      يترجم تلقائيًا لرسالة في Topic اسمه flights.raw
```

## الخطوة 3: اتجاه تاني — من Kafka لنظام خارجي (Sink Connector)

بالعكس، لو عايز تاخد بيانات من `Topic` معين وتحطها في نظام خارجي، بتستخدم `Sink Connector`.

```
[Kafka Topic]  ──Sink Connector──▶  [Elasticsearch / S3 / DB]

مثال: كل رسالة في Topic اسمه flights.processed
      تتكتب تلقائيًا كصف في جدول تحليلات في BigQuery
```

## الخطوة 4: طيب ليه مانكتبش الكود ده بنفسنا أصلًا؟

سؤال منطقي بعد ما فهمنا الفكرة: إحنا أصلًا بنعرف نكتب `Producer` و`Consumer` بـ Python — ليه مانستخدمش ده بدل `Kafka Connect`؟

**الإجابة**: لو كتبت الكود بنفسك، هتضطر تتعامل يدويًا مع تفاصيل معقدة زي: إعادة المحاولة عند الفشل (`retries`)، تتبع آخر offset تمت معالجته (`offset tracking`)، معالجة الأخطاء الجزئية، والتوسع الأفقي. `Kafka Connect` بيوفر كل ده جاهز ومُختبر، وبيشتغل بشكل موزع (`distributed mode`) وقابل للتوسع زي أي جزء تاني في Kafka.

## الخطوة 5: مكتبة جاهزة من الـ Connectors

ميزة إضافية: فيه مكتبة كبيرة جدًا من الـ `Connectors` الجاهزة لقواعد بيانات، أنظمة تخزين سحابية، أنظمة بحث، وغيرها — من غير ما تكتب كود تكامل بنفسك خالص، مجرد إعداد (`configuration`) بصيغة JSON.

## الخطوة 6: مثال تطبيقي في سياق مشروع أوسع

في مشاريع تانية شبيهة (زي مشروع تتبع السفن `Maritime AIS`)، دور `Kafka Connect` بيكون طبيعي جدًا: تسحب بيانات خام من مصدر خارجي (API أو قاعدة بيانات) وتضخها في Kafka كـ `Source`، أو تصدر النتائج المعالجة لمخزن تحليلي كـ `Sink` — من غير ما تكتب سكريبت Python منفصل يعمل الشغل ده يدويًا زي اللي بنعمله في باقي المشروع.

## مثال بكود Python — التعامل مع Kafka Connect REST API

`Kafka Connect` نفسه مش بيتكلم بنفس الـ Kafka protocol اللي بيستخدمه `kafka-python-ng` — هو نظام منفصل بيديره عن طريق REST API. يعني من Python، بنستخدم مكتبة `requests` العادية:

```python
import requests
import json

CONNECT_URL = "http://localhost:8083"

# إنشاء Source Connector يسحب بيانات من جدول Postgres لـ Topic في Kafka
source_config = {
    "name": "flights-checkin-source",
    "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "connection.url": "jdbc:postgresql://localhost:5432/airport_db",
        "table.whitelist": "flights_checkin",
        "topic.prefix": "flights.",   # هينتج Topic اسمه flights.flights_checkin
        "mode": "incrementing",
        "incrementing.column.name": "id",
    },
}

response = requests.post(
    f"{CONNECT_URL}/connectors",
    headers={"Content-Type": "application/json"},
    data=json.dumps(source_config),
)
print(response.status_code, response.json())
```

متابعة حالة الـ `Connector` بعد إنشائه:

```python
status = requests.get(f"{CONNECT_URL}/connectors/flights-checkin-source/status")
print(json.dumps(status.json(), indent=2, ensure_ascii=False))
```

## تمرين صغير

- لو عايز تاخد بيانات من ملف CSV بيتحدث كل شوية وتحطها في Kafka Topic، ده هيكون `Source` ولا `Sink` connector؟
- ما الفرق العملي بين إنك تكتب `Producer` بلغة برمجة بنفسك (زي اللي عملناه في درس الـ Producers)، وإنك تستخدم `Source Connector` جاهز لنفس المهمة؟

---
◀ [الدرس السابق: Retention Policies](./08-retention-policies.md) | [الفهرس](./00-index.md) | التالي ▶ [الدرس 10: Kafka Streams](./10-kafka-streams.md)

</div>



*************************************************




# Kafka Connect

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

```text
Source Connector  → بيسحب بيانات من نظام خارجي ويحطها في Kafka
Sink Connector    → بياخد بيانات من Kafka ويحطها في نظام خارجي
```

## 3. مثال المطار — Airport

**السياق:**

تخيل قاعدة بيانات قديمة (Database) بتسجل فيها شركة الطيران كل تحديثات الرحلات، من غير ما تعرف حاجة عن Kafka خالص. عايزين البيانات دي توصل لـ Topic: `flight_status` أوتوماتيك، من غير ما نكتب كود Producer يدوي بيتصل بقاعدة البيانات دي.

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

## 4. الرسم التوضيحي الكامل

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

## 5. ماذا يحدث داخل Kafka Connect؟

**خطوة بخطوة:**

```text
1. تشغل Kafka Connect كـ عملية منفصلة (Worker)
2. تحدد له إعدادات الـ Connector (اسم الجدول، عنوان قاعدة البيانات، اسم الـ Topic)
3. الـ Source Connector يبدأ يراقب الجدول باستمرار
4. أي صف جديد يتضاف في الجدول، الـ Connector يسحبه ويحوله لرسالة Kafka
5. الرسالة تتبعت لـ Topic المحدد، بنفس آلية أي Producer عادي
```

**نقطة مهمة:**

Kafka Connect مش نظام مختلف عن Producer/Consumer من ناحية Kafka نفسه — هو فعلياً بيستخدم نفس مبادئ الـ Producer والـ Consumer اللي اتعلمناها، بس بشكل جاهز ومُعد مسبقاً (Pre-built)، من غير ما تكتب الكود بنفسك.

## 6. مثال Configuration حقيقي

إعداد Source Connector بيسحب من قاعدة بيانات:

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

**شرح الإعدادات المهمة:**

**الإعداد:**

```text
"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector"
```

بيحدد نوع الـ Connector المستخدم — هنا JDBC، يعني بيتصل بقواعد بيانات SQL عادية.

**الإعداد:**

```text
"table.whitelist": "flight_updates"
```

اسم الجدول اللي عايزين نراقبه في قاعدة البيانات.

**الإعداد:**

```text
"mode": "incrementing"
```

بيقول للـ Connector: "راقب عمود مُعرِّف تلقائي الزيادة (زي ID)، وأي صف جديد ليه رقم أكبر من آخر رقم شفته، اسحبه".

**الإعداد:**

```text
"topic.prefix": "flight_status"
```

اسم الـ Topic اللي هتتحط فيه البيانات المسحوبة.

## 7. الخطأ الشائع

**الخطأ:**

الاعتقاد إن Kafka Connect بيعمل معالجة أو تحويل معقد للبيانات (زي فلترة، تجميع، حسابات).

**التصحيح:**

Kafka Connect وظيفته نقل البيانات بس (Data Movement)، مش معالجتها. لو محتاج تعالج البيانات أثناء انتقالها (زي فلترة رسالة معينة أو حساب متوسط)، محتاج أداة تانية اسمها Kafka Streams، وده بالظبط موضوعنا الجاي.

## تمرين صغير

**سؤال:**

لو عايز تاخد بيانات Topic: `baggage_events` وتحطها في نظام تخزين طويل المدى زي Amazon S3 عشان الأرشفة، هل ده هيحتاج Source Connector ولا Sink Connector؟ وليه؟

دلوقتي هنكمل بآخر مفهوم أساسي في القائمة.

---

# Kafka Streams

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

## 4. الرسم التوضيحي — عملية Filter بسيطة

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

## 5. مفهوم مهم: Stream مقابل Table

**المفهوم الأول: KStream**

**الشرح:**

بيمثل تدفق مستمر من الأحداث المستقلة عن بعض — كل حدث حاجة منفصلة (زي كل تحديث حالة رحلة).

**المفهوم الثاني: KTable**

**الشرح:**

بيمثل آخر حالة معروفة لكل Key — شبيه بمفهوم Log Compaction اللي اتكلمنا عنه قبل كده.

**مثال يوضح الفرق:**

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

## 6. مثال Python Code — استخدام مكتبة Faust (مكافئ Kafka Streams بلغة Python)

**ملحوظة مهمة:**

Kafka Streams أصلها مكتبة Java/Scala رسمية من Kafka نفسه. في Python، بنستخدم مكتبة مشابهة في الفكرة اسمها Faust.

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

**شرح الكود سطر بسطر:**

**السطر:**

```text
app = faust.App('flight-delay-counter', broker='kafka://localhost:9092')
```

بننشئ تطبيق Streams، ونحدد اسمه وعنوان الـ Kafka Cluster.

**السطر:**

```text
flight_topic = app.topic('flight_status', value_type=dict)
```

بنحدد الـ Topic المصدر اللي هنعالج بياناته.

**السطر:**

```text
@app.agent(flight_topic)
async def process_flights(flights):
```

ده تعريف "معالج" (Agent) هيشتغل تلقائياً على كل رسالة جديدة توصل لـ `flight_topic`.

**السطر:**

```text
if flight['status'] == 'delayed':
    await delayed_count_topic.send(value={...})
```

المنطق الفعلي: لو حالة الرحلة "متأخرة"، ابعت رسالة جديدة لـ Topic تاني اسمه `delayed_flights_count`.

## 7. الخطأ الشائع

**الخطأ الأول:**

الاعتقاد إن Kafka Streams نظام منفصل عن Kafka، محتاج سيرفرات خاصة بيه زي Kafka Connect.

**التصحيح:**

Kafka Streams هي مكتبة بس (Library)، مدمجة جوه تطبيقك أنت. مفيش Cluster منفصل ليها — هي بتشتغل كجزء من تطبيقك العادي، وبتقرأ وتكتب من/لـ Kafka زي أي Producer/Consumer عادي.

**الخطأ الثاني:**

الخلط بين Kafka Connect و Kafka Streams.

**الفرق الجوهري:**

```text
Kafka Connect → نقل بيانات بس، من غير معالجة (Data Movement)
Kafka Streams → معالجة وتحويل البيانات فعلياً (Data Processing)
```

## 8. تمرين صغير

**سؤال:**

لو عايز تعمل تطبيق يحسب متوسط سرعة كل سفينة (فاكر بيانات السفن اللي شفناها قبل كده) على مدار كل 5 دقايق، وتبعت النتيجة لـ Topic جديد اسمه `vessel_avg_speed`:

هل ده استخدام مناسب لـ Kafka Connect ولا Kafka Streams؟ وليه؟
