<div dir="rtl">

# الدرس 10: Kafka Streams — KStream vs. KTable

## الخطوة 0: كل اللي فاتنا كان قراءة وكتابة... طيب المعالجة نفسها فين؟

لحد دلوقتي كنا بنتكلم عن نقل البيانات: `Producer` بيكتب، `Consumer` بيقرأ، `Kafka Connect` بينقل من/لأنظمة خارجية. لكن ماذا لو عايز *تعالج* البيانات وهي مارّة (`process on the fly`) — زي حساب عدد الركاب اللي دخلوا بوابة معينة في آخر 5 دقايق، أو تجميع بيانات كل رحلة لحظيًا؟ ده نوع مختلف من المهمة، اسمه `Stream Processing`.

## الخطوة 1: الحل — Kafka Streams

`Kafka Streams` هو الإطار المخصص لبناء منطق معالجة مستمر شغال فوق Kafka مباشرة، من غير ما تحتاج نظام معالجة منفصل زي Spark. في مشروع المطار استخدمنا `Faust` (مكتبة Python) كتطبيق لمفاهيم الـ `Kafka Streams`.

## الخطوة 2: أبسط تمثيل للبيانات — KStream

أول طريقة نمثل بيها البيانات المتدفقة: كل رسالة هي حدث مستقل بيمثل شيء حصل في لحظة معينة، وكل الأحداث بتتحسب — مافيش "حدث جديد بيمسح حدث قديم". التمثيل ده اسمه `KStream`.

```
KStream: flight_boarding_events

10:00 → {passenger: "Ahmed", flight: "101", action: "boarded"}
10:01 → {passenger: "Sara",  flight: "101", action: "boarded"}
10:02 → {passenger: "Ahmed", flight: "101", action: "boarded"}  ← حدث منفصل، مش تحديث!
```

فكر فيه زي سجل دخول (`log`) — كل سطر مهم لوحده، وكل الأسطر بتتراكم مع بعض من غير ما توحل محل بعضها.

## الخطوة 3: مشكلة — أحيانًا مش عايزين كل التاريخ، عايزين آخر حالة بس

تخيل عايز تعرف "إيه حالة رحلة 101 دلوقتي؟" مش "إيه كل الأحداث اللي حصلت للرحلة 101؟". لو استخدمت `KStream`، هتضطر تمر على كل الأحداث من الأول عشان توصل لآخر حالة — ده غير عملي.

## الخطوة 4: الحل — KTable

`KTable` بيمثل *آخر حالة* معروفة لكل `key`، زي جدول (`table`) بيتحدث باستمرار. لو جالك حدث جديد بنفس الـ `key`، هو بيستبدل (`update`) القيمة القديمة، مش يضيفها — عكس الـ `KStream` تمامًا.

```
KTable: current_flight_status  (key = flight_id)

10:00 → flight_101: "boarding"
10:15 → flight_101: "departed"   ← استبدلت "boarding"
10:15 → flight_205: "delayed"

الحالة النهائية في الـ KTable:
flight_101 → "departed"
flight_205 → "delayed"
```

**ملاحظة**: ده اللي بيربط `KTable` مباشرة بمفهوم `Log Compaction` اللي اتعلمناه في الدرس اللي فات — الـ `KTable` داخليًا محتاج نفس فكرة "الاحتفاظ بآخر قيمة لكل key بس"، وده بالظبط سبب إن الـ changelog topics بتاعتها لازم تستخدم `cleanup.policy=compact`.

## الخطوة 5: الفرق الجوهري في جدول واحد

| | `KStream` | `KTable` |
|---|---|---|
| يمثل | تدفق أحداث (events) | حالة حالية (state) |
| رسالة جديدة بنفس الـ key | تُضاف كحدث منفصل | تستبدل القيمة القديمة |
| مثال | "راكب صعد الطائرة" (حدث تاريخي) | "حالة الرحلة الآن" (حالة حالية) |
| علاقته بالـ Retention (الدرس اللي فات) | عادة `Time-based` | `Log Compaction` |

## الخطوة 6: استخدامهم مع بعض — التجميع (Aggregation)

في تطبيقات حقيقية، غالبًا بتستخدم الاتنين مع بعض: تاخد `KStream` من الأحداث الخام (`boarding events`)، وتعمل عليه تجميع (`aggregation`) عشان تطلع منه `KTable` بيمثل "عدد الركاب الحاليين على متن كل رحلة لحظيًا".

```
KStream (boarding events) ──aggregate by flight_id──▶ KTable (current passenger count per flight)
```

## مثال بكود Python (`Faust`)

تعريف `KStream` بسيط بيستقبل أحداث الصعود ويطبعها (خطوة 2):

```python
import faust

app = faust.App("boarding-app", broker="kafka://localhost:9092")

boarding_topic = app.topic("flights.boarding")  # هنا الـ value_type=dict اتشالت (setup-gotcha معروفة)

@app.agent(boarding_topic)
async def process_boarding_events(events):
    async for event in events:
        print(f"Event: {event}")   # كل event بيتعامل معاه لوحده، مافيش استبدال
```

تعريف `KTable` بيحتفظ بآخر حالة معروفة لكل رحلة (خطوة 4):

```python
flight_status_table = app.Table(
    "current-flight-status",     # هيتولد changelog topic باسم قريب من ده
    default=str,
    partitions=3,                 # لازم يطابق عدد partitions الـ topic المصدر (setup-gotcha معروفة)
)

@app.agent(boarding_topic)
async def update_flight_status(events):
    async for event in events:
        flight_status_table[event["flight"]] = event["action"]  # استبدال، مش إضافة
```

مثال تجميع (خطوة 6) — حساب عدد الركاب الحاليين لكل رحلة من تدفق أحداث الصعود:

```python
passenger_count_table = app.Table("passenger-count", default=int, partitions=3)

@app.agent(boarding_topic)
async def count_passengers(events):
    async for event in events:
        if event["action"] == "boarded":
            passenger_count_table[event["flight"]] += 1
```

## تمرين صغير

- لو عايز تحسب "عدد مرات دخول كل بوابة اليوم"، هل ده أقرب لـ `KStream` ولا `KTable`؟
- لو عايز تعرف "آخر بوابة دخل منها كل راكب"، ده أقرب لإيه؟

---
◀ [الدرس السابق: Kafka Connect](./09-kafka-connect.md) | [الفهرس](./00-index.md)

</div>
