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
