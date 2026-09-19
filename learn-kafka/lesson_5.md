<div dir="rtl">

# الدرس 5: Brokers and Clusters

## الخطوة 0: كل الكلام اللي فات... شغال فين بالظبط؟

في كل الدروس اللي فاتت، اتكلمنا عن `Topics` و`Partitions` و`Producers` و`Consumers` — لكن كل ده مفاهيم منطقية. السؤال المنطقي دلوقتي: الـ `Partitions` دي متخزنة فعليًا على إيه؟ على أنهي جهاز؟ الإجابة: على سيرفرات اسمها `Brokers`.

## الخطوة 1: إيه هو الـ Broker؟

الـ `Broker` هو سيرفر واحد شغال من Kafka، مسؤول عن تخزين البيانات فعليًا (الـ `Partitions`) والرد على طلبات الـ `Producers` والـ `Consumers`. فكر فيه زي مبنى فرعي من مباني المطار — كل مبنى بيخزن ويدير جزء من البيانات.

## الخطوة 2: مشكلة الـ Broker الواحد

لو كل حاجة متخزنة على `Broker` واحد بس:
- لو الجهاز ده وقع، النظام كله بيقف (`Single point of failure`).
- سعة التخزين والمعالجة محدودة بطاقة جهاز واحد بس.

## الخطوة 3: الحل — Cluster

عشان نحل المشكلة دي، بنشغل أكتر من `Broker` مع بعض، ومجموعتهم دي بتتسمى `Cluster`. الـ `Cluster` بيشتغل كوحدة واحدة متكاملة من وجهة نظر الـ `Producers` والـ `Consumers`، حتى لو فعليًا مكوّن من أجهزة متعددة منفصلة.

```
Cluster
┌───────────┐  ┌───────────┐  ┌───────────┐
│ Broker 1  │  │ Broker 2  │  │ Broker 3  │
└───────────┘  └───────────┘  └───────────┘
```

## الخطوة 4: إزاي بيتوزع الـ Partitions على الـ Brokers؟

بما إن عندنا أكتر من `Broker` دلوقتي، Kafka بيوزع الـ `Partitions` بتاعة الـ `Topic` الواحد على الـ `Brokers` المختلفة. وهنا سؤال مهم لازم نوضحه بدقة: هل ممكن نفس الـ `Broker` يحمل أكتر من `Partition` لنفس الـ `Topic`؟

**الإجابة: أيوه**، ده طبيعي جدًا وشائع. تخيل عندك `Topic` بـ 6 `Partitions`، وعندك 3 `Brokers` بس — لازم رياضيًا كل `Broker` يحمل أكتر من partition واحدة.

```
Cluster (3 Brokers) - Topic له 6 Partitions:

Broker 1: [P0] [P3]
Broker 2: [P1] [P4]
Broker 3: [P2] [P5]
```

اللي مش بيحصل هو إن **نفس النسخة بالظبط** من نفس الـ `Partition` تتكرر مرتين على نفس الـ `Broker` — كل partition (أو بالأخص كل نسخة منها) بتتخزن مرة واحدة بس على broker معين، مش نفسها مرتين على نفس الجهاز. (هنشرح فكرة "النسخ المتعددة" دي بالتفصيل في درس الـ Replication الجاي).

## الخطوة 5: مين عارف مين؟ — Cluster Metadata

عشان الـ `Cluster` يشتغل كوحدة واحدة، لازم كل `Broker` فيه يعرف معلومات أساسية عن باقي الـ `Cluster`:

- مين الـ `Broker` المسؤول (`Leader`) عن كل `Partition` معينة.
- إيه توزيع النسخ الاحتياطية لكل partition.
- حالة باقي الـ `Brokers` (شغالين ولا واقعين).

المعلومات دي مجتمعة اسمها `Cluster Metadata`، ولازم تكون محدّثة ومتزامنة بين كل الـ brokers طول الوقت — وده سؤال هيقودنا مباشرة لدرسين جايين: مين بيدير المعلومات دي (`ZooKeeper` مقابل `KRaft`)، وإزاي بيتحدد الـ `Leader` أصلًا (`Replication`).

## مثال بكود Python (`kafka-python-ng`)

معرفة كل الـ `Brokers` الموجودة في الـ `Cluster` (خطوة 3):

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers="localhost:9092")
cluster = consumer.cluster   # معلومات الـ Cluster Metadata (خطوة 5)

for broker in cluster.brokers():
    print(f"Broker id={broker.nodeId} → {broker.host}:{broker.port}")

consumer.close()
```

معرفة على أنهي `Broker` كل `Partition` من partitions الـ `Topic` متخزنة (خطوة 4):

```python
from kafka.admin import KafkaAdminClient

admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
metadata = admin.describe_topics(["flights.checkin"])

for topic in metadata:
    for partition in topic["partitions"]:
        print(f"Partition {partition['partition']} → Broker id={partition['leader']}")

admin.close()
```

## تمرين صغير

لو عندك `Cluster` من `Broker` واحد بس (`single-broker`)، وعايز تعمل `Topic` بـ 4 `Partitions`:

- هل ده هيشتغل من ناحية توزيع الـ `Partitions`؟ ليه أو ليه لأ؟
- لو ده الـ `Cluster` بتاعك الوحيد، إيه المشكلة الأساسية اللي هتقابلها لو الجهاز ده وقع فجأة؟

---
◀ [الدرس السابق: Consumer Groups](./04-consumer-groups.md) | [الفهرس](./00-index.md) | التالي ▶ [الدرس 6: Replication](./06-replication.md)

</div>
