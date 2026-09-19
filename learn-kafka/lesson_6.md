<div dir="rtl">

# الدرس 6: Replication (Leader/Follower/ISR)

## الخطوة 0: مشكلة معلقة من الدرس اللي فات

في الدرس اللي فات وضحنا إن كل `Partition` بتتخزن على `Broker` معين. بس ده بيسيب مشكلة قديمة/جديدة: لو الـ `Broker` اللي فيه `Partition` معينة وقع (عطل كهرباء، صيانة، عطل هاردوير)، إيه اللي بيحصل للبيانات دي؟ هل بتتفقد؟ هل النظام بيقف عن العمل لحد ما الـ broker يرجع؟

## الخطوة 1: الحل — نسخ متعددة من نفس الـ Partition

الحل اللي Kafka بيقدمه: بدل ما كل `Partition` تتخزن نسخة واحدة بس على `Broker` واحد، بيتم الاحتفاظ بعدة نسخ متطابقة من نفس الـ `Partition`، موزعة على `Brokers` مختلفة. العملية دي اسمها `Replication`، وعدد النسخ اسمه `Replication Factor`.

```
Partition 0 بـ Replication Factor = 3:

Broker 1: [نسخة من Partition 0]
Broker 2: [نسخة من Partition 0]
Broker 3: [نسخة من Partition 0]
```

لو broker واحد وقع، فيه نسختين تانيين احتياط جاهزين.

## الخطوة 2: مشكلة جديدة — لو فيه 3 نسخ، مين اللي بيرد على القراءة/الكتابة؟

لو الـ 3 نسخ دول شغالين بالتوازي، ومين ما كان يقدر يستقبل قراءة أو كتابة، هتحصل مشكلة كبيرة: نسختين مختلفتين ممكن ياخدوا رسائل بترتيب مختلف، وهيبقى فيه تضارب في تحديد "إيه الترتيب الصحيح فعليًا؟".

## الخطوة 3: الحل — نسخة واحدة قائدة (Leader) والباقي متابعين (Followers)

عشان نتجنب التضارب ده، من بين النسخ المتعددة، نسخة واحدة بس بتتعين كـ `Leader`، والباقي بيبقوا `Followers`:

- **`Leader`**: هو النسخة اللي بتستقبل *كل* عمليات القراءة والكتابة من الـ `Producers` والـ `Consumers` — من غيرها.
- **`Follower`**: مهمته الوحيدة إنه ينسخ (`replicate`) البيانات من الـ `Leader` باستمرار، عشان يفضل محدّث وجاهز يبقى `Leader` بديل لو الأصلي وقع.

```
Partition 0 (Replication Factor = 3):

Broker 1: [Partition 0 - LEADER]   ◀── كل القراءة/الكتابة هنا فقط
Broker 2: [Partition 0 - Follower] ◀── بينسخ من الـ Leader فقط
Broker 3: [Partition 0 - Follower] ◀── بينسخ من الـ Leader فقط
```

بكده التضارب اتحل: مصدر الحقيقة الوحيد لأي partition هو الـ `Leader` بتاعها بس.

## الخطوة 4: مشكلة تانية — إيه اللي يضمن إن الـ Followers فعلًا محدّثين؟

مش كل الـ `Followers` بالضرورة "لاحقين" على آخر تحديث من الـ `Leader` في كل لحظة — لو follower بطيء، أو فيه مشكلة شبكة مؤقتة، ممكن يتأخر شوية عن آخر رسالة اتكتبت على الـ Leader.

## الخطوة 5: الحل — قائمة الـ ISR (In-Sync Replicas)

عشان نميز مين فعلًا "لاحق ومحدّث بالكامل" ومين "متأخر"، Kafka بيحتفظ بقايمة اسمها `ISR (In-Sync Replicas)` — فيها الـ `Leader` نفسه بالإضافة لكل الـ `Followers` اللي فعلًا محدّثين ولاحقين تمامًا.

```
ISR = {Leader, Follower(محدَّث), Follower(محدَّث)}
لو follower اتأخر كتير → بيتشال من الـ ISR مؤقتًا
لو رجع لاحق تاني → بيترجع للـ ISR
```

## الخطوة 6: هنا بترجع نقطة من درس الـ Producers — إعداد acks

فاكر في درس الـ Producers لما اتكلمنا عن `acks=all`؟ دلوقتي نقدر نفهمها بدقة أكبر: الرسالة متعتبرش "متأكد منها" في وضع `acks=all` غير لما **كل أعضاء الـ ISR** (مش كل النسخ الموجودة، ومش الـ Leader بس) يأكدوا استلامها.

## الخطوة 7: طيب لو الـ Leader نفسه وقع؟ — Leader Election

لو الـ `Broker` اللي فيه الـ `Leader` وقع، لازم يتحدد `Leader` جديد فورًا عشان النظام يكمل شغل — العملية دي اسمها `Leader Election`.

```
قبل: Broker 1 (Leader) + Broker 2, 3 (Followers - في الـ ISR)
Broker 1 يقع فجأة! 💥
Leader Election يحصل تلقائيًا...
بعد: Broker 2 (Leader جديد) + Broker 3 (Follower)
     Broker 1 (لما يرجع) → هيبقى Follower
```

## الخطوة 8: نقطة دقيقة جدًا — الـ Leader الجديد بيتختار من فين بالظبط؟

سؤال مهم: ليه Kafka بيختار الـ `Leader` الجديد من الـ `ISR` بس، مش من أي `Follower` موجود حتى لو مش لاحق؟

**السبب**: لو اخترنا follower متأخر (يعني مش في الـ ISR، لسه ماوصلوش آخر تحديثات) كـ leader جديد، ممكن نخسر بيانات كانت اتكتبت على الـ leader القديم ولسه الـ follower ده مانسخهاش. باختيار الـ leader الجديد من الـ ISR بس، بنضمن إنه محدّث بالكامل ومافيش فقدان بيانات.

## مثال بكود Python (`kafka-python-ng`)

إنشاء `Topic` بـ `Replication Factor` أكبر من 1 (خطوة 1):

```python
from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(bootstrap_servers="localhost:9092")

topic = NewTopic(
    name="flights.boarding",
    num_partitions=3,
    replication_factor=3,   # 3 نسخ من كل partition، موزعة على brokers مختلفة
)

admin.create_topics([topic])
admin.close()
```

معرفة مين الـ `Leader` ومين في الـ `ISR` لكل `Partition` (خطوة 3 و5):

```python
from kafka.admin import KafkaAdminClient

admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
metadata = admin.describe_topics(["flights.boarding"])

for topic in metadata:
    for partition in topic["partitions"]:
        print(
            f"Partition {partition['partition']} → "
            f"Leader: {partition['leader']}, "
            f"ISR: {partition['isr']}"
        )

admin.close()
```

ضبط `acks="all"` عشان يعتمد فعليًا على الـ `ISR` كامل (ربط مباشر بدرس الـ Producers، خطوة 6):

```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    acks="all",   # ينتظر تأكيد كل أعضاء الـ ISR، مش الـ Leader بس
)
producer.send("flights.boarding", value=b'{"event": "boarding_started"}')
producer.flush()
```

## تمرين صغير

لو عندك `Partition` بـ `Replication Factor = 3`، لكن follower واحد بس فيهم لاحق فعلًا (يعني الـ `ISR` = {Leader, Follower1} بس، وFollower2 متأخر ومش موجود في الـ ISR):

- لو استخدمت `acks=all`، هل الرسالة هتستنى تأكيد من Follower2 المتأخر كمان؟ ليه؟
- لو الـ `Leader` وقع دلوقتي، مين اللي المفروض يبقى `Leader` جديد؟ ليه بالظبط مش الـ follower التاني؟

---
◀ [الدرس السابق: Brokers and Clusters](./05-brokers-clusters.md) | [الفهرس](./00-index.md) | التالي ▶ [الدرس 7: ZooKeeper vs. KRaft](./07-zookeeper-vs-kraft.md)

</div>
