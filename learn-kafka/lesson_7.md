<div dir="rtl">

# الدرس 7: ZooKeeper vs. KRaft

## الخطوة 0: سؤال معلق من الدرس اللي فات

في درس الـ `Brokers and Clusters` وصلنا لسؤال: مين اللي بيدير الـ `Cluster Metadata` — يعني مين عارف مين هو الـ `Leader` لكل partition، وحالة كل broker (شغال ولا واقع)؟ لازم يكون فيه جهة مسؤولة عن المعلومات دي وتحديثها باستمرار.

## الخطوة 1: الحل القديم — نظام خارجي منفصل (ZooKeeper)

تاريخيًا، Kafka كان بيعتمد على نظام تنسيق خارجي تمامًا اسمه `ZooKeeper` لتخزين وإدارة الـ `Metadata` دي.

```
┌─────────────┐
│  ZooKeeper  │  ◀── نظام منفصل تمامًا، لازم يتشغل جنب Kafka
│  Ensemble   │
└──────┬──────┘
       │ (يدير الـ metadata)
┌──────┴──────────────────┐
│   Kafka Cluster          │
│  Broker1  Broker2  Broker3│
└──────────────────────────┘
```

## الخطوة 2: مشاكل النظام القديم

الاعتماد على `ZooKeeper` كنظام منفصل جاب معاه مشاكل عملية:

- محتاج تشغيل وصيانة نظام كامل منفصل (`Zookeeper Ensemble`) جنب الـ `Kafka Cluster` — يعني نظامين لازم تديرهم بدل واحد.
- تعقيد إضافي كبير في الإعداد والـ `Ops` (نسخ احتياطي، مراقبة، تحديثات — كل ده لنظامين منفصلين).
- حد أقصى عملي لعدد الـ `Partitions` اللي الـ cluster يقدر يتحملها بكفاءة، بسبب الحمل على `ZooKeeper` نفسه.

## الخطوة 3: الحل الحديث — إدارة الـ Metadata داخليًا (KRaft)

عشان نحل المشاكل دي، Kafka طور نظام بديل اسمه `KRaft` (اختصار `Kafka Raft`)، وفكرته الأساسية: الـ `Metadata` بقت بتتدار *داخل* Kafka نفسه، من غير أي نظام خارجي منفصل خالص.

## الخطوة 4: إزاي KRaft بيشتغل من غير نظام خارجي؟

بدل الاعتماد على `ZooKeeper`، مجموعة مختارة من الـ `Brokers` نفسها بتاخد دور إضافي اسمه `Controller`، وبيستخدموا بروتوكول إجماع (`Raft consensus`) بينهم عشان يتفقوا على حالة الـ `Metadata` باستمرار.

```
┌─────────────────────────────────────┐
│         Kafka Cluster (KRaft)        │
│                                       │
│  Broker1(Controller)  Broker2  Broker3│
│         └── الـ metadata متدارة هنا داخليًا ──┘
└─────────────────────────────────────┘
```

يعني الـ `Broker` الواحد ممكن يكون بيلعب دورين مع بعض: دوره الأساسي كـ `Broker` عادي (يخزن partitions ويرد على producers/consumers)، ودور إضافي كـ `Controller` (يشارك في إدارة الـ metadata).

## الخطوة 5: مقارنة مباشرة

| | `ZooKeeper` (القديم) | `KRaft` (الحديث) |
|---|---|---|
| عدد الأنظمة المطلوبة | نظامين منفصلين (Kafka + ZooKeeper) | نظام واحد بس (Kafka) |
| تعقيد الإعداد والصيانة | أعلى | أبسط |
| قابلية التوسع (عدد partitions) | محدودة نسبيًا | أعلى بكفاءة أفضل |
| سرعة بدء تشغيل الـ cluster | أبطأ | أسرع |

## الخطوة 6: القرار في مشروعنا

مشروع المطار مبني بالكامل على `KRaft` مش `ZooKeeper` — وده كان جزء أساسي من الـ `docker-compose.yml`، حيث كل `Broker` بيتعرف بدور مزدوج ممكن (`broker` و/أو `controller`) من غير أي حاجة خارجية زيادة (فاكر مشكلة الـ `CLUSTER_ID` اللي لازم يكون base64-encoded UUID؟ دي تفصيلة خاصة بإعداد الـ KRaft بالظبط).

## مثال بكود Python (`kafka-python-ng`)

معرفة نوع النظام اللي الـ cluster شغال عليه مش حاجة الـ client بيسألها مباشرة (لأن ده تفصيلة داخلية في Kafka نفسه، مش في الـ protocol اللي بيتكلم بيه الـ client)، لكن نقدر نتأكد إن الـ cluster شغال ونشوف الـ `Controller` الحالي بتاعه:

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers="localhost:9092")
cluster = consumer.cluster

# في KRaft، أحد الـ brokers ده بيلعب دور الـ controller
print(f"Controller الحالي: Broker id={cluster.controller_id}")

for broker in cluster.brokers():
    print(f"Broker id={broker.nodeId} → {broker.host}:{broker.port}")

consumer.close()
```

```bash
# إعداد الـ docker-compose.yml لبروكر بدور مزدوج (broker + controller) في KRaft:
# KAFKA_PROCESS_ROLES: 'broker,controller'
# KAFKA_CONTROLLER_QUORUM_VOTERS: '1@broker-1:9093,2@broker-2:9095,3@broker-3:9093'
```

## تمرين صغير

- ليه شركة بتبني نظام Kafka جديد من الصفر النهاردة الأرجح تختار `KRaft` مباشرة بدل `ZooKeeper`؟
- تقدر تفتكر عيب واحد محتمل لو خلطت وضع فيه بعض الـ `Brokers` بيشتغلوا كـ `Controllers` وبعضهم لأ بشكل غير متوازن؟

---
◀ [الدرس السابق: Replication](./06-replication.md) | [الفهرس](./00-index.md) | التالي ▶ [الدرس 8: Retention Policies](./08-retention-policies.md)

</div>
