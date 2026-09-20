<div dir="rtl" align="right">

# Consumer Group — مجموعة من الـ Consumers بتشتغل مع بعض

تمام! وصلنا لمفهوم مهم جداً، وهو اللي هيوضح إزاي أنظمة حقيقية بتتعامل مع كميات ضخمة من البيانات بالتوازي.

## 1. الاسم بالإنجليزية

**المصطلح:** Consumer Group

**الترجمة:** مجموعة من الـ Consumers بتشتغل مع بعض

## 2. شرح بسيط جداً بالعربية

**التعريف الأساسي:**

الـ Consumer Group هو مجموعة من الـ Consumers، بيشتغلوا مع بعض تحت اسم واحد مشترك، عشان يقسموا شغل قراءة الـ Partitions بينهم.

**النقطة الجوهرية:**

كل Partition جوه الـ Topic، بيتقرأ من طرف Consumer واحد بس جوه نفس الـ Group، مش أكتر.

**ده معناه:**

الرسالة الواحدة مش بتتقرأ مرتين جوه نفس الـ Consumer Group. لكنها ممكن تتقرأ من Consumer Group تاني تماماً، وده حاجة تانية خالص هنشرحها.

## 3. مثال المطار — Airport

**السياق:**

فاكر `Topic: flight_status` مقسوم لـ 3 Partitions. تخيل دلوقتي إن نظام تسجيل الوصول مش Consumer واحد بس، لأ هو فريق كامل من 3 موظفين، كل واحد شايل شاشة.

**التوزيع:**

<div dir="ltr" align="left">

```text
Consumer Group: "checkin-system"

الموظف الأول (Consumer 1) → مسؤول عن Partition 0
الموظف الثاني (Consumer 2) → مسؤول عن Partition 1
الموظف الثالث (Consumer 3) → مسؤول عن Partition 2
```

</div>

**الفايدة:**

بدل ما موظف واحد يقرأ كل الرحلات لوحده وياخد وقت طويل، الشغل اتقسم على 3 موظفين، وكل واحد بيشتغل بالتوازي مع التاني.

## 4. الرسم التوضيحي

<div dir="ltr" align="left">

```text
Topic: flight_status ✈️  (3 Partitions)

┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Partition 0   │  │  Partition 1   │  │  Partition 2   │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Consumer 1    │  │  Consumer 2    │  │  Consumer 3    │
└──────────────┘  └──────────────┘  └──────────────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                           ▼
                ┌────────────────────┐
                │  Consumer Group:      │
                │  "checkin-system"      │
                └────────────────────┘
```

</div>

> **نقطة مهمة في الرسم:** كل Consumer مسؤول عن Partition واحد بس. مفيش تداخل، ومفيش رسالة بتتقرأ مرتين جوه نفس الـ Group.

## 5. مثال Kafka حقيقي — لما نضيف Consumer Group تاني

**السيناريو:**

تخيل عندنا كمان نظام تاني اسمه "نظام الإشعارات" (Notification System)، وهو كمان محتاج يقرأ من نفس `Topic: flight_status`، بس لغرض مختلف (يبعت إشعارات للركاب).

<div dir="ltr" align="left">

```text
Topic: flight_status ✈️  (3 Partitions)

┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Partition 0   │  │  Partition 1   │  │  Partition 2   │
└───┬──────┬───┘  └───┬──────┬───┘  └───┬──────┬───┘
    │      │           │      │           │      │
    ▼      │           ▼      │           ▼      │
┌────────┐ │      ┌────────┐ │      ┌────────┐ │
│Consumer1│ │      │Consumer2│ │      │Consumer3│ │
└────────┘ │      └────────┘ │      └────────┘ │
Group:     │      Group:     │      Group:     │
checkin-   │      checkin-   │      checkin-   │
system     │      system     │      system     │
           ▼                 ▼                 ▼
      ┌────────┐        ┌────────┐        ┌────────┐
      │Consumer A│       │Consumer B│       │Consumer C│
      └────────┘        └────────┘        └────────┘
      Group:            Group:            Group:
      notification-      notification-      notification-
      system             system             system
```

</div>

**النقطة الأهم في الرسم ده:**

نفس الرسالة (مثلاً "EK1 landed") بتتقرأ مرتين بالظبط — مرة من `checkin-system` ومرة من `notification-system`. لكن جوه كل Group لوحده، الرسالة بتتقرأ مرة واحدة بس.

**القاعدة الذهبية:**

<div dir="ltr" align="left">

```text
كل Consumer Group بيشوف كل رسايل الـ Topic (بترتيب Partitions بتاعته)
كل Partition جوه نفس الـ Group، بيتقرأ من Consumer واحد بس
```

</div>

## 6. ماذا يحدث داخل Kafka؟ — مفهوم Rebalancing

**السؤال المهم:**

إيه اللي بيحصل لو واحد من الـ Consumers الثلاثة (مثلاً Consumer 2) وقع فجأة؟

**الإجابة خطوة بخطوة:**

1. Kafka يلاحظ إن Consumer 2 مابقاش بيرد (Heartbeat توقف)
2. Kafka يشيل Consumer 2 من الـ Group
3. Kafka يعيد توزيع الـ Partitions على الـ Consumers الباقيين
4. Partition 1 (اللي كان Consumer 2 مسؤول عنها) بتتوزع على Consumer 1 أو Consumer 3
5. الـ Consumer الجديد اللي استلم الـ Partition، بيبدأ من آخر Committed Offset محفوظ ليها

**اسم العملية دي:** **Rebalancing**

**مثال على الرسالة اللي ممكن تشوفها في اللوجات:**

<div dir="ltr" align="left">

```text
Consumer group 'checkin-system' is rebalancing
```

</div>

**الشرح بالعربي للرسالة دي:**

ده معناه إن Kafka دلوقتي بتعيد توزيع الـ Partitions بين الـ Consumers المتبقيين في الـ Group، غالباً بسبب إن Consumer اتشال أو انضم واحد جديد.

## 7. Python Code — تشغيل أكتر من Consumer في نفس الـ Group

الكود بيبقى مطابق تماماً بين كل الـ Consumers، الفرق الوحيد إنك بتشغله في processes منفصلة:

<div dir="ltr" align="left">

```python
from kafka import KafkaConsumer
import json

def start_consumer(consumer_name):
    consumer = KafkaConsumer(
        'flight_status',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id='checkin-system',       # نفس الاسم بالظبط في كل نسخة
        auto_offset_reset='earliest',
        enable_auto_commit=False
    )

    for message in consumer:
        flight = message.value
        print(f"[{consumer_name}] Partition {message.partition}, Offset {message.offset}")
        process_flight_update(flight)
        consumer.commit()
```

</div>

**شرح النقطة الأهم:**

<div dir="ltr" align="left">

```python
group_id='checkin-system'
```

</div>

ده اللي بيربط الـ Consumers ببعض كـ Group واحد. لو شغلت الدالة دي 3 مرات (3 processes منفصلة)، Kafka هيتعرف عليهم أوتوماتيك كـ Group واحد، وهيوزع الـ 3 Partitions عليهم لوحده — إنت مش محتاج تحدد يدوياً مين ياخد أنهي Partition.

## 8. مقارنة مهمة: Consumer مقابل Consumer Group

**الفرق الأساسي:**

الـ Consumer هو "الموظف الفردي"، والـ Consumer Group هو "الفريق كله".

**النقطة:**

لو عندك Consumer واحد بس جوه Group، هو هيقرأ من كل الـ Partitions لوحده (زي ما شرحنا قبل كده). لو زودت عدد الـ Consumers جوه نفس الـ Group، الشغل هيتقسم عليهم أوتوماتيك.

## 9. الخطأ الشائع

**الخطأ الأول:**

الاعتقاد إنك لو زودت عدد الـ Consumers جوه Group، السرعة هتزيد دايماً.

**التصحيح:**

لو عدد الـ Consumers أكتر من عدد الـ Partitions، الـ Consumers الزيادة هيقعدوا من غير شغل خالص (Idle). يعني لو عندك 3 Partitions و5 Consumers، هيبقى فيه 2 Consumers قاعدين مبيعملوش حاجة.

<div dir="ltr" align="left">

```text
Topic: flight_status (3 Partitions)
Consumer Group فيها 5 Consumers:

Consumer 1 → Partition 0
Consumer 2 → Partition 1
Consumer 3 → Partition 2
Consumer 4 → ❌ مفيش Partition متاحة له
Consumer 5 → ❌ مفيش Partition متاحة له
```

</div>

**القاعدة الذهبية:**

<div dir="ltr" align="left">

```text
أقصى استفادة من التوازي = عدد الـ Partitions
```

</div>

**الخطأ التاني:**

الخلط بين الـ Groups: لو استخدمت `group_id` مختلف، الـ Consumers مش هيتشاركوا في قراءة نفس الرسايل — كل Group بيشوف كل حاجة لوحده بشكل مستقل تماماً.

## 10. تمرين صغير

**الموقف:**

<div dir="ltr" align="left">

```text
Topic: baggage_events له 4 Partitions
عندك Consumer Group اسمها "baggage-tracker" فيها Consumer واحد بس دلوقتي
```

</div>

**سؤالي ليك:**

لو زودت عدد الـ Consumers جوه الـ Group دي لـ 2 (يعني بقى عندك Consumer A و Consumer B)، إزاي هيتقسم الـ 4 Partitions بينهم؟ وبعدين لو زودت العدد لـ 4 Consumers، إيه اللي هيتغير؟ وإيه اللي هيحصل لو زودت لـ 6 Consumers؟

</div>
