<div dir="rtl">

# الدرس 4: Consumer Groups

## الخطوة 0: مشكلة — Consumer واحد مش كفاية

في الدرس اللي فات، اتكلمنا عن `Consumer` واحد بيقرأ رسائل من `Topic`. لكن لو الـ `Topic` بتاعنا مقسم لـ 6 `Partitions` وبييجيله آلاف الرسائل في الثانية، `Consumer` واحد بس مش هيلحق يعالج كل الحمل ده لوحده. محتاجين طريقة نوزع بيها الشغل على أكتر من `Consumer` في نفس الوقت.

## الخطوة 1: الحل الساذج — كذا Consumer مستقل يقرأوا كله

أول فكرة ممكن تيجي في بالك: نشغل كذا `Consumer` مستقل، وكل واحد يقرأ *كل* رسائل الـ `Topic`. المشكلة هنا: كل الرسائل هتتعالج مرات متكررة (مرة لكل consumer)، وده مش اللي عايزينه لو غرضنا توزيع الحمل، لأننا عايزين كل رسالة تتعالج مرة واحدة بس، بس بسرعة أكبر بفضل التوازي.

## الخطوة 2: الحل الصحيح — Consumer Group

عشان نحل المشكلة دي، Kafka بيديك مفهوم `Consumer Group`: مجموعة من الـ `Consumers` بتشتغل مع بعض تحت اسم واحد، وبتتقاسم شغل قراءة نفس الـ `Topic` — كل `Consumer` جوه المجموعة بياخد `Partitions` مختلفة، مش كل حاجة.

تخيل فريق موظفين شغالين مع بعض على بوابة الصعود، وكل واحد فيهم مسؤول عن جزء من الطابور عشان يخلصوا بسرعة — ده بالظبط دور الـ `Consumer Group`.

## الخطوة 3: القاعدة الذهبية للتوزيع

القاعدة الأساسية اللي كل حاجة تانية مبنية عليها: **كل `Partition` بتتقرأ بواسطة `Consumer` واحد بس جوه نفس الـ `Consumer Group`** في نفس اللحظة. مافيش اتنين consumers من نفس الجروب يقرأوا نفس الـ partition في نفس الوقت.

```
Topic: flights.boarding (4 partitions)
Consumer Group: "boarding-team"

Consumer A ──▶ Partition 0, Partition 1
Consumer B ──▶ Partition 2
Consumer C ──▶ Partition 3
```

**ليه القاعدة دي مهمة؟** لو اتنين consumers قروا نفس الـ partition في نفس الوقت، هيحصل تضارب: مين يعمل commit بعد التاني؟ ومين المسؤول عن أي offset؟ القاعدة دي بتمنع التعقيد ده من الأساس، وبتضمن إن كل رسالة جوه الـ group تتعالج مرة واحدة بس.

## الخطوة 4: نتيجة منطقية — العدد الأقصى للتوازي

بما إن كل `Partition` مربوطة بـ `Consumer` واحد بس جوه الجروب، يبقى أقصى عدد consumers مفيدين جوه جروب واحد = عدد الـ `Partitions` بتاعة الـ `Topic`. لو عدد الـ `Consumers` أكتر من عدد الـ `Partitions`، الزيادة دي هتقعد من غير شغل (`Idle`).

```
4 Partitions + 5 Consumers (نفس الـ Group):
Consumer A → Partition 0
Consumer B → Partition 1
Consumer C → Partition 2
Consumer D → Partition 3
Consumer E → (Idle — مفيش partition متاحة له!)
```

## الخطوة 5: طيب لو Consumer وقع أو انضم جديد؟

النظام ده لازم يكون مرن: إيه اللي بيحصل لو واحد من الـ consumers وقع فجأة (كراش)، أو consumer جديد انضم للجروب؟ التوزيع الحالي للـ partitions لازم يتغير عشان محدش يفضل من غير تغطية. العملية دي اسمها `Rebalancing`.

```
قبل: Consumer A(P0,P1) + Consumer B(P2,P3)
Consumer B يقع فجأة!
Rebalancing يحصل...
بعد: Consumer A(P0,P1,P2,P3)  ← ياخد كل حاجة لحد ما B يرجع أو consumer جديد يدخل
```

## الخطوة 6: تكلفة الـ Rebalancing

أثناء عملية الـ `Rebalancing`، Kafka بيوقف مؤقتًا معالجة الرسائل لكل consumers الجروب (`Stop-the-world` لفترة قصيرة) لحد ما التوزيع الجديد يتثبت بالكامل. يعني rebalancing متكرر كتير (زي لو consumer بيقع ويرجع كل شوية) بيبقى مؤشر خطر على استقرار النظام، مش مجرد تفصيلة تقنية بسيطة.

## الخطوة 7: إزاي بيتحدد التوزيع بالظبط؟ استراتيجيات الـ Assignment

لما الـ `Rebalancing` يحصل، Kafka محتاج استراتيجية يقرر بيها مين ياخد أنهي partition. أشهر الاستراتيجيات:

- **`Range`**: بيوزع partitions متتالية لكل consumer (زي: consumer A ياخد 0-1، consumer B ياخد 2-3).
- **`RoundRobin`**: بيوزعهم بالتبادل واحد واحد على كل الـ consumers.
- **`Sticky`**: بيحاول يحافظ على أكبر قدر من التوزيع القديم زي ما هو أثناء الـ rebalancing، عشان يقلل الحركة الزيادة والتأثير على الـ consumers اللي مكنش لهم دخل في سبب الـ rebalancing.

## الخطوة 8: لو عايز نفس الرسائل تتعالج بأكتر من نظام مختلف؟

سؤال منطقي: لو عايزين نظامين مختلفين تمامًا (مثلًا نظام تحليلات ونظام إشعارات) يقرأوا *كل* رسائل نفس الـ `Topic`، من غير ما يتقاسموا الشغل زي اللي شرحناه فوق؟ الحل بسيط: كل نظام يبقى في `Consumer Group` منفصل خالص. كل `Consumer Group` بيتعامل بشكل مستقل تمامًا عن باقي الجروبات، وبيقرأ كل الرسائل بنفسه من الأول.

```
Topic: flights.boarding

Consumer Group "analytics-team":  ياخد كل الـ partitions لوحده (مجموعة مستقلة)
Consumer Group "notifications-team": كمان ياخد كل الـ partitions لوحده (مجموعة مستقلة تانية)

→ نفس الرسالة بتتقرأ مرة بواسطة analytics-team، ومرة تانية بواسطة notifications-team
```

## مثال بكود Python (`kafka-python-ng`)

تشغيل أكتر من `Consumer` في نفس الـ `Consumer Group` (خطوة 2-3) — الكود ده بالظبط نفسه، بس تشغله في أكتر من عملية (`process`) منفصلة، وكلهم بنفس الـ `group_id`:

```python
from kafka import KafkaConsumer

# شغّل النسخة دي كذا مرة في terminals منفصلة (تمثيل لـ Consumer A و B و C)
consumer = KafkaConsumer(
    "flights.boarding",
    bootstrap_servers="localhost:9092",
    group_id="boarding-team",   # نفس الاسم لكل النسخ = نفس الـ Consumer Group
)

for message in consumer:
    print(f"[{consumer.config['client_id']}] partition={message.partition} offset={message.offset}")
```

Kafka هيوزع الـ `Partitions` تلقائيًا بين النسخ دي بمجرد ما تشتغل، وهتلاحظ إعادة التوزيع (`Rebalancing`، خطوة 5) لو قفلت واحدة منهم وشغلتها تاني.

معرفة توزيع الـ `Partitions` الحالي على أعضاء الجروب باستخدام `KafkaAdminClient` (خطوة 3-4):

```python
from kafka.admin import KafkaAdminClient

admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
description = admin.describe_consumer_groups(["boarding-team"])

for group in description:
    for member in group.members:
        print(f"Member: {member.member_id} → Partitions: {member.member_assignment}")

admin.close()
```

## تمرين صغير

عندك `Topic` بـ 6 `Partitions`، وعندك `Consumer Group` فيه 3 consumers شغالين:

- كل consumer هياخد كام partition في الحالة العادية؟
- لو consumer رابع دخل الجروب فجأة، هيحصل إيه بالظبط؟
- لو الـ 3 consumers دول كانوا كل واحد في `Consumer Group` منفصل بدل جروب واحد، هيتغير التوزيع إزاي، وهيتغير عدد مرات معالجة كل رسالة إزاي؟

---
◀ [الدرس السابق: Consumers](./03-consumers.md) | [الفهرس](./00-index.md) | التالي ▶ [الدرس 5: Brokers and Clusters](./05-brokers-clusters.md)

</div>
