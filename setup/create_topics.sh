#!/bin/bash
# ============================================================
# إنشاء الـ Topics الأساسية بتاعة نظام المطار
# كل Topic ليه إعدادات مختلفة حسب طبيعة بياناته
# ============================================================
#
# ملحوظة مهمة: أداة kafka-topics مش مثبتة على جهازك (Host)،
# هي موجودة جوه الـ Container بتاع الـ Broker نفسه.
# عشان كده كل أمر هنا بيتنفذ عن طريق:
#     docker exec -it broker-1 kafka-topics ...
# يعني "ادخل جوه Container اسمه broker-1 وشغل الأمر ده من جواه"

CONTAINER="broker-1"
BROKER="localhost:9092"   # ده عنوان الـ Broker من الداخل (جوه شبكة Docker)

echo "=== Creating Topic: flight_status ✈️  ==="
# 3 Partitions عشان نوزع الشغل على 3 Consumers بالتوازي (Consumer Group)
# Replication Factor = 3 عشان دي بيانات حساسة (حالة الرحلة) ومينفعش تتفقد
# acks='all' من ناحية الـ Producer هيستفيد فعلياً هنا لأن فيه 3 نسخ حقيقية
docker exec -it $CONTAINER kafka-topics --create \
  --topic flight_status \
  --bootstrap-server $BROKER \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=1209600000 \
  --config min.insync.replicas=2 \
  --if-not-exists

echo "=== Creating Topic: baggage_events 🧳 ==="
# 4 Partitions عشان حجم بيانات الأمتعة أكبر (كل شنطة ليها كذا حدث)
docker exec -it $CONTAINER kafka-topics --create \
  --topic baggage_events \
  --bootstrap-server $BROKER \
  --partitions 4 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2 \
  --if-not-exists

echo "=== Creating Topic: customs_clearance 🛂 ==="
docker exec -it $CONTAINER kafka-topics --create \
  --topic customs_clearance \
  --bootstrap-server $BROKER \
  --partitions 2 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2 \
  --if-not-exists

echo "=== Creating Topic: gate_assignment (Log Compaction) 🚪 ==="
# ده Topic مختلف تماماً في نوع الـ Retention:
# مش عايزين تاريخ كل التغييرات، عايزين بس "آخر بوابة" لكل رحلة
# فبنستخدم cleanup.policy=compact بدل المسح بالوقت
docker exec -it $CONTAINER kafka-topics --create \
  --topic gate_assignment \
  --bootstrap-server $BROKER \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --if-not-exists

echo "=== Creating Topic: delayed_flights_count (Kafka Streams output) 📊 ==="
docker exec -it $CONTAINER kafka-topics --create \
  --topic delayed_flights_count \
  --bootstrap-server $BROKER \
  --partitions 1 \
  --replication-factor 3 \
  --if-not-exists

echo "=== Creating Topic: airport-delay-monitor-delayed-counts-by-airline-changelog (Faust KTable) 📈 ==="
# ده Topic داخلي بتاع Faust نفسه (مش من تصميمنا احنا)، بيستخدمه
# عشان يحفظ حالة الـ KTable (delayed_counts) بشكل دائم.
#
# لو سبناه لـ Faust ينشئه بنفسه وقت التشغيل، بيحصل Race Condition:
# Faust يطلب إنشاءه، وقبل ما الـ Metadata تنتشر بالكامل جوه الـ
# Cluster، Faust يحاول يتأكد من عدد الـ Partitions ويلاقيها "0"
# مؤقتاً، فيرمي:
#   PartitionsMismatch: ... has 0 partitions
#
# عشان كده بننشئه إحنا يدوياً هنا الأول، بنفس عدد الـ Partitions
# بتاع الـ Topic المصدر (flight_status = 3)، عشان Faust يلاقيه
# جاهز ومتزامن تماماً من الأول ومحتاجش ينشئه بنفسه.
#
# نستخدم cleanup.policy=compact لأن الـ Changelog Topic بطبيعته
# محتاج يحتفظ بس بآخر قيمة لكل Key (نفس مبدأ Log Compaction
# اللي استخدمناه في gate_assignment)
docker exec -it $CONTAINER kafka-topics --create \
  --topic airport-delay-monitor-delayed-counts-by-airline-changelog \
  --bootstrap-server $BROKER \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --if-not-exists

echo ""
echo "=== كل الـ Topics اتعملت. عرض التفاصيل: ==="
docker exec -it $CONTAINER kafka-topics --describe --bootstrap-server $BROKER
