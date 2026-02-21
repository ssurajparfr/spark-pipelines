cat << 'EOF' > README.md
# Spark Pipelines (Feature Teams)

Repository for defining Spark jobs and submitting pipelines using SparkSqlPipeline CR.

Platform team provides the operator.
Feature teams provide:
- Spark Python scripts
- Input/output paths
- Pipeline CR definition

---

## 🚀 Setup

### 1️⃣ Upload Spark Jobs to MinIO

mc cp jobs/step1_process.py <alias>/spark-jars/
mc cp jobs/step2_summary.py <alias>/spark-jars/

---

### 2️⃣ Create MinIO Secret in Namespace

kubectl create secret generic minio-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=<key> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<secret> \
  -n <namespace>

---

### 3️⃣ Submit Pipeline

kubectl apply -f examples/demo-pipeline.yaml -n <namespace>

---

## 🔎 Monitor

kubectl get sparksqlpipeline -n <namespace>
kubectl describe sparksqlpipeline demo-pipeline -n <namespace>
kubectl logs <spark-driver-pod> -n <namespace>

---

## 🧹 Cleanup

kubectl delete -f examples/demo-pipeline.yaml -n <namespace>

SparkApplications are garbage collected automatically.
EOF