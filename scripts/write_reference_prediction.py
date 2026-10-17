"""
write_reference_prediction.py

為 test-reference-pod 寫入實驗用的預測資料，只改 ConfigMap、不動 Pod。
  python write_reference_prediction.py                       # B2: 0.3 / 0.2
  python write_reference_prediction.py --cpu 0.05 --mem 0.02 # B1
  python write_reference_prediction.py --disable             # A
  python write_reference_prediction.py --corrupt             # C
"""

import argparse
import json
from datetime import datetime, timezone

from kubernetes import client, config

NAMESPACE = "kube-system"
CONFIGMAP_NAME = "resource-predictions"
LABEL = {"app": "predictive-scheduler"}
DATA_KEY = "predictions.json"
WORKLOAD_ID = "test-reference-pod"


def build_text(cpu: float, mem: float, enabled: bool, corrupt: bool) -> str:
    if corrupt:
        return '{"enabled": true,'  # 刻意截斷的 JSON
    payload = {
        "enabled": enabled,
        "max_staleness_seconds": 600,
        "predictions": {
            WORKLOAD_ID: {
                "predicted_cpu": cpu,
                "predicted_memory": mem,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        },
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def write_to_configmap(text: str):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    body = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(name=CONFIGMAP_NAME, labels=LABEL),
        data={DATA_KEY: text},
    )
    try:
        v1.read_namespaced_config_map(name=CONFIGMAP_NAME, namespace=NAMESPACE)
        v1.replace_namespaced_config_map(name=CONFIGMAP_NAME, namespace=NAMESPACE, body=body)
        print(f"[更新] {NAMESPACE}/{CONFIGMAP_NAME}")
    except client.ApiException as e:
        if e.status == 404:
            v1.create_namespaced_config_map(namespace=NAMESPACE, body=body)
            print(f"[建立] {NAMESPACE}/{CONFIGMAP_NAME}")
        else:
            raise


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cpu", type=float, default=0.3)
    ap.add_argument("--mem", type=float, default=0.2)
    ap.add_argument("--disable", action="store_true", help="enabled=false（實驗 A）")
    ap.add_argument("--corrupt", action="store_true", help="寫入壞掉的 JSON（實驗 C）")
    args = ap.parse_args()

    text = build_text(args.cpu, args.mem, not args.disable, args.corrupt)
    print("[內容預覽]")
    print(text)
    write_to_configmap(text)