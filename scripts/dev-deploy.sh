#!/usr/bin/env bash
# dev-deploy.sh
#
# 把「測試 → build image → 載入 kind → 部署 → 驗證」打包成一個指令。
# 每次 build 都重新打一個唯一 tag，避免沿用同一個 tag 時
# kind 節點 / Pod 繼續用到舊 image 卻看不出來。
#
# 用法：
#   ./scripts/dev-deploy.sh                # 日常使用
#   ./scripts/dev-deploy.sh --skip-tests   # 跳過 go vet / go test
#   ./scripts/dev-deploy.sh --apply        # 先 kubectl apply YAML（首次部署或改過 YAML 時用）
#
# 可用環境變數覆蓋預設值（第一次執行請先確認印出來的值是否正確）：
#   NS              scheduler 所在 namespace        (預設 predictive-scheduler-system)
#   DEPLOY          Deployment 名稱                 (預設：該 namespace 只有一個時自動偵測)
#   KIND_CLUSTER    kind 叢集名稱                   (預設 research-cluster)
#   SRC_IMAGE       make local-image 產出的 image   (預設：自動挑最新含 IMAGE_GREP 的本地 image)
#   IMAGE_GREP      自動挑 image 時使用的關鍵字     (預設 kube-scheduler)
#   MANIFEST        deployment YAML 路徑            (預設 manifests/predictivescheduler/deploy.yaml)
#   ROLLOUT_TIMEOUT rollout 等待上限                (預設 120s)

set -euo pipefail

NS="${NS:-predictive-scheduler-system}"
DEPLOY="${DEPLOY:-}"
KIND_CLUSTER="${KIND_CLUSTER:-research-cluster}"
SRC_IMAGE="${SRC_IMAGE:-}"
IMAGE_GREP="${IMAGE_GREP:-kube-scheduler}"
MANIFEST="${MANIFEST:-manifests/predictivescheduler/deploy.yaml}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-120s}"

SKIP_TESTS=0
APPLY=0

for arg in "$@"; do
  case "$arg" in
    --skip-tests) SKIP_TESTS=1 ;;
    --apply)      APPLY=1 ;;
    -h|--help)    sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "未知參數: $arg" >&2; exit 2 ;;
  esac
done

step() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m[警告]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[失敗]\033[0m %s\n' "$*" >&2; exit 1; }

cd "$(git rev-parse --show-toplevel)"

# ---------- 1. 測試 ----------
if (( SKIP_TESTS == 0 )); then
  step "1/6 go vet + 單元測試"
  go vet ./pkg/predictivescheduler/...
  go test ./pkg/predictivescheduler/... -count=1 -race
else
  step "1/6 跳過測試（--skip-tests）"
fi

# ---------- 2. build ----------
step "2/6 make local-image"
make local-image

if [[ -z "$SRC_IMAGE" ]]; then
  # docker images 預設由新到舊排序，取第一個符合關鍵字且不是 <none> 的
  SRC_IMAGE="$(docker images --format '{{.Repository}}:{{.Tag}}' \
    | grep -v '<none>' | grep -i "$IMAGE_GREP" | head -1 || true)"
fi
[[ -n "$SRC_IMAGE" ]] || die "找不到剛 build 的 image，請用 SRC_IMAGE=... 指定"
echo "來源 image: $SRC_IMAGE"

# ---------- 3. 重新打唯一 tag 並載入 kind ----------
TAG="dev-$(date +%Y%m%d-%H%M%S)-$(git rev-parse --short HEAD)"
NEW_IMAGE="${SRC_IMAGE%:*}:${TAG}"

step "3/6 tag 為 ${NEW_IMAGE} 並載入 kind 叢集 ${KIND_CLUSTER}"
docker tag "$SRC_IMAGE" "$NEW_IMAGE"
kind load docker-image "$NEW_IMAGE" --name "$KIND_CLUSTER"

for node in $(kind get nodes --name "$KIND_CLUSTER"); do
  if docker exec "$node" crictl images | grep -q "$TAG"; then
    echo "  ✔ $node 已有 $TAG"
  else
    die "$node 找不到 $TAG，kind load 可能失敗"
  fi
done

# ---------- 4. 部署 ----------
if (( APPLY == 1 )); then
  step "4/6 kubectl apply ${MANIFEST}"
  kubectl apply -f "$MANIFEST"
else
  step "4/6 略過 apply（需要時加 --apply）"
fi

if [[ -z "$DEPLOY" ]]; then
  mapfile -t names < <(kubectl -n "$NS" get deploy \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
  if (( ${#names[@]} == 1 )); then
    DEPLOY="${names[0]}"
  else
    die "namespace $NS 裡有 ${#names[@]} 個 Deployment，請用 DEPLOY=... 指定（首次部署請加 --apply）"
  fi
fi
echo "Deployment: $DEPLOY"

PULL_POLICY="$(kubectl -n "$NS" get deploy "$DEPLOY" \
  -o jsonpath='{.spec.template.spec.containers[0].imagePullPolicy}')"
if [[ "$PULL_POLICY" == "Always" ]]; then
  warn "imagePullPolicy=Always，節點會嘗試去 registry 拉 ${TAG}（不存在），請改成 IfNotPresent"
fi

kubectl -n "$NS" set image "deploy/$DEPLOY" "*=${NEW_IMAGE}"

# ---------- 5. 等待 rollout ----------
step "5/6 等待 rollout（上限 ${ROLLOUT_TIMEOUT}）"
kubectl -n "$NS" rollout status "deploy/$DEPLOY" --timeout="$ROLLOUT_TIMEOUT"

# ---------- 6. 驗證 ----------
step "6/6 驗證"
echo "--- Pod 實際使用的 image ---"
kubectl -n "$NS" get pods -o jsonpath=\
'{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[0].image}{"\t"}{.status.containerStatuses[0].imageID}{"\n"}{end}'
kubectl -n "$NS" get pods -o jsonpath='{.items[*].spec.containers[0].image}' \
  | grep -q "$TAG" || die "沒有任何 Pod 使用新 image ${NEW_IMAGE}"

SA="$(kubectl -n "$NS" get deploy "$DEPLOY" \
  -o jsonpath='{.spec.template.spec.serviceAccountName}')"
SA="${SA:-default}"
echo "--- RBAC：$SA 能否讀取 kube-system 的 ConfigMap ---"
kubectl auth can-i list configmaps -n kube-system \
  --as="system:serviceaccount:${NS}:${SA}" || warn "RBAC 不足，PreScore 讀不到 ConfigMap"

echo "--- 最近 20 行 log ---"
kubectl -n "$NS" logs "deploy/$DEPLOY" --tail=20 || true

printf '\n\033[1;32m部署完成：%s\033[0m\n' "$NEW_IMAGE"