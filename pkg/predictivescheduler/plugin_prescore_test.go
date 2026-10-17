// plugin_prescore_test.go
//
// 測試對象：PreScore() 的資料流（ConfigMap → loadPredictions → CycleState），
// 以及「載入失敗時排程不可以中斷」這條設計原則。
//
// 用 client-go 內建的 fake clientset 取代真的 API Server，
// 概念上等同 Python 測試裡用 mock 取代 requests 或 kubernetes client。
//
// 預期：在 prescore.go 修正前，下面「失敗情境」的測試會 FAIL（紅燈），
// 因為目前 PreScore 遇到錯誤會回傳錯誤狀態、讓整個排程週期失敗；
// 修正（錯誤時記 log 並回傳 nil）之後應該全部 PASS（綠燈）。

package predictivescheduler

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// fakeFullHandle 同時提供 Node 快照（給 Score）與 clientset（給 PreScore）。
type fakeFullHandle struct {
	framework.Handle
	lister framework.SharedLister
	client kubernetes.Interface
}

func (h *fakeFullHandle) SnapshotSharedLister() framework.SharedLister { return h.lister }
func (h *fakeFullHandle) ClientSet() kubernetes.Interface              { return h.client }

func newPluginWithClient(cs kubernetes.Interface, nodes map[string]*framework.NodeInfo) *PredictiveScheduler {
	h := &fakeFullHandle{
		lister: &fakeSharedLister{nodeInfos: &fakeNodeInfoLister{nodes: nodes}},
		client: cs,
	}
	return &PredictiveScheduler{handle: h, targetUtilization: 0.5}
}

func newPredictionsConfigMap(data map[string]string) *v1.ConfigMap {
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "resource-predictions",
			Namespace: predictionsNamespace,
			Labels:    map[string]string{"app": "predictive-scheduler"},
		},
		Data: data,
	}
}

const validPredictionsJSON = `{
  "enabled": true,
  "max_staleness_seconds": -1,
  "predictions": {
    "web": {"predicted_cpu": 0.25, "predicted_memory": 0.25, "updated_at": "2026-10-10T00:00:00Z"}
  }
}`

// ---------- 正常路徑 ----------

func TestPreScore_LoadsPayloadIntoCycleState(t *testing.T) {
	cs := k8sfake.NewSimpleClientset(newPredictionsConfigMap(map[string]string{
		predictionsDataKey: validPredictionsJSON,
	}))
	pl := newPluginWithClient(cs, testNodes())
	state := framework.NewCycleState()

	status := pl.PreScore(context.Background(), state, newPod(optInLabels, "2", "8Gi"), nil)
	if !status.IsSuccess() {
		t.Fatalf("PreScore 應該成功，實際: %v", status)
	}

	payload, err := getPredictionsFromState(state)
	if err != nil {
		t.Fatalf("CycleState 應該有資料: %v", err)
	}
	entry, ok := GetPrediction(payload, "web")
	if !ok || entry.PredictedCPU != 0.25 || entry.PredictedMemory != 0.25 {
		t.Errorf("讀回的預測值不對: ok=%v entry=%+v", ok, entry)
	}
}

// 找不到 ConfigMap：loadPredictions 本來就設計成回傳 Enabled=false，不算錯誤。
func TestPreScore_NoConfigMapMeansDisabled(t *testing.T) {
	pl := newPluginWithClient(k8sfake.NewSimpleClientset(), testNodes())
	state := framework.NewCycleState()

	if status := pl.PreScore(context.Background(), state, newPod(optInLabels, "2", "8Gi"), nil); !status.IsSuccess() {
		t.Fatalf("沒有 ConfigMap 時 PreScore 應該成功，實際: %v", status)
	}
	payload, err := getPredictionsFromState(state)
	if err != nil {
		t.Fatalf("CycleState 應該有資料: %v", err)
	}
	if payload.Enabled {
		t.Error("沒有 ConfigMap 時 payload.Enabled 應該是 false")
	}
}

// ---------- 失敗情境：排程不可以中斷，必須 fallback ----------

func TestPreScore_FailuresMustNotBreakScheduling(t *testing.T) {
	tests := []struct {
		name   string
		client func() kubernetes.Interface
	}{
		{"JSON 格式壞掉（例如手動編輯多/少逗號）", func() kubernetes.Interface {
			return k8sfake.NewSimpleClientset(newPredictionsConfigMap(map[string]string{
				predictionsDataKey: `{"enabled": true,`,
			}))
		}},
		{"ConfigMap 缺少 predictions.json 這個 key", func() kubernetes.Interface {
			return k8sfake.NewSimpleClientset(newPredictionsConfigMap(map[string]string{
				"wrong-key": validPredictionsJSON,
			}))
		}},
		{"API 呼叫失敗（模擬 RBAC 不足或 API Server 異常）", func() kubernetes.Interface {
			cs := k8sfake.NewSimpleClientset()
			cs.PrependReactor("list", "configmaps",
				func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, fmt.Errorf("forbidden")
				})
			return cs
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl := newPluginWithClient(tt.client(), testNodes())
			state := framework.NewCycleState()
			pod := newPod(optInLabels, "2", "8Gi") // 靜態 requests: 2000m / 8Gi

			// 1) PreScore 不可以回傳錯誤（否則整個排程週期失敗）
			if status := pl.PreScore(context.Background(), state, pod, nil); !status.IsSuccess() {
				t.Fatalf("載入失敗時 PreScore 不應中斷排程，實際回傳: %v", status)
			}

			// 2) 接著 Score 要能 fallback 回靜態值：小 Node 佔比 0.5 → 100 分
			got, status := pl.Score(context.Background(), state, pod, "small")
			if !status.IsSuccess() {
				t.Fatalf("Score 應該成功: %v", status)
			}
			if got != 100 {
				t.Errorf("fallback 分數 = %d, 預期 100", got)
			}
		})
	}
}

// 完整串接：PreScore 讀到有效 ConfigMap → Score 用預測值算分（小 Node 預期 50）。
func TestPreScoreThenScore_UsesPrediction(t *testing.T) {
	cs := k8sfake.NewSimpleClientset(newPredictionsConfigMap(map[string]string{
		predictionsDataKey: validPredictionsJSON,
	}))
	pl := newPluginWithClient(cs, testNodes())
	state := framework.NewCycleState()
	pod := newPod(optInLabels, "2", "8Gi")

	if status := pl.PreScore(context.Background(), state, pod, nil); !status.IsSuccess() {
		t.Fatalf("PreScore 失敗: %v", status)
	}
	got, status := pl.Score(context.Background(), state, pod, "small")
	if !status.IsSuccess() {
		t.Fatalf("Score 失敗: %v", status)
	}
	if got != 50 {
		t.Errorf("分數 = %d, 預期 50（用預測值 0.25 算）；若是 100 代表走了靜態 fallback", got)
	}
}
