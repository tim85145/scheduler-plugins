// plugin_score_test.go
//
// 測試對象：PredictiveScheduler.Score() 本體（不是單獨的 helper 函式）。
// 驗證重點：
//   1. 命中預測值時，分數用「預測值換算」的結果算（手算對照）
//   2. 各種 fallback 情境（沒 opt-in / 查無 workload / enabled=false / CycleState 沒資料）
//      分數用「靜態 resource.requests」算
//   3. 預測值確實會改變分數（預測 vs 靜態不同）、異質 Node 分數不同（防全同分 bug）
//   4. Node 可分配資源為 0 時回傳錯誤，不是 NaN
//
// 手算基準（targetUtilization=0.5，referenceMachine = 4000m CPU / 16Gi）：
//   小 Node 4000m/16Gi，預測 0.25/0.25 → 絕對量 1000m/4Gi → 佔比 0.25 → 分數 50
//   大 Node 8000m/32Gi，同樣預測      → 佔比 0.125            → 分數 25
//   靜態 requests 2000m/8Gi：小 Node 佔比 0.5 → 100；大 Node 佔比 0.25 → 50
//
// 還沒有實際編譯執行過。newPayload() 裡的欄位名稱是依照
// score.go 的用法推測的，如果跟 load_predictions.go 的實際定義不同，
// 只需要改那一個 helper，其他測試不用動。

package predictivescheduler

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// ---------- 假的 Handle（類似 Python 的 Mock / 子類別只覆寫一個方法）----------
//
// Go 的「內嵌介面」技巧：struct 內嵌 framework.Handle，就自動擁有它所有方法
// （值是 nil，被呼叫會 panic），我們只覆寫 Score() 會用到的 SnapshotSharedLister。
// 概念上等同 Python 的 MagicMock(spec=Handle) 再指定一個方法的回傳值。

type fakeNodeInfoLister struct {
	framework.NodeInfoLister
	nodes map[string]*framework.NodeInfo
}

func (l *fakeNodeInfoLister) Get(name string) (*framework.NodeInfo, error) {
	ni, ok := l.nodes[name]
	if !ok {
		return nil, fmt.Errorf("node %s not found", name)
	}
	return ni, nil
}

type fakeSharedLister struct {
	framework.SharedLister
	nodeInfos *fakeNodeInfoLister
}

func (l *fakeSharedLister) NodeInfos() framework.NodeInfoLister { return l.nodeInfos }

type fakeHandle struct {
	framework.Handle
	lister framework.SharedLister
}

func (h *fakeHandle) SnapshotSharedLister() framework.SharedLister { return h.lister }

// ---------- 測試資料建構 helper ----------

func newNodeInfo(name, cpu, mem string) *framework.NodeInfo {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse(cpu),
				v1.ResourceMemory: resource.MustParse(mem),
			},
		},
	}
	ni := framework.NewNodeInfo()
	ni.SetNode(node)
	return ni
}

func newPod(labels map[string]string, cpu, mem string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default", Labels: labels},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "c",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse(cpu),
						v1.ResourceMemory: resource.MustParse(mem),
					},
				},
			}},
		},
	}
}

// newPayload 集中處理「建立預測資料」，欄位名稱若跟實際定義不同，只改這裡。
// MaxStalenessSeconds 設負數＝不檢查新鮮度，避免測試依賴 UpdatedAt 的時間格式。
// TODO: 對照 load_predictions.go 確認欄位名稱與 Workloads 的值是值型別還是指標。
func newPayload(enabled bool, workloadID string, cpu, mem float64) *PredictionsPayload {
	return &PredictionsPayload{
		Enabled:             enabled,
		MaxStalenessSeconds: -1,
		Predictions: map[string]PredictionEntry{
			workloadID: {PredictedCPU: cpu, PredictedMemory: mem},
		},
	}
}

// newTestPlugin 組出一個只含 nodes 的 plugin，targetUtilization 固定 0.5。
func newTestPlugin(nodes map[string]*framework.NodeInfo) *PredictiveScheduler {
	h := &fakeHandle{
		lister: &fakeSharedLister{nodeInfos: &fakeNodeInfoLister{nodes: nodes}},
	}
	return &PredictiveScheduler{handle: h, targetUtilization: 0.5}
}

func testNodes() map[string]*framework.NodeInfo {
	return map[string]*framework.NodeInfo{
		"small": newNodeInfo("small", "4", "16Gi"),
		"big":   newNodeInfo("big", "8", "32Gi"),
	}
}

var (
	optInLabels  = map[string]string{optInLabelKey: optInLabelValue, "app": "web"}
	noOptInLabel = map[string]string{"app": "web"}
)

// ---------- 測試 ----------

func TestScore_Scenarios(t *testing.T) {
	tests := []struct {
		name      string
		labels    map[string]string
		payload   *PredictionsPayload // nil = 不寫進 CycleState（模擬 PreScore 沒存到資料）
		nodeName  string
		wantScore int64
	}{
		{"命中預測 / 小Node", optInLabels, newPayload(true, "web", 0.25, 0.25), "small", 50},
		{"命中預測 / 大Node", optInLabels, newPayload(true, "web", 0.25, 0.25), "big", 25},
		{"沒有 opt-in label → 靜態", noOptInLabel, newPayload(true, "web", 0.25, 0.25), "small", 100},
		{"查無此 workload → 靜態", optInLabels, newPayload(true, "other", 0.25, 0.25), "small", 100},
		{"enabled=false → 靜態 / 大Node", optInLabels, newPayload(false, "web", 0.25, 0.25), "big", 50},
		{"CycleState 沒資料 → 靜態", optInLabels, nil, "small", 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl := newTestPlugin(testNodes())
			state := framework.NewCycleState()
			if tt.payload != nil {
				state.Write(predictionStateKey, &predictionStateData{payload: tt.payload})
			}
			pod := newPod(tt.labels, "2", "8Gi") // 靜態 requests: 2000m / 8Gi

			got, status := pl.Score(context.Background(), state, pod, tt.nodeName)
			if !status.IsSuccess() {
				t.Fatalf("Score 回傳非成功狀態: %v", status)
			}
			if got != tt.wantScore {
				t.Errorf("分數 = %d, 預期 %d", got, tt.wantScore)
			}
		})
	}
}

// 預測值必須真的影響結果：同一個 Node、同一個 Pod，有無預測分數要不同。
func TestScore_PredictionChangesResult(t *testing.T) {
	pl := newTestPlugin(testNodes())
	pod := newPod(optInLabels, "2", "8Gi")

	withPred := framework.NewCycleState()
	withPred.Write(predictionStateKey, &predictionStateData{payload: newPayload(true, "web", 0.25, 0.25)})
	withoutPred := framework.NewCycleState()

	a, _ := pl.Score(context.Background(), withPred, pod, "small")
	b, _ := pl.Score(context.Background(), withoutPred, pod, "small")
	if a == b {
		t.Errorf("有預測(%d)與無預測(%d)分數相同，預測值沒有影響評分", a, b)
	}
}

// 異質叢集：不同規格 Node 不可全部同分（對應之前互相抵銷的 bug）。
func TestScore_HeterogeneousNodesDiffer(t *testing.T) {
	pl := newTestPlugin(testNodes())
	pod := newPod(optInLabels, "2", "8Gi")
	state := framework.NewCycleState()
	state.Write(predictionStateKey, &predictionStateData{payload: newPayload(true, "web", 0.25, 0.25)})

	small, _ := pl.Score(context.Background(), state, pod, "small")
	big, _ := pl.Score(context.Background(), state, pod, "big")
	if small == big {
		t.Errorf("小 Node 與大 Node 分數相同(%d)，異質 Node 沒有被區分", small)
	}
}

// 可分配資源為 0 的 Node：要回傳錯誤，不能算出 NaN。
func TestScore_ZeroAllocatableReturnsError(t *testing.T) {
	broken := framework.NewNodeInfo()
	broken.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "broken"}})
	pl := newTestPlugin(map[string]*framework.NodeInfo{"broken": broken})

	_, status := pl.Score(context.Background(), framework.NewCycleState(), newPod(optInLabels, "1", "1Gi"), "broken")
	if status.IsSuccess() {
		t.Error("Allocatable 為 0 的 Node 應該回傳錯誤狀態，但回傳成功")
	}
}

// 找不到 Node：要回傳錯誤。
func TestScore_UnknownNodeReturnsError(t *testing.T) {
	pl := newTestPlugin(testNodes())
	_, status := pl.Score(context.Background(), framework.NewCycleState(), newPod(optInLabels, "1", "1Gi"), "nope")
	if status.IsSuccess() {
		t.Error("查無此 Node 應該回傳錯誤狀態")
	}
}
