// score_test.go
//
// 針對 score.go 裡不需要真正連上 K8s API 的純邏輯函式寫測試：
// podOptedIn()、workloadIDFromPod()、blendedUtilizationScore()、
// predictedResourceFraction()、staticResourceFraction()。
// Score() 方法本身需要 pl.handle.SnapshotSharedLister()，這裡不測。

package predictivescheduler

import (
	"math"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// newTestNodeInfo 是測試用的輔助函式，建立一個指定 CPU/Memory 容量的 NodeInfo，
// 不用每個測試案例都重複寫一次建立 Node 的樣板程式碼。
func newTestNodeInfo(cpuMilli, memoryBytes int64) *framework.NodeInfo {
	node := &v1.Node{
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(memoryBytes, resource.BinarySI),
			},
		},
	}
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	return nodeInfo
}

// newTestPod 是測試用的輔助函式，建立一個指定 label 跟資源需求的 Pod。
func newTestPod(labels map[string]string, cpuMilli, memoryBytes int64) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Labels: labels},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
							v1.ResourceMemory: *resource.NewQuantity(memoryBytes, resource.BinarySI),
						},
					},
				},
			},
		},
	}
}

func TestPodOptedIn(t *testing.T) {
	cases := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"沒有任何 label", nil, false},
		{"有 label 但沒有門檻 key", map[string]string{"app": "nginx"}, false},
		{"有門檻 key 但值不是 true", map[string]string{optInLabelKey: "false"}, false},
		{"門檻 key 跟值都對", map[string]string{optInLabelKey: "true"}, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pod := newTestPod(c.labels, 0, 0)
			got := podOptedIn(pod)
			if got != c.want {
				t.Errorf("podOptedIn(labels=%v) = %v, 預期 %v", c.labels, got, c.want)
			}
		})
	}
}

func TestWorkloadIDFromPod(t *testing.T) {
	cases := []struct {
		name            string
		labels          map[string]string
		wantParticipate bool
		wantID          string // 只有 wantParticipate 是 true 時才檢查這個值
	}{
		{
			name:            "沒有 opt-in，不參與",
			labels:          map[string]string{"app": "nginx"},
			wantParticipate: false,
		},
		{
			name:            "有 opt-in 但沒有 app label，不參與",
			labels:          map[string]string{optInLabelKey: "true"},
			wantParticipate: false,
		},
		{
			name:            "兩個 label 都有，參與並回傳正確識別碼",
			labels:          map[string]string{"app": "nginx", optInLabelKey: "true"},
			wantParticipate: true,
			wantID:          "nginx",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pod := newTestPod(c.labels, 0, 0)
			id, participate := workloadIDFromPod(pod)
			if participate != c.wantParticipate {
				t.Errorf("participate = %v, 預期 %v", participate, c.wantParticipate)
			}
			if c.wantParticipate && id != c.wantID {
				t.Errorf("id = %q, 預期 %q", id, c.wantID)
			}
			if !c.wantParticipate && id == "" {
				t.Errorf("不參與時,id 應該要是說明原因的字串,不該是空字串")
			}
		})
	}
}

// almostEqual 是浮點數比較用的輔助函式。浮點數運算會有極小的誤差，
// 不能直接用 == 比較，要檢查兩個數字的差距是不是在容許範圍內。
func almostEqual(a, b, tolerance float64) bool {
	return math.Abs(a-b) <= tolerance
}

func TestBlendedUtilizationScore(t *testing.T) {
	const tolerance = 0.01

	cases := []struct {
		name              string
		utilizationAfter  float64
		targetUtilization float64
		want              float64
	}{
		{"目標 0.5，剛好打到目標，滿分", 0.5, 0.5, 100},
		{"目標 0.5，完全沒用到，離目標最遠，0分", 0.0, 0.5, 0},
		{"目標 0.5，完全塞滿，離目標最遠，0分", 1.0, 0.5, 0},
		{"目標 1.0(純 bin-packing)，化簡回 utilization*100", 0.7, 1.0, 70},
		{"目標 0.0(純 spread)，化簡回 (1-utilization)*100", 0.7, 0.0, 30},
		{"utilizationAfter 超過 1，夾回 1 再計算", 1.5, 1.0, 100},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := blendedUtilizationScore(c.utilizationAfter, c.targetUtilization)
			if !almostEqual(got, c.want, tolerance) {
				t.Errorf("blendedUtilizationScore(%v, %v) = %v, 預期約 %v",
					c.utilizationAfter, c.targetUtilization, got, c.want)
			}
		})
	}
}

func TestStaticResourceFraction(t *testing.T) {
	// Node 有 4000 毫核心(4 顆)、16GB 記憶體
	nodeInfo := newTestNodeInfo(4000, 16*1024*1024*1024)
	// Pod 宣告需要 1000 毫核心(1 顆)、4GB 記憶體
	pod := newTestPod(nil, 1000, 4*1024*1024*1024)

	cpuFraction, memFraction := staticResourceFraction(pod, nodeInfo)

	if !almostEqual(cpuFraction, 0.25, 0.001) {
		t.Errorf("cpuFraction = %v, 預期約 0.25(1000/4000)", cpuFraction)
	}
	if !almostEqual(memFraction, 0.25, 0.001) {
		t.Errorf("memFraction = %v, 預期約 0.25(4GB/16GB)", memFraction)
	}
}

func TestPredictedResourceFraction(t *testing.T) {
	entry := &PredictionEntry{PredictedCPU: 0.3, PredictedMemory: 0.2}

	t.Run("排到大 Node，佔用比例應該比較小", func(t *testing.T) {
		bigNode := newTestNodeInfo(referenceMachineMilliCPU*2, referenceMachineMemoryBytes*2)
		cpuFraction, _ := predictedResourceFraction(entry, bigNode)
		// 絕對需求量不變，Node 容量變兩倍，比例應該減半
		if !almostEqual(cpuFraction, 0.15, 0.001) {
			t.Errorf("大 Node 上的 cpuFraction = %v, 預期約 0.15", cpuFraction)
		}
	})

	t.Run("排到小 Node，佔用比例應該比較大", func(t *testing.T) {
		smallNode := newTestNodeInfo(referenceMachineMilliCPU/2, referenceMachineMemoryBytes/2)
		cpuFraction, _ := predictedResourceFraction(entry, smallNode)
		// 絕對需求量不變，Node 容量減半，比例應該變兩倍
		if !almostEqual(cpuFraction, 0.6, 0.001) {
			t.Errorf("小 Node 上的 cpuFraction = %v, 預期約 0.6", cpuFraction)
		}
	})
}
