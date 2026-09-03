// score.go
//
// Score extension point：真正的評分公式，決定每個候選 Node 拿到幾分。
// 排程器對每個候選 Node 各別呼叫一次，分數通常落在 [0, 100] 區間，分數越高
// 代表排程器越傾向選這個 Node。
//
// 核心策略：混合 bin-packing(優先塞滿、追求最大利用率)與 spread(優先留餘裕、
// 降低預測失準風險)兩種傾向，用 binPackingWeight 這個可調參數決定比例：
//   weight 越接近 1 → 越偏向 bin-packing
//   weight 越接近 0 → 越偏向 spread
//   weight = 0.5    → 兩者各半
//
// 異質叢集(Node 規格不同)重要說明：
//   predicted_cpu/predicted_memory 是仿照 Google Cluster Trace 的正規化方式，
//   數值是「相對於某台參考機器容量的比例」，不是「相對於任意 Node 的比例」。
//   查證過 Google 官方文件：這個正規化基準的絕對數值(參考機器實際容量)
//   官方並不公開，屬於研究方法上的已知限制。
//   因此評分公式必須先把預測比例換算回一個絕對用量(乘上參考機器容量常數)，
//   再拿這個絕對用量去對照「這一台」候選 Node 的實際容量，才能讓同一個工作負載
//   排到不同規格的 Node 上時，正確反映出佔用比例的差異——這是能否適用異質叢集
//   (私有雲、混合雲、規格不一的 Node)的關鍵。
//   referenceMachine 那組常數目前是佔位數值，需要依你實際訓練資料/部署環境校準，
//   這點值得在論文方法論裡明確討論，是一個真實存在、而非可以忽略的限制。
//
// 有幾個簡化假設，還不是最終版本：
//   1. workload 識別碼：要求 Pod 同時貼上 predictive-scheduler.io/enabled
//      門檻標籤與 "app" 標籤才算參與，兩者缺一都視為不參與，直接 fallback，
//      不像早期版本那樣拿不到 app 標籤就退回用 Pod 名稱(那個做法不可靠，
//      已經拿掉)。之後要接上真正的 workload 身份對應機制時，
//      改 workloadIDFromPod() 這個函式即可。
//   2. 沒有可信預測值時(GetPrediction 回傳 false)，fallback 回 Pod 宣告的
//      靜態 resource.requests，呼應之前定案的「管理者可以選擇不用預測結果」
//      這個原則，即使是因為新鮮度或 enabled 開關被關掉，排程也不會中斷。
//      這條 fallback 路徑本身不受參考機器換算問題影響，因為 Pod 宣告的
//      resource.requests 本來就已經是絕對單位(比如 500m、2Gi)，不需要換算。

package predictivescheduler

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// maxScore 對應 K8s 慣例，Score 回傳值的上限。
const maxScore = 100.0

// referenceMachineMilliCPU / referenceMachineMemoryBytes 是訓練資料正規化基準的
// 絕對容量換算值。Google 官方不公開這個數值，這裡先用佔位常數，
// 之後需要依實際訓練資料/部署環境校準，這是論文方法論要明確討論的限制，
// 不是可以隨意假設的細節。
const (
	referenceMachineMilliCPU    = 4000                    // 佔位值，待校準
	referenceMachineMemoryBytes = 16 * 1024 * 1024 * 1024 // 佔位值(16Gi)，待校準
)

// optInLabelKey / optInLabelValue 是這個專案專屬的門檻標籤。
// 只有明確貼上這個 label 的 Pod,才會真的嘗試查詢預測值,
// 避免叢集裡其他跟這個專案無關、但剛好也有 "app" label 的既有 Pod
// 被意外捲入預測邏輯。key 刻意加上網域前綴,避免跟其他工具的 label 撞名。
const optInLabelKey = "predictive-scheduler.io/enabled"
const optInLabelValue = "true"

// podOptedIn 檢查這個 Pod 是不是明確同意加入預測機制。
func podOptedIn(pod *v1.Pod) bool {
	value, ok := pod.Labels[optInLabelKey]
	return ok && value == optInLabelValue
}

// workloadIDFromPod 同時檢查「有沒有資格參與」跟「識別碼是什麼」這兩件事：
// 沒有 opt-in、或沒有 app label,都視為不參與預測機制。
// 回傳 false 時,第一個字串不是空值,而是明確寫出不參與的原因,
// 方便之後要記錄 log 或除錯時,不用另外再判斷一次是哪種情況造成的。
func workloadIDFromPod(pod *v1.Pod) (string, bool) {
	if !podOptedIn(pod) {
		return "未貼上 predictive-scheduler.io/enabled 標籤，不參與預測", false
	}
	appLabel, ok := pod.Labels["app"]
	if !ok {
		return "已同意加入預測機制，但缺少 app 標籤，無法識別身份", false
	}
	return appLabel, true
}

// staticResourceFraction 是 fallback 用的邏輯：從 Pod 宣告的靜態
// resource.requests 算出「佔這個 Node 容量的比例」。
// 這條路徑不需要參考機器換算，因為 resource.requests 本來就是絕對單位。
func staticResourceFraction(pod *v1.Pod, nodeInfo *framework.NodeInfo) (cpuFraction, memFraction float64) {
	var requestedCPU, requestedMemory int64
	for _, container := range pod.Spec.Containers {
		requestedCPU += container.Resources.Requests.Cpu().MilliValue()
		requestedMemory += container.Resources.Requests.Memory().Value()
	}

	allocatableCPU := nodeInfo.Allocatable.MilliCPU
	allocatableMemory := nodeInfo.Allocatable.Memory

	if allocatableCPU > 0 {
		cpuFraction = float64(requestedCPU) / float64(allocatableCPU)
	}
	if allocatableMemory > 0 {
		memFraction = float64(requestedMemory) / float64(allocatableMemory)
	}
	return cpuFraction, memFraction
}

// predictedResourceFraction 把「相對於參考機器的預測比例」，
// 換算成「相對於這一台候選 Node 的比例」——這是能否正確支援異質叢集的關鍵函式。
// 同一個絕對需求量，排到大 Node 上算出來的比例會變小，排到小 Node 上比例會變大，
// 而不是不管 Node 規格如何，都直接沿用同一個比例。
func predictedResourceFraction(entry *PredictionEntry, nodeInfo *framework.NodeInfo) (cpuFraction, memFraction float64) {
	absoluteCPU := entry.PredictedCPU * float64(referenceMachineMilliCPU)
	absoluteMemory := entry.PredictedMemory * float64(referenceMachineMemoryBytes)

	if nodeInfo.Allocatable.MilliCPU > 0 {
		cpuFraction = absoluteCPU / float64(nodeInfo.Allocatable.MilliCPU)
	}
	if nodeInfo.Allocatable.Memory > 0 {
		memFraction = absoluteMemory / float64(nodeInfo.Allocatable.Memory)
	}
	return cpuFraction, memFraction
}

// blendedUtilizationScore 是核心公式：給定「這個 Pod 排進去之後，Node 的預期使用率(0~1)」
// 跟「理想的目標使用率」，算出離目標越近分數越高的結果。
// targetUtilization 越接近 1 → 越偏向 bin-packing(喜歡塞滿)
// targetUtilization 越接近 0 → 越偏向 spread(喜歡留餘裕)
// targetUtilization = 0.5   → 偏好使用率適中的 Node,不是「不管使用率是多少都一樣」
func blendedUtilizationScore(utilizationAfter, targetUtilization float64) float64 {
	if utilizationAfter > 1 {
		utilizationAfter = 1 // 超過 Node 容量,視同滿載,不要讓分數算出負數或超過範圍
	}

	distance := utilizationAfter - targetUtilization
	if distance < 0 {
		distance = -distance
	}

	// normalizer 取目標值離 0 或 1 哪個邊界比較遠,確保分數落在 [0, 100] 範圍內，
	// 且在 targetUtilization=1 或 0 這兩個極端時，會分別化簡回最初單純的
	// bin-packing ->(utilizationAfter*100) 或 spread ->((1-utilizationAfter)*100) 的算法。
	normalizer := targetUtilization
	if (1 - targetUtilization) > normalizer {
		normalizer = 1 - targetUtilization
	}

	return maxScore * (1 - distance/normalizer)
}

// Score 是這個 plugin 真正對外評分的方法，排程器對每個候選 Node 各別呼叫一次。
func (pl *PredictiveScheduler) Score(
	ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	nodeName string,
) (int64, *framework.Status) {
	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.AsStatus(fmt.Errorf("取得 Node %s 資訊失敗: %w", nodeName, err))
	}

	// 正常情況下,Filter 階段應該已經排除可分配資源異常的 Node,
	// Score() 理論上不會被呼叫在這種 Node 上。但這裡仍然明確防呆,
	// 避免萬一 Allocatable 是 0(Node 故障、尚未就緒等異常狀態),
	// 導致後面的除法算出 NaN,安靜地汙染整個評分結果而不易被發現。
	if nodeInfo.Allocatable.MilliCPU <= 0 || nodeInfo.Allocatable.Memory <= 0 {
		return 0, framework.AsStatus(fmt.Errorf(
			"node %s 可分配資源異常(CPU=%d, Memory=%d),可能故障或尚未就緒",
			nodeName, nodeInfo.Allocatable.MilliCPU, nodeInfo.Allocatable.Memory,
		))
	}

	var cpuFraction, memFraction float64

	payload, err := getPredictionsFromState(state)
	if err != nil {
		// CycleState 裡沒有資料，代表 PreScore 那階段就已經出過問題，
		// 這裡不讓排程失敗，直接 fallback 回靜態值。
		cpuFraction, memFraction = staticResourceFraction(pod, nodeInfo)
	} else {
		workloadIDOrReason, participating := workloadIDFromPod(pod)
		if !participating {
			// 這裡的 workloadIDOrReason 裝的不是識別碼,而是不參與的原因,
			// 用 V(4) 這個較低的詳細等級記錄,避免每次排程都洗版 log。
			klog.V(4).InfoS("Pod 不參與預測機制,fallback 回靜態值",
				"pod", klog.KObj(pod), "node", nodeName, "reason", workloadIDOrReason)
			cpuFraction, memFraction = staticResourceFraction(pod, nodeInfo)
		} else {
			entry, ok := GetPrediction(payload, workloadIDOrReason)
			if ok {
				cpuFraction, memFraction = predictedResourceFraction(entry, nodeInfo)
			} else {
				// enabled=false、查無此 workload、或資料太舊,都會讓 ok 是 false,
				// 三種情況統一 fallback 回靜態值,不特別區分處理。
				cpuFraction, memFraction = staticResourceFraction(pod, nodeInfo)
			}
		}
	}

	currentCPUFraction := float64(nodeInfo.Requested.MilliCPU) / float64(nodeInfo.Allocatable.MilliCPU)
	currentMemFraction := float64(nodeInfo.Requested.Memory) / float64(nodeInfo.Allocatable.Memory)

	cpuScore := blendedUtilizationScore(currentCPUFraction+cpuFraction, pl.targetUtilization)
	memScore := blendedUtilizationScore(currentMemFraction+memFraction, pl.targetUtilization)

	// CPU 跟 Memory 兩個分數先簡單取平均,合併成一個總分。
	// 之後如果想讓其中一種資源的影響力更大,這裡可以改成加權平均。
	finalScore := (cpuScore + memScore) / 2

	return int64(finalScore), nil
}
