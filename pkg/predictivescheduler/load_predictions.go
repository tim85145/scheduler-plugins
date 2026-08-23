// load_predictions.go
//
// 對應 Python 端 write_configmap.py 寫出的 ConfigMap，讀取、解析、
// 提供給 Score plugin 在 PreScore 階段使用。
//
// 設計風格對照 dinozavyr/scheduler-plugins 的 loadLatencyData()：
//   - 用 ClientSet().CoreV1().ConfigMaps().List() + LabelSelector 撈取，不用 Get()
//   - 每次 PreScore 都重新讀一次，不做快取，確保拿到的是最新寫入的內容
//
// 這份檔案只負責「讀取 + 判斷這筆預測能不能用」，不涉及排程分數怎麼算，
// 分數計算的邏輯(bin packing 或留餘裕)是下一步要另外設計的部分。

package predictivescheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	predictionsNamespace = "kube-system"
	predictionsLabel     = "app=predictive-scheduler"
	predictionsDataKey   = "predictions.json"
)

// PredictionEntry 對應 JSON 裡 predictions 底下，單一 workload 的預測結果。
// 之後要擴充新資源類型(例如 GPU)，這裡加一個新欄位、對應 JSON 多一個
// predicted_gpu key 即可，不用改動其他任何地方。
type PredictionEntry struct {
	PredictedCPU    float64 `json:"predicted_cpu"`
	PredictedMemory float64 `json:"predicted_memory"`
	UpdatedAt       string  `json:"updated_at"`
}

// PredictionsPayload 對應整包 JSON 的最外層結構。
type PredictionsPayload struct {
	Enabled             bool                       `json:"enabled"`
	MaxStalenessSeconds int                        `json:"max_staleness_seconds"`
	Predictions         map[string]PredictionEntry `json:"predictions"`
}

// loadPredictions 對照 loadLatencyData()：用 label selector 撈 ConfigMap，
// 解析 data["predictions.json"] 這個字串,還原成 PredictionsPayload。
func loadPredictions(handle framework.Handle) (*PredictionsPayload, error) {
	configMaps, err := handle.ClientSet().CoreV1().ConfigMaps(predictionsNamespace).List(
		context.TODO(),
		metav1.ListOptions{LabelSelector: predictionsLabel},
	)
	if err != nil {
		return nil, fmt.Errorf("無法列出 predictions ConfigMap: %w", err)
	}
	if len(configMaps.Items) == 0 {
		// 找不到 ConfigMap，代表預測機制還沒被設定，視同關閉，不當作錯誤中斷排程。
		return &PredictionsPayload{Enabled: false}, nil
	}

	raw, ok := configMaps.Items[0].Data[predictionsDataKey] //理論上 configMaps 符合 Label 也就只會有一筆，所以直接抓第一筆 .Item[0]
	if !ok {
		return nil, fmt.Errorf("ConfigMap 裡找不到 %s 這個 key", predictionsDataKey)
	}

	var payload PredictionsPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("解析 predictions JSON 失敗: %w", err)
	}
	return &payload, nil
}

// isFresh 依照 max_staleness_seconds 的規則判斷這筆預測還新不新鮮：
//
//	負數 → 不檢查，永遠視為新鮮
//	0    → 零容忍，幾乎必然判定為過期
//	正數 → 落後秒數在門檻內才算新鮮
func isFresh(entry PredictionEntry, maxStalenessSeconds int) bool {
	// 負數
	if maxStalenessSeconds < 0 {
		return true
	}

	updatedAt, err := time.Parse(time.RFC3339, entry.UpdatedAt)
	if err != nil {
		// updated_at 格式壞掉，保守起見視為不新鮮，不要用一筆時間格式有問題的資料。
		return false
	}

	// 正數或零
	staleness := time.Since(updatedAt) // 更新至今過了多久(秒)
	return staleness <= time.Duration(maxStalenessSeconds)*time.Second
}

// GetPrediction 是 Score plugin 實際會呼叫的入口：
// 給一個 workload 的識別字串，回傳「能不能用、能用的話預測值是多少」。
// enabled 開關、新鮮度檢查、workload 存不存在，三個判斷都封裝在這裡，
// PreScore 那邊呼叫的人不用重複寫這些判斷邏輯。
func GetPrediction(payload *PredictionsPayload, workloadID string) (*PredictionEntry, bool) {
	if payload == nil || !payload.Enabled {
		return nil, false
	}

	entry, ok := payload.Predictions[workloadID]
	if !ok {
		return nil, false
	}

	if !isFresh(entry, payload.MaxStalenessSeconds) {
		return nil, false
	}

	return &entry, true
}
