// load_predictions_test.go
//
// 針對 load_predictions.go 裡不需要真正連上 K8s API 的純邏輯函式寫測試：
// isFresh()、GetPrediction()。
// loadPredictions() 本身需要真正呼叫 API，這裡不測，留給實際在 kind 叢集上驗證。

package predictivescheduler

import (
	"testing"
	"time"
)

// TestIsFresh 用表格驅動測試(table-driven test)的方式，一次驗證多種情境。
// 這是 Go 很常見的測試寫法：把每種情境整理成一筆資料，用迴圈跑過所有情境，
// 不用為每個情境各自寫一個函式，概念上有點像 Python pytest 的 @pytest.mark.parametrize。
func TestIsFresh(t *testing.T) {
	now := time.Now().UTC()

	cases := []struct {
		name                string
		updatedAt           string
		maxStalenessSeconds int
		want                bool
	}{
		{
			name:                "負數門檻，不管多舊都信任",
			updatedAt:           "2000-01-01T00:00:00Z", // 故意用很舊的時間
			maxStalenessSeconds: -1,
			want:                true,
		},
		{
			name:                "正數門檻，剛更新過，在門檻內",
			updatedAt:           now.Add(-10 * time.Second).Format(time.RFC3339),
			maxStalenessSeconds: 600,
			want:                true,
		},
		{
			name:                "正數門檻，太舊了，超過門檻",
			updatedAt:           now.Add(-1 * time.Hour).Format(time.RFC3339),
			maxStalenessSeconds: 600,
			want:                false,
		},
		{
			name:                "零容忍門檻，即使剛更新過，仍視為過期",
			updatedAt:           now.Add(-5 * time.Second).Format(time.RFC3339),
			maxStalenessSeconds: 0,
			want:                false,
		},
		{
			name:                "updated_at 格式錯誤，保守判定為不新鮮",
			updatedAt:           "這不是一個合法的時間格式",
			maxStalenessSeconds: 600,
			want:                false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			entry := PredictionEntry{UpdatedAt: c.updatedAt}
			got := isFresh(entry, c.maxStalenessSeconds)
			if got != c.want {
				t.Errorf("isFresh(%+v, %d) = %v, 預期 %v", entry, c.maxStalenessSeconds, got, c.want)
			}
		})
	}
}

// TestGetPrediction 驗證 enabled 開關、workload 存在與否、新鮮度，三層判斷是否正確。
func TestGetPrediction(t *testing.T) {
	freshTime := time.Now().UTC().Add(-10 * time.Second).Format(time.RFC3339)
	staleTime := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)

	freshPayload := &PredictionsPayload{
		Enabled:             true,
		MaxStalenessSeconds: 600,
		Predictions: map[string]PredictionEntry{
			"workload-a": {PredictedCPU: 0.3, PredictedMemory: 0.1, UpdatedAt: freshTime},
			"workload-b": {PredictedCPU: 0.5, PredictedMemory: 0.2, UpdatedAt: staleTime},
		},
	}

	disabledPayload := &PredictionsPayload{
		Enabled: false,
		Predictions: map[string]PredictionEntry{
			"workload-a": {PredictedCPU: 0.3, PredictedMemory: 0.1, UpdatedAt: freshTime},
		},
	}

	cases := []struct {
		name       string
		payload    *PredictionsPayload
		workloadID string
		wantOK     bool
	}{
		{"payload 是 nil", nil, "workload-a", false},
		{"enabled=false，整包視為不可用", disabledPayload, "workload-a", false},
		{"workload 存在且新鮮", freshPayload, "workload-a", true},
		{"workload 存在但已過期", freshPayload, "workload-b", false},
		{"查無此 workload", freshPayload, "workload-不存在", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			entry, ok := GetPrediction(c.payload, c.workloadID)
			if ok != c.wantOK {
				t.Errorf("GetPrediction(...) ok = %v, 預期 %v", ok, c.wantOK)
			}
			if ok && entry == nil {
				t.Errorf("ok 是 true，但 entry 卻是 nil")
			}
		})
	}
}
