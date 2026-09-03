// prescore.go
//
// 把 loadPredictions() 撈到的預測資料，接進 K8s Scheduling Framework 的
// PreScore extension point，存進 CycleState，讓同一輪排程週期裡的 Score
// 階段可以直接讀取，不用針對每個候選 Node 各自重打一次 API。
//
// 這份檔案只負責「資料流通不通」，還沒有真正的評分公式——
// 評分公式(怎麼把預測值轉換成 Node 分數)是下一步要做的事。
//
// 注意：這裡的 PreScorePlugin 介面簽名、CycleState 的用法，是照公開的
// Kubernetes Scheduling Framework API 寫的，還沒有實際在你的 go.mod 版本
// 上編譯測試過，之後你 go build 時如果簽名對不上，多半是你這份
// k8s.io/kubernetes 依賴的版本跟這裡寫的不完全一致，貼錯誤訊息給我，
// 我們一起對照調整。

package predictivescheduler

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Name 是這個 plugin 在 K8s Scheduling Framework 裡的識別名稱，
// 排程器設定檔(KubeSchedulerConfiguration)要啟用/調整權重時，就是用這個字串指名。
const Name = "PredictiveScheduler"

// predictionStateKey 是這個 plugin 在 CycleState 裡，用來存取資料的專屬 key。
// 取名習慣上會加上 package 名稱當前綴，避免跟其他 plugin 存進 CycleState 的 key 撞名。
const predictionStateKey = "predictivescheduler.PredictionsPayload"

// PredictiveScheduler 是這個 plugin 本身的結構，包含方法 Name 以及 PreScore，
// PreScore/Score 這些方法都是靠 framework.Handle 連到 K8s API。
// targetUtilization 是評分公式的核心參數：Node 排進去之後，理想的使用率大概落在哪裡，
// 目前先給一個合理的預設值(0.5)，之後要讓管理者能調整的話，
// 改 New() 讀取排程器設定檔傳進來的參數即可，Score() 跟評分公式本身不用改。
type PredictiveScheduler struct {
	handle            framework.Handle
	targetUtilization float64
}

// New 是 Scheduling Framework 用來建立這個 plugin 實例的建構子，
// 排程器啟動時會呼叫這個函式，把 handle 交給你，之後整個 plugin 生命週期都用同一個 handle。
func New(_ context.Context, _ interface{}, handle framework.Handle) (framework.Plugin, error) {
	return &PredictiveScheduler{
		handle:            handle,
		targetUtilization: 0.5, // 預設值,待之後排程模擬驗證後再校準
	}, nil
}

// Name 是 framework.Plugin 這個介面要求一定要實作的方法，回傳上面定義的 Name 常數。
func (pl *PredictiveScheduler) Name() string {
	return Name
}

// predictionStateData 是要存進 CycleState 的資料的包裝型別。
// CycleState.Write() 要求存進去的資料必須實作 framework.StateData 這個介面，
// 而這個介面只要求一個方法：Clone()。
type predictionStateData struct {
	payload *PredictionsPayload
}

// Clone 是 framework.StateData 介面要求的方法。
// 這裡的預測資料在同一輪排程週期內是唯讀的(不會被 Score 階段修改)，
// 所以直接回傳同一份參照就夠了，不用真的複製一份新的出來。
func (d *predictionStateData) Clone() framework.StateData {
	return d
}

// PreScore 是這個 plugin 真正接上 Scheduling Framework 的地方。
// 每一輪排程只會被呼叫一次，在這裡把 loadPredictions() 撈到的資料，
// 寫進 CycleState，供接下來的 Score 階段(針對每個候選 Node 各別呼叫)取用。
func (pl *PredictiveScheduler) PreScore(
	ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	nodes []*v1.Node,
) *framework.Status {
	payload, err := loadPredictions(pl.handle)
	if err != nil {
		return framework.AsStatus(fmt.Errorf("PreScore 載入預測資料失敗: %w", err))
	}

	state.Write(predictionStateKey, &predictionStateData{payload: payload})
	return nil
}

// getPredictionsFromState 是給 Score 階段用的輔助函式，
// 從 CycleState 裡把 PreScore 存進去的資料讀回來。
// 之後寫評分公式時，Score() 一開始就會呼叫這個函式拿到 payload，
// 再對每個候選 Node，各自呼叫 GetPrediction() 判斷能不能用、預測值是多少。
func getPredictionsFromState(state *framework.CycleState) (*PredictionsPayload, error) {
	data, err := state.Read(predictionStateKey)
	if err != nil {
		return nil, fmt.Errorf("CycleState 裡沒有找到 predictions 資料: %w", err)
	}

	stateData, ok := data.(*predictionStateData)
	if !ok {
		return nil, fmt.Errorf("CycleState 裡的資料型別不是預期的 predictionStateData")
	}

	return stateData.payload, nil
}
