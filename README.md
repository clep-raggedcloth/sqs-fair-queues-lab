# Amazon SQS Fair Queues 検証環境

Amazon SQS Standard QueueでFair Queuesを利用し、次の2点をAWS上で検証するためのコードです。

1. テナントAのバースト発生後、静かなテナントB・Cのdwell timeが平常値へ戻るまでの時間
2. Lambdaの最大同時実行数を20に制限したとき、Fair Queuesによるquiet tenantの遅延改善を観測できるか

ConsumerはGoのLambda、AWSリソースはTerraform、負荷生成と結果回収はGo CLIで実装しています。ビルドとAWSへのデプロイにはDev Containerを使用できます。

## 最初に押さえるFair Queuesの判定条件

Fair Queuesは、同じ`MessageGroupId`を持つメッセージを同一テナントとして扱います。テナントがnoisy neighborと判定される条件は次のどちらかです。

- Concurrency share経路：テナントが全in-flightの10%を超え、かつ自身のin-flightが30件以上
- Processing-time share経路：直近の総処理時間に占めるテナントの割合が10%を超える

したがって「コンシューマーが30未満ならFair Queuesは動かない」という条件ではありません。正しくは、同一テナントのin-flightが30件未満ならConcurrency share経路の30件条件を満たせない、です。

```mermaid
flowchart TD
    Start["テナントAの負荷を観測"] --> Count{"Aのin-flightが30件以上<br/>かつ全体の10%超か"}
    Count -->|Yes| Noisy["Aをnoisy tenantとして検出可能"]
    Count -->|No| Time{"Aの処理時間シェアが<br/>10%超か"}
    Time -->|Yes| Noisy
    Time -->|No| Normal["現時点ではnoisy判定なし"]
    Noisy --> Prefer["B・Cなどquiet tenantの<br/>メッセージを優先配信"]
```

AWSは分散システムであり、しきい値付近の判定は近似です。また、Processing-time shareの集計期間や内部の判定時刻は公開されていません。この検証ではサービス内部の時刻を直接測るのではなく、B・Cのdwell timeが継続的に改善した時刻を反応時間として扱います。

## シナリオ名

`c100`、`c20`は、SQSイベントソースマッピングに設定するLambdaのMaximum Concurrencyを表します。

| シナリオ | MessageGroupId | Maximum Concurrency | 用途 |
|---|---|---:|---|
| `fair-c100` | テナントID | 100 | 30件条件へ到達できるFair Queues |
| `baseline-c100` | なし | 100 | `fair-c100`の比較対象 |
| `fair-c20` | テナントID | 20 | 少数コンシューマー構成でのFair Queues |
| `baseline-c20` | なし | 20 | `fair-c20`の比較対象 |

キュー自体はすべてStandard Queueです。`fair-*`と`baseline-*`の違いは、実験CLIが送信時に`MessageGroupId`を付けるかどうかです。

## 全体構成

```mermaid
flowchart LR
    Runner["Go experiment CLI<br/>負荷生成・結果回収"]

    subgraph C100["Maximum Concurrency = 100"]
        FC100["fair-c100<br/>Standard Queue<br/>MessageGroupIdあり"] --> FL100["Go Consumer Lambda<br/>BatchSize = 1"]
        BC100["baseline-c100<br/>Standard Queue<br/>MessageGroupIdなし"] --> BL100["Go Consumer Lambda<br/>BatchSize = 1"]
    end

    subgraph C20["Maximum Concurrency = 20"]
        FC20["fair-c20<br/>Standard Queue<br/>MessageGroupIdあり"] --> FL20["Go Consumer Lambda<br/>BatchSize = 1"]
        BC20["baseline-c20<br/>Standard Queue<br/>MessageGroupIdなし"] --> BL20["Go Consumer Lambda<br/>BatchSize = 1"]
    end

    Runner --> FC100
    Runner --> BC100
    Runner --> FC20
    Runner --> BC20

    FL100 --> Logs["CloudWatch Logs<br/>message_started JSON"]
    BL100 --> Logs
    FL20 --> Logs
    BL20 --> Logs
    Logs --> Runner
    Metrics["SQS・Lambda Metrics"] --> Dashboard["CloudWatch Dashboard"]
```

`BatchSize=1`に固定することで、1回のLambda実行が保持するin-flightメッセージを1件にします。Lambdaの同時実行数とSQSのin-flight件数は完全に同一のメトリクスではありませんが、バッチによる倍率を排除できます。

## Dev Container

ホスト側にGoやTerraformを直接インストールする必要はありません。Dev Containerには次の環境が含まれています。この検証は実AWSのSQS、Lambda、CloudWatchを対象とし、ローカルのAWS互換サービスは使用しません。

- Go 1.26
- Terraform
- AWS CLI
- VS CodeのGo、Terraform、AWS Toolkit拡張

```mermaid
flowchart LR
    Host["ホスト環境<br/>Docker・Dev Containers"] --> App["app container<br/>Go 1.26<br/>Terraform<br/>AWS CLI"]
    App --> AWS["実AWS<br/>SQS Fair Queues<br/>Lambda・CloudWatch"]
```

### 起動

VS Codeでは、このリポジトリを開いて`Dev Containers: Reopen in Container`を実行します。初回起動時はコンテナのビルドとGoモジュールのダウンロードが自動実行されます。

コンテナ内のワークスペースは`/workspace`です。

```bash
cd /workspace
go version
terraform version
aws --version
```

## Consumerが記録する値

Consumerは処理を始めた直後に、次のJSONを標準出力へ記録します。

```json
{
  "event_type": "message_started",
  "experiment_id": "reaction-20260704T120000Z",
  "scenario": "fair-c100",
  "tenant": "B",
  "phase": "probe",
  "sequence": 12,
  "sqs_sent_ms": 1783140000000,
  "handler_started_ms": 1783140000234,
  "dwell_ms": 234,
  "work_ms": 2000
}
```

`dwell_ms`は、SQSが付与した`SentTimestamp`からLambdaハンドラー開始までの時間です。ログ出力後は`work_ms`だけ`time.Sleep`し、メッセージを意図的にin-flightへ保持します。

## 検証1：バースト後の反応時間

`fair-c100`と`baseline-c100`へ同じ負荷を送ります。

```mermaid
sequenceDiagram
    participant R as Experiment CLI
    participant A as Tenant A
    participant Q as fair-c100 / baseline-c100
    participant BC as Tenant B・C probes
    participant L as Consumer Lambda

    R->>Q: 20テナントの均等なウォームアップ負荷
    Q->>L: 最大100並列で処理
    R->>R: キューが空になるまで待機
    loop 20秒
        BC->>Q: B・Cのbaselineを送信
    end
    R->>R: baselineが処理済みになるまで待機
    A->>Q: Aの最初のバッチを送信
    Q-->>R: 最初のバッチを受理（t=0）
    R->>A: 残りのバーストを継続
    loop 100ms間隔
        BC->>Q: B・Cを交互に送信
    end
    Q->>L: メッセージを配信
    L->>L: dwell timeを記録して2秒処理
    Note over Q,L: fair-c100ではAがnoisyと判定された後にB・Cを優先
```

見る値は次のとおりです。

- バースト開始後のB・Cの`dwell_ms`時系列
- バースト前baselineから算出した平常範囲へ、B・Cの両方が戻るまでの時間
- `fair-c100`と`baseline-c100`のp50・p95
- Aの`handler-active estimate`が30件以上となったか、およびその継続時間
- Aの`handler-active estimate / ApproximateNumberOfMessagesNotVisible` proxyが10%を超えたか
- Aのバックログが残っているのにB・Cが先に処理されたか
- `ApproximateNumberOfNoisyGroups`が0より大きくなったか

`BatchSize=1`なので、collectorは各メッセージの半開区間`[handler_started_ms, handler_started_ms + work_ms)`を重ね合わせ、テナント別の同時処理数を再構成します。この区間はメッセージが確実に処理中である部分だけを数えるため、SQS in-flight件数の下限推定です。ハンドラー開始前の受信時間と終了後の削除遅延は含みません。テナント別handler-active件数同士の比率は`handler_active_share`として出力しますが、除外区間がテナントごとに異なり得るため、SQS in-flightシェアの下限とは扱いません。

10%条件との整合性確認には、1秒ごとの`A handler-active / ApproximateNumberOfMessagesNotVisible`を`count_share_proxy`として別途出力します。分子はハンドラー開始前のin-flightメッセージを含まない下限推定で、分母はSQSのApproximate属性です。そのため`criteria_proxy_met=true`は観測値がConcurrency share条件と整合したことを示す強い補助証拠として扱いますが、AWS内部判定の直接的な証明ではありません。`false`の場合も、AWS内部で条件を満たさなかったとは判断しません。

30件以上側の代表証拠には`fair-c100`を使用します。検証2の`c20`はLambda同時呼び出し数自体が20以下であるため、Concurrency share経路よりも、少数コンシューマー構成でquiet tenantの遅延改善を実際に観測できるかをfair/baselineの差から確認します。

CloudWatchのSQSメトリクスは1分粒度なので、秒単位の反応時間にはConsumerログを使います。実験CLIはこれとは別に、実験中の`GetQueueAttributes`をデフォルト1秒間隔で保存します。属性値自体はApproximateであるため、厳密な上限の証明ではなく観測された短時間ピークの証拠として扱います。取得失敗は0へ変換せず`status=error`の欠測行として保存し、次のtickで再試行します。5回連続で失敗した場合もmanifestと部分CSVを保存してからエラー終了します。`ApproximateNumberOfNoisyGroups`も補助証拠です。

実行例：

```bash
build/experiment run-reaction \
  --config build/experiment-config.json \
  --burst 30000 \
  --probes 6000 \
  --probe-interval 100ms \
  --baseline-duration 20s \
  --work-ms 2000
```

1回の結果だけで断定せず、キューを空にした状態から5〜10回実行し、反応時間の中央値とp95を比較してください。各実行で`collect`まで完了した後、次の例で検証1の`recovery-estimate.json`をシナリオ別に集計できます。

```bash
jq -s '
  def pct($p): sort | .[(((length - 1) * $p) | floor)];

  add
  | group_by(.scenario)
  | map(
      . as $runs
      | [$runs[]
          | select(.recovery_observed == true and .recovery_latency_ms != null)
          | .recovery_latency_ms] as $values
      | {
          scenario: $runs[0].scenario,
          total_runs: ($runs | length),
          recovered_runs: ($values | length),
          unrecovered_runs: (($runs | length) - ($values | length)),
          median_ms: (if ($values | length) > 0 then ($values | pct(0.50)) else null end),
          p95_ms: (if ($values | length) > 0 then ($values | pct(0.95)) else null end),
          min_ms: (if ($values | length) > 0 then ($values | min) else null end),
          max_ms: (if ($values | length) > 0 then ($values | max) else null end)
        }
    )
' results/reaction-*/recovery-estimate.json
```

中央値・p95は回復を観測できた試行だけから計算されるため、`total_runs`、`recovered_runs`、`unrecovered_runs`を必ず併記してください。回復しなかった試行を無視して中央値だけを比較すると、結果を良い側へ偏らせます。

## 検証2：最大同時実行数20でのFair Queues効果

`fair-c20`と`baseline-c20`を同時に使用します。`BatchSize=1`かつLambda Maximum Concurrencyを20に設定し、同じA/B/C負荷に対するquiet tenantのdwell timeを比較します。`fair-c20`だけでは、B・Cの遅延がFair Queuesによって改善したのか、通常のLambda/SQSの処理順によるものか判断できないため、`baseline-c20`を必須の比較対象とします。

Maximum Concurrency 20ではLambdaハンドラー内で同時処理中となるメッセージは20件以下です。ただし、イベントソースマッピングがハンドラー開始前に受信したメッセージもSQS in-flightへ含まれるため、テナントAのin-flightが必ず30件未満になるとは保証できません。本試験はConcurrency share経路の不成立を証明するものではなく、少数コンシューマー構成でfair/baseline間の改善量と改善開始時間を確認する試験です。Processing-time share経路でnoisy tenantと判定される可能性も残ります。

検証2では、測定中のB/Cプローブとは独立した`--baseline-interval`を使用します。既定では500ms間隔で100秒間、合計200件（B/C各約100件）のbaselineを取得します。これによりc20をbaseline負荷だけで飽和させず、回復しきい値に使用するbaseline p95の標本数も確保します。

検証1では平準化の開始が約200秒後、継続的な回復がさらに後に観測されたため、検証2はshort/longへ分けず、600秒の単一観測として実行します。B/Cプローブを観測期間中継続して送り、同じ試行を次の時間帯に分けて比較します。

```text
0〜200秒     early
200〜400秒   reaction
400〜600秒   stabilization
```

実行例：

```bash
build/experiment run-low \
  --config build/experiment-config.json
```

既定値は`--probe-interval 500ms --probes 1200`で、観測時間は600秒です。`work-ms=2000`の場合、B・Cが定常的に使用するスロットは合計約4となり、残りをAが使用できます。baselineの既定値は`--baseline-interval 500ms --baseline-duration 100s`です。

確認する内容：

- `fair-c20`と`baseline-c20`の両方が実行結果に含まれる
- Lambda最大同時実行数が20以下である
- 1秒間隔の`GetQueueAttributes`でSQSのApproximate in-flightを確認できる
- Aの`handler-active estimate`が30未満で推移したか
- 0〜200秒、200〜400秒、400〜600秒それぞれのB・Cのp50・p95
- `fair-c20`の改善が現れた200秒区間と、`recovery-estimate.json`による継続回復時刻

観測した`ApproximateNumberOfMessagesNotVisible`が30件以上となった試行では、Concurrency share経路を除外できません。30件未満で推移した場合も値はApproximateであるため、経路不成立の直接証明ではなく補助証拠として扱います。

時間帯別の結果は`observation-window-summary.json`へ出力します。各行はプローブの送信予定時刻（`sequence × probe-interval`）を基準に200秒ごとへ分類し、その時間帯に割り当てられたB/Cの最終的なdwell timeを集計します。実際の`SentTimestamp`は送信遅延やtick落ちで予定から後ろへずれ得るため、境界付近の1件が隣の時間帯へ誤分類されて窓全体が`incomplete`扱いになることを避ける目的で、予定時刻を分類に使用します。`expected_count`、`observed_count`、`completion_rate`も保存し、ログが不足した時間帯は`status=incomplete`としてp50・p95を確定結果に使用しません。差がなかった場合もFair Queuesが無効であるとは断定せず、「この負荷・600秒の観測期間ではfair/baseline間の改善を確認できなかった」と判断します。

### Fair Queues条件の観測証拠

`fair-queue-condition-evidence.json`へ、Concurrency share経路とProcessing-time share経路を分けて出力します。

- Concurrency share：Aのhandler-activeが30件以上、かつ`A handler-active / ApproximateNumberOfMessagesNotVisible`が10%を超えた観測点を記録
- Processing-time share：Consumerログから直近60秒の処理時間を再構成し、Aの処理時間シェアが10%を超えた観測点を記録

各経路について、条件を観測できたか、最初に観測したバースト開始からの経過時間、ピーク値、しきい値を保存します。条件判定は`MessageGroupId`を付与する`fair-*`だけに適用し、`baseline-*`は`not_applicable_without_message_group_id`とします。ただし、どちらの経路もAWS内部状態の直接測定ではありません。Concurrency shareは分子がhandler-activeによる下限推定、分母がSQSのApproximate属性です。Processing-time shareのAWS内部集計窓は非公開なので、60秒窓は本検証で定義したproxyです。また、バースト直後の60秒ルックバック窓には事前baselineフェーズのB/C処理が含まれるため、Processing-time shareの`first_observed_elapsed_ms`はシェアが希釈された分だけ数秒〜十数秒遅れて記録されます。

同じ条件を5〜10回実行し、`fair-c20`と`baseline-c20`の時間帯別p50・p95、回復時間、SQS in-flight、`ApproximateNumberOfNoisyGroups`を比較してください。

## 必要なもの

- Docker
- Dev Container対応エディタ、またはDocker Compose
- 実AWS検証時に利用できるAWS認証情報
- Lambda同時実行クォータ

`reserve_concurrency=true`の場合、各関数にイベントソースの上限より5多いReserved Concurrencyを設定します。必要な合計はTerraformが計算するため、適用後に次のコマンドで確認してください。

```bash
terraform -chdir=terraform output reserved_concurrency_total
```

アカウントの同時実行クォータには、この出力値に加えてLambdaが要求する未予約同時実行枠も必要です。クォータが不足する場合は`reserve_concurrency=false`で構築できますが、他ワークロードの影響を受けやすくなります。

実験CLIを実行するIAM主体には、対象キューへの以下の権限が必要です。

- `sqs:SendMessage`
- `sqs:GetQueueAttributes`
- `sqs:PurgeQueue`
- CloudWatch Logsの`StartQuery`と`GetQueryResults`
- CloudWatchの`GetMetricData`

## コンテナ内でのテストとビルド

```bash
cd /workspace
make tidy
make test
make build
```

生成物：

```text
build/
├── experiment
└── lambda/
    ├── bootstrap
    └── consumer.zip
```

## 実AWSへのデプロイ

Dev Containerはホストの`~/.aws`を自動ではマウントしません。コンテナ内でAWS SSOプロファイルを設定し、利用するAWSアカウントを明示的に確認してください。

```bash
export AWS_REGION=ap-northeast-1

aws configure sso --profile fairqueue-verification
export AWS_PROFILE=fairqueue-verification
aws sso login --profile "$AWS_PROFILE"
aws sts get-caller-identity
```

この操作でプロファイルはコンテナ内の`/root/.aws`へ作成されます。コンテナを作り直した場合は、必要に応じて再設定してください。

認証確認後、コンテナ内でTerraformを実行します。

```bash
make build

terraform -chdir=terraform init
terraform -chdir=terraform plan
terraform -chdir=terraform apply

terraform -chdir=terraform output -json experiment_config \
  > build/experiment-config.json
```

設定を変更する場合：

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
```

## 結果の回収

実験終了時に、CLIがmanifestのパスを表示します。

```bash
build/experiment collect \
  --config build/experiment-config.json \
  --manifest results/<experiment-id>/manifest.json
```

次のファイルが生成されます。

```text
results/<experiment-id>/
├── manifest.json
├── queue-depth-samples.csv
├── queue-depth-aligned.csv
├── concurrency-share-proxy.csv
├── fair-queue-condition-evidence.json
├── events.csv
├── handler-active-estimate.csv
├── handler-active-summary.json
├── summary.json
├── observation-summary.json
├── observation-window-summary.json
├── recovery-estimate.json
└── metrics.json
```

- `events.csv`：メッセージ単位の受信開始時刻とdwell time。LT用の時系列グラフの入力
- `handler-active-estimate.csv`：`handler_started_ms`から`work_ms`の終了までを重ね合わせた、シナリオ・テナント別の同時処理数と`handler_active_share`の時系列。`_all`は全テナント合計
- `handler-active-summary.json`：テナント別の推定同時処理数・handler-active比率の最大値と継続時間。件数は確実に処理中だった区間による下限推定だが、比率はSQS in-flightシェアの下限ではない
- `queue-depth-samples.csv`：バースト送信直前からキューが空になるまで、SQS `GetQueueAttributes`の絶対時刻・visible・not-visible・取得状態をデフォルト1秒間隔で保存。値はApproximateで、取得失敗時の数値欄は空欄。manifestにも実行状態とサンプル総数・エラー数を保存
- `queue-depth-aligned.csv`：collectorが最初のAメッセージのSQS `SentTimestamp`を共通のt=0としてサンプルを補正。ログがない場合のみmanifest時刻へfallbackし、`elapsed_source`へ基準を保存
- `concurrency-share-proxy.csv`：同時刻のA handler-active件数とApproximate not-visibleからcount/share proxyを計算。30件条件、10% proxy、両方の成立フラグを保存。`criteria_proxy_met=true`はConcurrency share条件と整合する補助証拠だが、`false`は内部条件が不成立だったことを意味しない
- `summary.json`：キューが空になるまでの全期間について、シナリオ・テナント・phaseごとの件数、p50、p95、最大dwell time
- `observation-summary.json`：バースト開始からプローブ送信終了までに送信されたメッセージを集計。処理開始が観測窓より後になった遅延メッセージも含む
- `observation-window-summary.json`：B・Cプローブを送信予定時刻（`sequence × probe-interval`）に基づいて200秒ごとの時間帯へ分け、シナリオ・テナント別の期待件数、観測件数、回収率、p50、p95、最大dwell timeを保存。期待件数未満は`status=incomplete`とする
- `fair-queue-condition-evidence.json`：件数経路の30件・10%条件と、直近60秒の処理時間シェアproxyによる10%条件について、成立観測、最初の観測時刻、ピーク値をシナリオ別に保存
- `recovery-estimate.json`：最初のAメッセージのSQS `SentTimestamp`をt=0とし、B・Cそれぞれについてbaseline p95から平常範囲を算出。各テナントの直近10件中8件以上が範囲内となり、B・Cの両方が回復した時刻を保存する
- `metrics.json`：SQSのnoisy group・in-flight・quiet group in-flightとLambda同時実行数を1分粒度のMaximumで保存。各ポイントに`phase`（`baseline`、`measurement`、`drain`、`boundary_overlap`）とバースト開始からの`elapsed_ms`を付与する。秒単位の反応時間ではなく補助証拠として使用する

CloudWatchの1分バケットがバースト開始または観測終了をまたぐ場合は`phase=boundary_overlap`とします。Aのバーストによる変化を確認するときは`measurement`のポイントを使用し、`baseline`や`boundary_overlap`のMaximumを混在させないでください。

`recovery-estimate.json`の平常範囲の上限（回復しきい値）は、B・Cごとに`max(2 × baseline p95, baseline p95 + 250ms)`で計算します。baselineを取得できなかった場合は`2 × work_ms`へフォールバックします。悪化を観測した後、各テナントの直近10件中8件以上がこのしきい値以下となった最初の窓を回復とし、その窓の最後のメッセージ開始時刻を採用します。`recovery_latency_ms`は悪化検出時点からではなく、Aのバースト開始からB・Cの両方が回復するまでの時間です。このしきい値は本検証の分析用定義であり、AWS Fair Queues内部の判定しきい値ではありません。

manifestには`baseline_duration_ms`、`baseline_interval_ms`、`baseline_messages_per_scenario`を保存し、回復しきい値の算出に使用したbaseline条件を再現できるようにします。

CloudWatch Logsまたはメトリクスの到着が遅れて一部件数が不足した場合は、数分待ってから同じ`collect`コマンドを再実行できます。

## 再実行とキューの初期化

実験コマンドは、対象となる2つのキューが空でない場合は開始しません。処理完了を待てない場合のみpurgeします。

```bash
build/experiment purge --config build/experiment-config.json
```

SQSの`PurgeQueue`は連続実行に制限があるため、purge直後は60秒以上空けてください。

## Terraformの削除

```bash
terraform -chdir=terraform destroy
```

DLQにメッセージが残っていても、Terraformによるキュー削除は可能です。実験結果のローカルCSVは`terraform destroy`では削除されません。

## 参考資料

- [How Amazon SQS fair queues work](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fair-queues-detailed.html)
- [Available CloudWatch metrics for Amazon SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-available-cloudwatch-metrics.html)
- [Creating and configuring an Amazon SQS event source mapping](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html)
- [Using the message group ID with Amazon SQS queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html)

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
