Docker-all-in-one

Sigma Rule Matcher + OpenTelemetry Collector + Jaeger + OpenSearch + Kafka를 Docker Compose로 한 번에 실행합니다.

요구 사항

Docker Desktop (WSL2 권장)

PowerShell 또는 bash

(옵션) SysmonAgent 설치 및 이벤트 발생 환경

사전 작업

OTEL/otelcol-contrib.zip 압축 해제
→ 압축 해제 시 파일 생성과 동시에 풀리도록(덮어쓰기 OK).

SysmonAgent가 필요한 경우, 에이전트 설치 및 로그 발생 환경 준비.

실행
docker-compose up -d


재빌드가 필요할 땐:

docker-compose up -d --build

확인
1) Jaeger

UI: http://localhost:16686

2) OpenSearch Dashboards

UI: http://localhost:5601

3) Kafka (컨테이너 내부 네트워크용 포트 사용)

컨슘(기존 메시지 포함):

docker exec -it kafka bash -lc `
'kafka-console-consumer --bootstrap-server kafka:29092 --topic raw_trace --from-beginning --property print.timestamp=true --property print.offset=true'


(테스트) 메시지 1건 프로듀스:

docker exec -it kafka bash -lc `
'printf "{\"ping\":\"pong\"}\n" | kafka-console-producer --bootstrap-server kafka:29092 --topic raw_trace && echo OK'


중요:

컨테이너 간 접속은 kafka:29092, 호스트(로컬 클라이언트)에서 붙을 땐 localhost:9092를 사용합니다.

--from-beginning을 주지 않으면 새로 들어오는 메시지만 보입니다.

로그 보기 / 중지
# 전체 로그 팔로우
docker-compose logs -f

# 특정 서비스 로그 (예: otelcol)
docker-compose logs -f otelcol

# 중지/정리
docker-compose down

트러블슈팅

Kafka 소비가 비어 있음

토픽 존재 확인:
docker exec -it kafka bash -lc 'kafka-topics --bootstrap-server kafka:29092 --list'

OTEL Collector → Kafka 브로커 주소가 kafka:29092인지 확인 (9092는 호스트 전용).

Collector에 tail_sampling이 켜져 있으면 조건(sigma.alert)에 맞지 않는 트레이스는 드롭됩니다. 검증을 위해 잠시 always_sample로 바꾸어 파이프라인부터 확인하세요.

Jaeger가 바로 내려감

OpenSearch가 준비되기 전에 Jaeger가 뜨면 실패할 수 있습니다. healthcheck + depends_on: service_healthy 구성을 고려하세요.

폴더 구조(예시)
.
├─ docker-compose.yml
├─ OTEL/
│  ├─ otelcol-contrib.zip
│  └─ otelcol-contrib/
│     └─ otel-collector-config.yaml
├─ sigma_matcher/
│  ├─ Dockerfile
│  ├─ main.go
│  └─ rules/
│     └─ rules/windows/...
└─ event/
   └─ events.jsonl   # (파일 exporter 사용 시)
