# Docker-all-in-one

Sigma Rule Matcher + OpenTelemetry Collector + Jaeger + OpenSearch + Kafka를 **Docker Compose**로 한 번에 실행합니다.

## 요구 사항

-   Docker Desktop (WSL2 권장)
-   PowerShell 또는 bash
-   (옵션) SysmonAgent 설치 및 이벤트 발생 환경

## 사전 작업

1. `OTEL/otelcol-contrib.zip` 압축 해제  
   → 압축 해제 시 **파일 생성과 동시에** 풀리도록(덮어쓰기 OK).
2. SysmonAgent가 필요한 경우, 에이전트 설치 및 로그 발생 환경 준비.

## 실행

```powershell
docker-compose up -d
```

> 재빌드가 필요할 땐:

```powershell
docker-compose up -d --build
```

## 확인

### 1) Jaeger

-   UI: http://localhost:16686

### 2) OpenSearch Dashboards

-   UI: http://localhost:5601

### 3) Kafka (컨테이너 내부 네트워크용 포트 사용)

**컨슘(기존 메시지 포함):**

```powershell
docker exec -it kafka bash -lc 'kafka-console-consumer --bootstrap-server kafka:29092 --topic raw_trace --from-beginning --property print.timestamp=true --property print.offset=true'
```

> **중요**
>
> -   컨테이너 간 접속은 `kafka:29092`, 호스트(로컬 클라이언트)에서 붙을 땐 `localhost:9092`.
> -   `--from-beginning`을중
