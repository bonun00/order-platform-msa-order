
# 🛒 Kafka 기반 비동기 주문 처리 시스템 (Event-Driven Order System)

본 프로젝트는 대규모 트래픽 환경에서 안정적이고 확장 가능한 주문 처리를 위해 **Apache Kafka**를 도입한 **이벤트 기반 아키텍처(Event-Driven Architecture)** 중심의 마이크로서비스 가상 프로젝트입니다. 

기존 동기식(HTTP/REST) 호출 구조에서 발생할 수 있는 서비스 간 강결합(Tight Coupling) 문제를 해결하고, 특정 서비스의 장애가 전체 시스템으로 확산되지 않도록 **장애 격리(Fault Tolerance)**와 **최종 정합성(Eventual Consistency)**을 보장하는 데 초점을 맞추어 설계되었습니다.

---

## 🏗️ 1. 시스템 아키텍처 및 이벤트 흐름 (System Architecture)

전체 시스템은 **주문(Order), 재고(Inventory), 결제(Payment)** 서비스로 구성되며, 각 서비스는 독자적인 데이터베이스를 가진 분산 환경입니다. 분산 트랜잭션 관리를 위해 **Choreographed Saga Pattern**을 적용했습니다.

## 🛠️ 2. 기술 스택 (Tech Stack)

* **Framework:** Java 17 / Spring Boot 3.x / Spring Kafka
* **Message Broker:** Apache Kafka (Docker Compose 기반 클러스터 구성)
* **Database:** MySQL 8.0 (비즈니스 데이터), Redis (멱등성 검증 및 캐싱)
* **Build Tool:** Gradle
## 🛠 Tech Stack

### ⚙️ Backend & Database
<p>
  <img src="https://skillicons.dev/icons?i=java,spring,mysql,postgres,redis,kafka" alt="backend skills" />
</p>

### ☁️ Infra & 💻 Frontend
<p>
  <img src="https://skillicons.dev/icons?i=aws,react,git,github" alt="infra and frontend skills" />
</p>
---

## 🏗 3. 카프카(Kafka) 기반 이벤트 중심 주문 아키텍처 (Event-Driven Architecture)

본 프로젝트는 마이크로서비스(MSA) 간의 결합도를 낮추고 비동기 처리를 위해 **Apache Kafka**를 도입하여 이벤트 기반으로 주문 로직을 처리합니다. 주문, 결제, 상점(재고), 단말기(배달) 도메인 간의 데이터 정합성을 사가 패턴(Saga Pattern)을 활용해 유지합니다.

### 🔄 Kafka 토픽 흐름 (Topic Flow)

1. **`Order Valid Request` & `Order Valid Result` (주문 유효성 검증)**
   - 고객이 주문을 생성하면, Order 서비스에서 Store 서비스로 메뉴 및 주문의 유효성 검증을 요청하고 그 결과를 반환받습니다.
2. **`Payment Result` (결제 결과 처리)**
   - Toss Payments API를 통한 결제가 성공적으로 완료되면 발행됩니다. Order 서비스는 이를 구독하여 주문 상태를 '재고 대기(Ready for stock)'로 변경합니다.
3. **`Stock Request` & `Stock Result` (재고 차감 및 보상 트랜잭션)**
   - 결제가 완료된 주문에 대해 Store 서비스에 재고 차감을 요청합니다.
   - **재고 차감 성공 시**: 주문 상태를 '수락 대기(Ready for accept)'로 변경합니다.
   - **재고 차감 실패 시**: 주문 상태를 '실패(Failed)'로 변경하고 즉시 `Refund Request`를 발행합니다.
4. **`Refund Request` (환불 및 롤백)**
   - 재고 부족 등의 이유로 프로세스가 실패했을 때 발행되는 **보상 트랜잭션**입니다. Payment 서비스에서 토스 결제 취소를 진행하고, Store 서비스에서는 롤백(재고 복구) 작업을 수행합니다.
5. **`Order Approve` (점주 주문 수락/거절)**
   - 점주가 해당 주문을 최종적으로 수락하거나 거절할 때 발행됩니다. 주문 거절 시 환불 프로세스로 이어지며, 수락 시 주문이 최종 확정됩니다.
6. **`Order Completed` (주문 완료)**
   - 점주가 수락한 최종 주문 상태가 발행되며, 단말기(Terminal) 서비스에서 이를 구독하여 후속 처리(배달 추적 등)를 진행합니다.

### 🚨 모니터링 및 장애 대응 (Monitoring & DLT)

- **Prometheus & Grafana**: 카프카 메시지 처리량, 지연 시간(Lag) 등의 주요 메트릭을 수집하고 시각화하여 시스템 상태를 실시간으로 모니터링합니다.
- **DLT (Dead Letter Topic)**: 컨슈머에서 지속적으로 처리에 실패한 불량 메시지는 DLT로 격리됩니다.
- **Discord Alert**: DLT로 메시지가 인입되거나 시스템에 치명적인 에러가 발생할 경우, Discord 웹훅(Webhook)을 통해 개발팀에 즉각적인 알림이 발송됩니다.

---

## ✨ 4. 핵심 기술적 의사결정 및 해결 과제

### 1) Transactional Outbox Pattern을 통한 메시지 발행 보장
* **문제 상황:** DB에 주문 정보를 저장하는 트랜잭션은 성공했으나, 네트워크 장애 등의 이유로 Kafka에 이벤트를 발행하는 행위가 실패할 경우 데이터 불일치 발생 가능성이 있었습니다.
* **해결 방법:** 이를 해결하기 위해 주문 생성과 동시에 같은 로컬 트랜잭션 내에서 `outbox` 테이블에 이벤트 데이터를 함께 저장합니다. 이후 별도의 스케줄러(또는 Debezium 같은 CDC Tool)가 `outbox` 테이블을 읽어 Kafka로 메시지를 발행함으로써, **최소 한 번은 전송(At-least-once delivery)**을 보장하도록 구현했습니다.

### 2) Dead Letter Queue (DLQ)를 통한 예외 처리 및 모니터링
* **문제 상황:** 로직 오류나 일시적인 인프라 장애로 인해 특정 메시지 처리가 지속적으로 실패(Retry Exhausted)할 경우, 전체 파티션의 메시지 처리가 멈추는(Blocking) 문제가 발생합니다.
* **해결 방법:** 각 토픽별로 리트라이 정책(`SimpleRetryPolicy`)을 설정하여 최대 3회 재시도 후에도 실패한 메시지는 각 토픽의 `.DLQ` 후속 토픽(예: `order-created.DLQ`)으로 발행 시키고 알림을 발송하여 시스템의 가용성을 유지했습니다.
