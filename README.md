
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

## 📦 3. 카프카 토픽 설계 (Kafka Topic Design)

메시지 순서 보장과 확장성을 고려하여 **주문 ID(Order ID)를 메시지 키(Message Key)**로 설정하여 동일한 주문에 대한 이벤트는 항상 동일한 파티션으로 인입되도록 설계했습니다.

| 토픽명 (Topic Name) | 발행처 (Producer) | 구독처 (Consumer) | 핵심 데이터 (Payload) | 설명 |
| :--- | :--- | :--- | :--- | :--- |
| `order-created` | Order Service | Inventory, Payment | `orderId`, `userId`, `items`, `totalPrice` | 사용자의 주문이 접수됨을 알림 |
| `stock-deducted` | Inventory Service | Order Service | `orderId`, `status` | 재고가 성공적으로 차감됨을 알림 |
| `stock-failed` | Inventory Service | Order Service | `orderId`, `reason` | 재고 부족 등으로 차감 실패 시 발행 |
| `payment-approved` | Payment Service | Order Service | `orderId`, `paymentId`, `status` | 결제 승인이 완료됨을 알림 |
| `payment-failed` | Payment Service | Order Service | `orderId`, `reason` | 한도 초과, PG사 장애 등으로 실패 시 발행 |

---

## ✨ 4. 핵심 기술적 의사결정 및 해결 과제

## ✨ 4. 핵심 기술적 의사결정 및 해결 과제

### 1) Transactional Outbox Pattern을 통한 메시지 발행 보장
* **문제 상황:** DB에 주문 정보를 저장하는 트랜잭션은 성공했으나, 네트워크 장애 등의 이유로 Kafka에 이벤트를 발행하는 행위가 실패할 경우 데이터 불일치 발생 가능성이 있었습니다.
* **해결 방법:** 이를 해결하기 위해 주문 생성과 동시에 같은 로컬 트랜잭션 내에서 `outbox` 테이블에 이벤트 데이터를 함께 저장합니다. 이후 별도의 스케줄러(또는 Debezium 같은 CDC Tool)가 `outbox` 테이블을 읽어 Kafka로 메시지를 발행함으로써, **최소 한 번은 전송(At-least-once delivery)**을 보장하도록 구현했습니다.

### 2) 멱등성 컨슈머(Idempotent Consumer) 구현을 통한 중복 처리 방지
* **문제 상황:** Kafka의 네트워크 재시도 메커니즘으로 인해 동일한 이벤트가 컨슈머에게 중복 전달될 수 있으며, 이는 중복 결제나 중복 재고 차감과 같은 치명적인 유실을 유발할 수 있습니다.
* **해결 방법:** 소비측(Consumer)에서 Redis를 활용한 **Distributed Lock** 및 **이벤트 처리 이력 관리**를 도입했습니다. 메시지의 고유 식별자(`orderId` + `eventType`)를 Redis에 저장하여, 이미 처리된 키값의 이벤트가 들어올 경우 로직을 무시하고 커밋(Ack)만 수행하도록 설계하여 중복 처리를 방지했습니다.

### 3) Dead Letter Queue (DLQ)를 통한 예외 처리 및 모니터링
* **문제 상황:** 로직 오류나 일시적인 인프라 장애로 인해 특정 메시지 처리가 지속적으로 실패(Retry Exhausted)할 경우, 전체 파티션의 메시지 처리가 멈추는(Blocking) 문제가 발생합니다.
* **해결 방법:** 각 토픽별로 리트라이 정책(`SimpleRetryPolicy`)을 설정하여 최대 3회 재시도 후에도 실패한 메시지는 각 토픽의 `.DLQ` 후속 토픽(예: `order-created.DLQ`)으로 발행 시키고 알림을 발송하여 시스템의 가용성을 유지했습니다.
