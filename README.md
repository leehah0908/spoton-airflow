# SpotOn 🏆

**스포츠 경기 일정 및 커뮤니티 플랫폼**

---

## 🏗️ 프로젝트 개요

**SpotOn**은 스포츠 팬들을 위한 플랫폼으로, 경기 일정 확인부터 실시간 응원 채팅, 커뮤니티 활동까지 한 곳에서 즐길 수 있는 서비스입니다.

🔗 **서비스 URL**: [https://onspoton.com/](https://onspoton.com/)

⚠️ **현재 모바일 최적화(반응형 웹) 작업 진행 중입니다.**  
현재 **모바일 환경에서는 일부 UI/UX가 원활하지 않을 수 있으므로, 웹 브라우저(PC)에서 이용해 주시면 감사하겠습니다.**  
빠른 시일 내에 반응형 웹 업데이트를 제공할 수 있도록 하겠습니다. 🚀

---

## 📂 관련 레포지토리

SpotOn 프로젝트는 **백엔드, 프론트엔드, Airflow가 별도 레포지토리**로 관리됩니다.

🔗 **Backend Repository**: [SpotOn Backend](https://github.com/leehah0908/spoton-backend)  
🔗 **Frontend Repository**: [SpotOn Frontend](https://github.com/leehah0908/spoton-frontend)

---

## 📌 기획 배경

스포츠를 사랑하는 팬들에게 가장 중요한 것은 **실시간 경기 정보**와 **팬들 간의 소통**입니다.  
하지만 기존의 스포츠 관련 서비스들은 경기 일정 제공에 집중되어 있거나, 팬 커뮤니티 기능이 제한적인 경우가 많습니다.

**SpotOn**은 이런 문제를 해결하고자 탄생했습니다.

✅ **한 곳에서 모든 스포츠 정보를!**

-   경기 일정, 결과, 팀별 맞춤 정보까지 한 번에 확인할 수 있습니다.

✅ **실시간 응원 & 커뮤니티 활성화!**

-   같은 팀을 응원하는 팬들과 실시간 채팅 및 자유로운 커뮤니티 소통이 가능합니다.

✅ **팬들 간의 연결을 강화!**

-   1:1 채팅 및 굿즈 나눔 기능을 통해 팬들끼리 더욱 가깝게 교류할 수 있습니다.

SpotOn은 단순한 경기 일정 확인 서비스를 넘어, **팬들이 함께 응원하고 소통할 수 있는 진정한 스포츠 커뮤니티**를 목표로 합니다.

---

## 🛠️ 기술 스택

### 🥇 Backend & Data Engineering

-   ![Spring Boot](https://img.shields.io/badge/Spring%20Boot-6DB33F?style=flat-square&logo=springboot&logoColor=white) - 애플리케이션 서버
-   ![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=flat-square&logo=mysql&logoColor=white) - 애플리케이션 서버 DB
-   ![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white) - 스케줄링
-   ![JPA](https://img.shields.io/badge/JPA-6DB33F?style=flat-square) , ![QueryDSL](https://img.shields.io/badge/QueryDSL-005571?style=flat-square) , ![JDBC](https://img.shields.io/badge/JDBC-003B57?style=flat-square) - 데이터베이스 접근
-   ![Redis](https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white) - 캐싱 및 세션 관리
-   ![JWT](https://img.shields.io/badge/JWT-000000?style=flat-square&logo=jsonwebtokens&logoColor=white) - 인증 및 보안
-   ![STOMP](https://img.shields.io/badge/STOMP-FF4500?style=flat-square&logo=rocket&logoColor=white) - 실시간 채팅

### 🥈 Frontend

-   ![React](https://img.shields.io/badge/React-21232A?style=flat-square&logo=react&logoColor=#61DAFB) - 사용자 인터페이스

### 🥉 DevOps

-   ![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat-square&logo=amazonwebservices&logoColor=white) - 클라우드 환경 (EC2, ECR, S3, RDS, Route53, CloudFront, IAM, ACM)
-   ![Nginx](https://img.shields.io/badge/Nginx-009639?style=flat-square&logo=nginx&logoColor=white) , ![Certbot](https://img.shields.io/badge/Certbot-3A6057?style=flat-square&logo=letsencrypt&logoColor=white) - 리버스 프록시 및 SSL 인증
-   ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white) - 컨테이너화
-   ![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white) - CI/CD 자동화

---

## ✨ 주요 기능

1️⃣ **경기 일정 및 결과 확인**

-   다양한 스포츠 경기 일정을 확인하고, 경기 결과를 실시간으로 확인

2️⃣ **실시간 경기 응원 채팅**

-   STOMP 기반의 웹소켓을 활용한 실시간 N:N 응원 채팅 기능 제공

3️⃣ **마이팀 설정**

-   사용자가 응원하는 팀을 선택하면, 해당 팀의 경기 일정이 한눈에 보이도록 구성

4️⃣ **커뮤니티 게시판**

-   스포츠 팬들끼리 자유롭게 의견을 나누고 소통할 수 있는 공간

5️⃣ **무료 굿즈 나눔 서비스**

-   팬들 간의 교류 활성화를 위한 굿즈 나눔 서비스

6️⃣ **유저 간 1:1 채팅**

-   굿즈 무료 나눔 성사를 위한 1:1 채팅 기능 제공

7️⃣ **회원가입 및 로그인**

-   SNS 소셜 로그인 지원 (Google, Kakao, Naver)
-   JWT 기반의 인증 시스템 적용
-   회원 정보 관리 (닉네임, 프로필 사진, 응원하는 팀 설정 등)

---

## 📌 아키텍처

아래 이미지는 SpotOn의 전체 아키텍처를 나타냅니다.

<img width="1000" alt="Image" src="https://github.com/user-attachments/assets/749dc1c5-c2a5-4a8b-bb80-437f6b859e09" />

---

## 🗄️ ERD

<img width="1000" alt="Image" src="https://github.com/user-attachments/assets/9760295b-3224-41f5-a9af-688ca45a489c" />

🔗 [ERD 보기](https://www.erdcloud.com/d/9kNb3ACqSjytwmne3)

---

## 📄 API 문서

API 명세는 **Postman**에서 확인할 수 있습니다.  
🔗 [API 문서 보기](https://documenter.getpostman.com/view/38853291/2sAYQWLtf7)

---

## 🚀 배포

SpotOn은 **AWS 인프라**를 활용하여 배포되었습니다.

-   **EC2**: 백엔드 서버 (단일 인스턴스)
-   **S3 + CloudFront**: 프론트엔드 정적 파일 배포 및 이미지 저장소
-   **S3**: 정적 이미지 파일 저장소
-   **RDS (MySQL)**: 관계형 데이터베이스
-   **ECR + Docker**: 컨테이너 이미지 저장 및 관리
-   **GitHub Actions**: CI/CD 자동화
-   **Route 53**: 도메인 네임 시스템 (DNS) 관리
-   **IAM**: AWS 서비스 접근 제어 및 권한 관리
-   **ACM**: SSL/TLS 인증서 관리 및 HTTPS 보안

---
