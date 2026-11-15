# Mediflow 연결 프로필

Forge는 인증된 실험 조회 화면이고, Mediflow가 데이터 동결·학습·평가·GPU 실행을 소유한다. 이 프로필은 Forge TrainingManager를 만들지 않는다. 새 학습, supervisor tick, 예약 변경은 TRAINING_OWNER만 수행한다.

## 실행

`FORGE_REPO`는 이 연동 브랜치의 checkout, `FORGE_PROFILE`은 별도 비공개 상태 디렉터리, `TELEMETRY_ROOT`는 운영자가 허용한 계측·데이터 참조 카탈로그다. 기존 학습 venv나 운영 Forge DB를 사용하지 않는다. Python 3.10 이상 표준 라이브러리만 필요하다.

```bash
python3 "$FORGE_REPO/server.py" \
  --backend mediflow --mediflow-root "$TELEMETRY_ROOT" \
  --db "$FORGE_PROFILE/state.sqlite3" \
  --mediflow-db "$FORGE_PROFILE/mediflow.sqlite3" \
  --auth-db "$FORGE_PROFILE/auth.sqlite3" \
  --host 127.0.0.1 --port 18765 --no-worker
```

별도 프로필의 강한 관리자 비밀번호로 로그인한다. 비밀번호·토큰·계정 DB는 Git이나 인계 JSON에 넣지 않는다. 인증 초기화가 필요한 새 프로필에는 `--prompt-initial-password`를 사용하고 로컬 설정을 완료한다. LAN 공개나 인증 해제는 필요하지 않다.

`--enable-training`과 Mediflow 모드를 함께 지정하면 시작 전에 거부한다. API에서도 Forge 실행·일시정지·재개·취소·재시도·다운로드·데이터 재분할을 거부한다. 불필요한 GPU 조회나 CUDA import를 하지 않는다. `/api/mediflow/*` 조회는 기존 로그인과 remote-view 권한을, 데이터 참조 등록은 추가로 CSRF와 remote-control 권한을 요구한다.

## 저장과 복구

원본 JSONL·상태·결과는 Mediflow가 기록한다. Forge는 별도 SQLite에 읽기 위치와 중복 제거 상태를 저장한다. 웹 서버와 브라우저의 종료는 학습 프로세스의 종료 조건이 아니다. Forge만 재시작하고 같은 프로필 DB와 계측 root를 사용하면 기록을 다시 읽는다. 부분 JSONL 마지막 줄은 완성될 때까지 보류한다. 파일 교체·절단으로 cursor가 무효화되면 해당 stream을 명시적으로 재조회한다.

완료된 과거 기록은 `historical_reconstructed`, 새 실행 중 직접 기록은 `live_measured`, 처리량 등 계산은 `derived`로 구분한다. 과거 step 시간·GPU 시계열이 없으면 null과 이유를 표시한다. 최종 loss는 실제 예제 수로 가중한 평균이며, 마지막 13/11개 배치를 16으로 나누지 않는다. 평가가 없으면 미실행이다. fresh reload sanity check는 독립 성능평가가 아니다.

외부 dataset은 immutable 참조 manifest로 등록한다. 기존 Forge 128 KiB/1,000행 업로드 경로로 변환하지 않으며, native split·processor·원본을 유지한다. 브라우저는 논리 manifest ID만 전송하고 서버는 운영자가 허용한 카탈로그만 읽는다. 원문 질문·정답·예측·이미지·가중치는 API에서 제공하지 않는다.

## Rollback

이 연동 프로필의 PID만 종료한다. 기존 Mediflow worker, supervisor, GPU lock, 예약에는 신호를 보내지 않는다. 프로필 DB와 계측 원본을 보존한다. 이전 검증 SHA의 별도 checkout으로 위 명령을 실행해 복구한다. 이 변경은 기존 운영 Forge와 학습 checkout에 적용하지 않았으므로 원래 서비스를 되돌릴 작업은 없다. 새 campaign 활성화의 rollback은 TRAINING_OWNER가 별도 rollout receipt에 기록한다.

## 도입 판정

CPU 테스트나 historical 화면만으로 RELEASE_READY를 만들지 않는다. 고정된 두 저장소 SHA의 후보에 대해 TRAINING_OWNER가 실제 text/VLM probe를 실행해야 한다. 브라우저의 서로 다른 두 step/loss, 새로고침 보존, Forge 재시작 중 학습 지속, 최종 metrics 일치와 계측 누락 범위를 확인한 뒤에만 RELEASE_READY를 기록한다. 최신 코드 변경이 있으면 이전 검증 SHA의 READY는 현재 후보에 적용되지 않는다.
