# spark-app
csv 파일을 읽어 Hive 테이블에 넣는 스파크 애플리케이션 프로젝트입니다.

## 환경
```Spark 3.3.3```
```Scala 2.13.10```
## 프로젝트 구조
프로젝트의 구조는 다음과 같습니다.<br>
```bash
spark-app
├─.idea
├─data
│  ├─hivetable              // 처리한 데이터를 저장할 Hive table 경로
│  ├─log                    // raw data(csv) 경로
│  └─metadata
├─project
│  └─build.properties
├─src
│  ├─main
│  │  └─scala
│  │     └─SparkApp        // 애플리케이션을 실행하는 파일
├─bulid.sbt                 // sbt 설정 파일
└─README.md
```