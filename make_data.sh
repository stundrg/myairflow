#!/bin/bash

# 인자값 확인
if [ $# -ne 1 ]; then
    echo "사용법: $0 <저장_경로>"
    echo "예: $0 /home/user/data"
    exit 1
fi

save_path="$1"

# 경로가 존재하는지 확인하고 없으면 생성
if [ ! -d "$save_path" ]; then
    mkdir -p "$save_path"
    if [ $? -ne 0 ]; then
        echo "경로 생성에 실패했습니다. 스크립트를 종료합니다."
        exit 1
    fi
fi

# 출력 파일 경로 설정
output_file="$save_path/data.csv"

# 과일 이름 리스트
fruits=("apple" "banana" "orange" "grape" "mango" "kiwi")

# user 이름 리스트 생성 (user_001 ~ user_100)
users=()
for i in $(seq -f "user_%03g" 1 100); do
    users+=("$i")
done

# CSV 헤더 작성
echo "id,name,value,timestamp" > "$output_file"

# 100,000 행 생성
for i in $(seq 1 10000); do
    # 랜덤으로 name 선택
    name=${users[$RANDOM % ${#users[@]}]}

    # 랜덤으로 value (과일) 선택
    value=${fruits[$RANDOM % ${#fruits[@]}]}

    # 마이크로초 단위 타임스탬프 (ISO 8601 형식)
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%S.%6NZ")

    # CSV 행 추가
    echo "$i,$name,$value,$timestamp" >> "$output_file"
done

echo "CSV 파일이 $output_file 에 생성되었습니다."
