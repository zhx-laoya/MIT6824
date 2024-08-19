#!/bin/bash

# 初始化计数器
success_count=0

for ((i=1; i<=10; i++)); do
    # 执行测试命令
    # 2A 
    # go test -run 2A 
    # go test -run TestTmp -race 
    # go test -run TestInitialElection2A
    # go test -run TestReElection2A

    # 2B 

    # go test -run TestBasicAgree2B
    # go test -run TestRPCBytes2B
    # go test -run TestFailAgree2B
    # go test -run TestFailNoAgree2B

    # go test -run TestConcurrentStarts2B
    # go test -run TestRejoin2B
    # go test -run TestBackup2B
    # go test -run TestCount2B
    # go test -run 2B
    # go test -run 2A 
    # go test -run 2B 
    # go test -run 2C 
    # go test -run TestSnapshotInstall2D
    go test -run TestSnapshotInstallUnreliable2D
    # 检查测试结果
    if [ $? -eq 0 ]; then
        # 测试成功
        success_count=$((success_count + 1))
    fi

    # 每执行 10 次输出一次成功执行的次数
    if [ $((i % 10)) -eq 0 ]; then
        echo "成功执行的次数：$success_count"
    fi
done
