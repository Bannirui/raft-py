#!/usr/bin/bash

echo "start deploy raft-py"

# 当前项目名
proj_name=$(basename "$PWD")
# 父目录
parent_dir=$(dirname "$PWD")
# 项目源码拷贝路径
function cpy_and_run() {
  local target_path="$parent_dir/${proj_name}_cpy_$1"
  echo "将$proj_name复制到$target_path"
  cp -r "$PWD" "$target_path"
  (
    # 进入新目录启动
    cd "$target_path" || exit
    # create venv
    echo "create venv"
    python3 -m venv .venv
    # activate venv
    source .venv/bin/activate
    # install
    echo "安装依赖"
    if [ -f "requirements.txt" ]; then
      python3 -m pip install -r requirements.txt
    fi
    # start
    echo "启动$target_path"
    nohup .venv/bin/python main.py --MyId=${1} > "output_$1.log" 2>&1 & echo $! > "pid_$1.txt"
  )
}

# 3个节点
for i in 1 2 3; do
  cpy_and_run "$i"
done