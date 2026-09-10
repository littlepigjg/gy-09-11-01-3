FROM python:3.12-slim

WORKDIR /app

# 使用国内镜像源加速 pip 安装
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# 默认启动 API 服务; simulator 容器会覆盖 CMD
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
