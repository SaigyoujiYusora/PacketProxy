import json
import logging
import random

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse
from typing import List, Dict
import asyncio

# 配置uvicorn日志
uvicorn_logger = logging.getLogger("uvicorn")
uvicorn_logger.setLevel(logging.DEBUG)


@asynccontextmanager
async def lifespan(app: FastAPI):
    uvicorn_logger.info("初始化完成")
    yield
    uvicorn_logger.info("开始关机流程")
    try:
        # await shutdown()
        print("真能跑到这？")
        # pass
    except Exception as e:
        uvicorn_logger.error(f"Exception during shutdown: {e}")


async def shutdown():
    uvicorn_logger.info("Shutting down: Cancelling all tasks")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        try:
            task.cancel()
        except Exception as e:
            uvicorn_logger.error(f"An error occurred while cancelling task {task}: {e}")
    # 使用 asyncio.shield 防止 gather 自身被取消
    try:
        await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))
    except asyncio.CancelledError:
        uvicorn_logger.info("Shutdown process was cancelled")
    uvicorn_logger.info("All tasks cancelled")


app = FastAPI(lifespan=lifespan)

# 客户端
clients: Dict[str, WebSocket] = {}

# 事件队列
event_queue = asyncio.Queue()


# @app.on_event("shutdown")
# async def shutdown_event():
#     print("Shutting down: Cancelling all tasks")
#     tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
#     [task.cancel() for task in tasks]
#     await asyncio.gather(*tasks, return_exceptions=True)
#
#


@app.get("/get_server_url/")
def get_server_url():
    # 返回主服务器的URL
    return JSONResponse({"server_url": "ws://localhost:8000/ws"})


# 用于存储所有 WebSocket 连接的列表
connections = []


@app.on_event("startup")
async def startup_event():
    logging.info("Application startup")


#
# @app.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     client_id = str(websocket.client)
#     clients[client_id] = websocket
#
#     try:
#         while True:
#             # 从事件队列中获取下一个数据包
#             data_packet = await event_queue.get()
#             if data_packet:
#                 await websocket.send_json(data_packet)
#                 print("开始转发数据包")
#                 print(data_packet)
#     except WebSocketDisconnect:
#         del clients[client_id]


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    try:
        await websocket.accept()
        connections.append(websocket)
        client_id = str(websocket.client)
        clients[client_id] = websocket
        try:
            while True:
                # 从事件队列中获取下一个数据包
                data_packet = await event_queue.get()
                if data_packet:
                    # 随机获取一个连接
                    print("获取连接")
                    connection = random.choice(connections)
                    print("开始转发数据包")
                    try:
                        await connection.send_json(data_packet)
                        # await websocket.send_json(data_packet)
                    except Exception as e:
                        print(f"数据下发失败: {e}")
                    print(data_packet)
        except WebSocketDisconnect:
            connections.remove(websocket)
            del clients[client_id]
            logging.info("Client disconnected")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


@app.post("/send_data/")
async def send_data(data_packet: Dict):
    # 接收到数据包后将其放入事件队列
    await event_queue.put(data_packet)
    return JSONResponse({"status": "数据包进入队列"})


@app.post("/send_data_one_client/")
async def sent_data_one(data_packet: Dict):
    # 接收到数据包后将其放入事件队列, 加入参数指定的客户端
    client_id = data_packet.get("client_id")
    if client_id in clients:
        await clients[client_id].send_json(data_packet)
        return JSONResponse({"status": "数据包已发送"})


@app.get("/query_data/")
async def query_data():
    # 打印整个事件队列
    data = []
    while not event_queue.empty():
        data.append(await event_queue.get())
    return JSONResponse({"data": data})


@app.post("/ws_test")
@app.get("/ws_test")
async def ws_test(request: Response):
    # 打印请求的header, body
    print("Request Headers:")
    print(dict(request.headers))
    return JSONResponse({"status": "ok"})


@app.get("/get_online_clients")
async def online_client():
    print(clients.keys())
    print(clients)
    print(connections)
    # 返回在线客户端的数量
    # return JSONResponse({"online_clients": clients.keys()})

# class Client:
#     def __init__(self, url):
#         self.url = url
#         self.queue = asyncio.Queue()
#         self.concurrency = 10
#
#     async def connect_to_server(self):
#         async with websockets.connect(self.url) as websocket:
#             await self.process_messages(websocket)
#
#     async def process_messages(self, websocket):
#         print("Connected to server")
#         while True:
#             try:
#                 try:
#                     print("into try")
#                     message = await asyncio.wait_for(websocket.recv(), timeout=3)
#                     print("wocao")
#                 except asyncio.TimeoutError:
#                     print("接收数据超时")
#                     continue
#                 except websockets.ConnectionClosed:
#                     print("连接已关闭")
#                     break
#                 data_packet = json.loads(message)
#                 await self.queue.put(data_packet)
#                 await self.handle_data()
#             except Exception as e:
#                 print(f"An error occurred: {e}")
#
#     async def handle_data(self):
#         print("into handle_data")
#         tasks = []
#         while not self.queue.empty():
#             for _ in range(self.concurrency):
#                 data_packet = await self.queue.get()
#                 task = asyncio.create_task(self.forward_data(data_packet))
#                 tasks.append(task)
#             await asyncio.gather(*tasks)
#
#     async def forward_data(self, data_packet):
#         print("into forward_data")
#         headers = data_packet.get("header", {})
#         url = data_packet.get("addr")
#         data = data_packet.get("data")
#         async with aiohttp.ClientSession() as session:
#             try:
#                 async with session.post(url, headers=headers, data=data) as response:
#                     status = response.status
#                     print(f"Forwarded to {url}, Response: {status}")
#                     print(await response.text())
#             except aiohttp.ClientError as e:
#                 print(f"Failed to forward data: {e}")
#
# @app.get("/test")
# async def cool():
#     print("Connecting to server")
#     async with aiohttp.ClientSession() as session:
#         async with session.get("http://localhost:8000/get_server_url/") as resp:
#             server_info = await resp.json()
#             client = Client(server_info["server_url"])
#             await client.connect_to_server()
#             print("Connection closed")
