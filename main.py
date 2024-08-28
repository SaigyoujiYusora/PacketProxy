import asyncio
import logging as org_logging
import random
from contextlib import asynccontextmanager
from typing import Dict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse

# 配置uvicorn日志
logging = org_logging.getLogger("uvicorn")
logging.setLevel(org_logging.DEBUG)
# 客户端
clients: Dict[str, WebSocket] = {}
# 事件队列
event_queue = asyncio.Queue()
# 用于存储所有 WebSocket 连接的列表
connections = []
# 客户端名称
client_alias_list = [
    "水星",
    "木卫二中转节点",
    "联邦第三行星级计算节点"
]
# 心跳包进程
heartbeat_tasks: Dict[int, asyncio.Task] = {}


# 傻逼吧 运行完了才跑后半截
@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("初始化完成")
    yield
    logging.info("开始关机流程")


app = FastAPI(lifespan=lifespan)


async def heartbeat():
    while True:
        await asyncio.sleep(5)
        for client in clients.values():
            try:
                await client.send_json({"type": "heartbeat"})
            except Exception as e:
                logging.error(f"心跳包发送失败: {e}")
                continue


"""
时尚小垃圾分界点
"""


@app.get("/get_server_url/")
def get_server_url():
    # 返回主服务器的URL
    return JSONResponse({"server_url": "ws://localhost:8000/ws"})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connections.append(websocket)
    client_id = str(websocket.client)
    clients[client_id] = websocket
    try:
        while True:
            # 等待事件队列或心跳包超时
            data_task = asyncio.create_task(event_queue.get())
            heartbeat_task = asyncio.create_task(asyncio.sleep(10))  # 心跳包间隔时间

            done, pending = await asyncio.wait(
                [data_task, heartbeat_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            logging.info(f"完成队列: {done}")

            if data_task in done:
                logging.debug(f"触发转发事件")
                # 如果接收到数据包
                data_packet = data_task.result()
                if not data_packet:
                    continue

                logging.info("获取连接")
                connection = random.choice(connections)
                logging.debug(connection)
                logging.info("开始转发数据包")

                try:
                    await connection.send_json(data_packet)
                except Exception as e:
                    logging.warning(f"数据下发失败: {e}")
                    logging.warning(connection)

                logging.info(data_packet)

            if heartbeat_task in done:
                logging.debug(f"触发心跳包")
                # 发送心跳包
                try:
                    await websocket.send_text("ping")
                    pong = await asyncio.wait_for(websocket.receive_text(), timeout=10)  # 等待客户端的 "pong"
                    logging.info(f"完成队列: {done}")
                    # if pong != "pong":
                    #     raise WebSocketDisconnect
                except asyncio.TimeoutError:
                    logging.warning("心跳包超时，客户端可能已断开连接")
                    raise WebSocketDisconnect

            # 取消未完成的任务
            for task in pending:
                task.cancel()
            # # 从事件队列中获取下一个数据包
            # try:
            #     data_packet = await event_queue.get()
            # except asyncio.CancelledError:
            #     connections.remove(websocket)
            #     del clients[client_id]
            #     logging.warning("ws监听事件已关闭")
            #     break
            # except WebSocketDisconnect:
            #     connections.remove(websocket)
            #     del clients[client_id]
            #     logging.info("客户端断开连接")
            # logging.info(f"wow")
            # # try:
            # #     data_packet = await asyncio.wait_for(event_queue.get(), timeout=5)
            # # except asyncio.TimeoutError:
            # #     print("等待数据超时")
            # #     print("关闭事件状态", shutdown_event.is_set())
            # #     if shutdown_event.is_set():
            # #         print("关闭事件已触发")
            # #         break
            # #     continue
            # # except asyncio.CancelledError:
            # #     print("事件队列已关闭2")
            # #     break
            # # 随机获取一个连接
            # if not data_packet:
            #     continue
            # print("获取连接")
            # connection = random.choice(connections)
            # print("开始转发数据包")
            # try:
            #     await connection.send_json(data_packet)
            #     # await websocket.send_json(data_packet)
            # except Exception as e:
            #     print(f"数据下发失败: {e}")
            # print(data_packet)
    except WebSocketDisconnect:
        connections.remove(websocket)
        del clients[client_id]
        logging.info("客户端断开连接")
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")
    finally:
        if websocket in connections:
            connections.remove(websocket)
        if client_id in clients:
            del clients[client_id]
        try:
            await websocket.close()  # 确保 WebSocket 连接关闭
        except RuntimeError:
            pass
        logging.info(f"WebSocket 连接已关闭，客户端 {client_id} 已断开连接")


@app.post("/send_data")
async def send_data(data_packet: Dict):
    # 接收到数据包后将其放入事件队列
    await event_queue.put(data_packet)
    return JSONResponse({"status": "数据包进入队列"})


@app.post("/send_data_one_client")
async def sent_data_one(data_packet: Dict):
    # 接收到数据包后将其放入事件队列, 加入参数指定的客户端
    client_id = data_packet.get("client_id")
    if client_id in clients:
        await clients[client_id].send_json(data_packet)
        return JSONResponse({"status": "数据包已发送"})


@app.get("/query_data")
async def query_data():
    # 打印整个事件队列
    data = []
    while not event_queue.empty():
        data.append(await event_queue.get())
    return JSONResponse({"data": data})


@app.post("/ws_test")
@app.get("/ws_test")
async def ws_test(request: Request):
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
