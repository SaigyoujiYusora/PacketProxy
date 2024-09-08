import asyncio
import json
import logging as org_logging
import random
from contextlib import asynccontextmanager
from random import randint
from typing import Dict, List, final

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse
from watchfiles import awatch
from websockets.sync.client import connect

# 配置uvicorn日志
logging = org_logging.getLogger("uvicorn")
logging.setLevel(org_logging.DEBUG)

# 客户端名称
client_alias_list = [
    "水星",
    "火星",
    "木卫二中转节点",
    "联邦第三行星级计算节点"
]


# 傻逼吧 运行完了才跑后半截
@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("初始化完成")
    yield
    logging.info("开始关机流程")


app = FastAPI(lifespan=lifespan)

"""
时尚小垃圾分界点
"""


class QueueManager:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.cache = []

    async def put(self, data):
        """
        将数据放入消息队列和缓存
        :param data: 要存的数据
        """
        await self.queue.put(data)
        self.cache.append(data)

    async def get(self):
        """
        从消息队列中取出数据
        理论上和asyncio.Queue.get()一样在无数据时挂起当前协程
        :return:
        """
        data = await self.queue.get()
        if data in self.cache:
            self.cache.remove(data)
        return data

    def empty(self):
        """
        判断消息队列是否为空
        :return:
        """
        return self.queue.empty()

    def peak(self):
        """
        偷看队列缓存的第一个元素
        :return:
        """
        return self.cache[-1]


class WebSocketManager:
    def __init__(self):
        """
        自己搓的WS管理器
        """
        self.clients: Dict[str, int] = {}  # {"客户端名称": 客户端索引}
        # self.connections: List[WebSocket] = []
        self.connections: Dict[int, WebSocket] = {}  # {客户端索引: WebSocket}
        # self.event_queue = asyncio.Queue()
        self.event_queue = QueueManager()  # 事件队列
        """
        格式: {"sessionID": client_index(Int)}
        示例: {"114514191": 1, "19198100": 0}
        """
        self.session_bind: Dict[str | int, int] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()

        # 客户端认证
        # await websocket.send_text('f{"code":}')

        # 客户端连接
        try:
            client_id = [*self.connections.keys()][-1] + 1
        except IndexError:
            client_id = 0
        self.connections[client_id] = websocket

        # 分配客户端标识名称
        diff = set(client_alias_list).difference(set(self.clients))
        if diff:
            client_name = list(diff)[0]
        else:
            client_name = f"客户端{client_id}"
        self.clients[client_name] = client_id

    async def disconnect(self, websocket: WebSocket,data = None):
        # 获取客户端索引
        client_id = self.get_id(websocket)
        client_name = self.get_name(websocket)
        # 从connections中移除客户端
        del self.connections[client_id]
        # 从clients中移除客户端
        del self.clients[client_name]


        # 尝试从空闲客户端列表获取一个客户端
        idle_client_ids = list(self.connections.keys())
        for index in self.session_bind.values():
            if index in idle_client_ids:
                idle_client_ids.remove(index)
        if idle_client_ids:
            new_client_id = random.choice(idle_client_ids)
        else:
            new_client_id = random.choice(list(self.connections.keys()))

        # 将发送失败的数据转交给新的客户端
        if data:
            logging.warning("移交发送失败的数据包给新的客户端")
            await self.send_data(data, client_id=new_client_id)
        # 将客户端的sessionID交给新的客户端
        for session, index in self.session_bind.items():
            if index == client_id:
                self.session_bind[session] = new_client_id


    async def send_data(self, data_packet: Dict, client_name: str = None, client_id: int = None):
        if client_name:
            await self.connections[self.clients[client_name]].send_json(data_packet)
        if client_id:
            await self.connections[client_id].send_json(data_packet)

    async def broadcast(self, message: str):
        for client in self.connections.values():
            await client.send_text(message)

    async def client_bind_session(self, session_id: str, client_id: int):
        self.session_bind[session_id] = client_id

    def get_id(self, websocket: WebSocket):
        """
        获取websocket在connections中的索引
        :param websocket:
        :return: 客户端id
        """
        try:
            index = list(self.connections.keys())[list(self.connections.values()).index(websocket)]
        except ValueError:
            index = None
        return index

    def get_name(self, websocket: WebSocket):
        """
        获取websocket在clients中的名称
        :param websocket:
        :return: 客户端名称
        """
        try:
            name = list(self.clients.keys())[list(self.clients.values()).index(self.get_id(websocket))]
        except ValueError:
            name = None
        return name

    async def heartbeat(self, websocket: WebSocket, interval: int = 20):
        while True:
            try:
                logging.info("发送心跳包")
                await websocket.send_text("ping")
                await asyncio.sleep(interval)
            except WebSocketDisconnect:
                logging.info("心跳包检测到客户端断开连接")
                await self.disconnect(websocket)
                break

    def random_choice_id(self):
        """
        随机选择一个客户端
        :return: 客户端id
        """
        client_id = random.choice(list(self.connections.keys()))
        return client_id


ws_manger = WebSocketManager()


@app.get("/get_server_url/")
def get_server_url():
    """
    获取主服务器的URL
    :return:
    """
    # 返回主服务器的URL
    return JSONResponse({"server_url": "ws://localhost:8000/ws"})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manger.connect(websocket)

    heartbeat_task = asyncio.create_task(ws_manger.heartbeat(websocket))
    while True:
        try:
            if websocket not in list(ws_manger.connections.values()):
                logging.info(f"终止{websocket}监听进程")
                break
            cache = ws_manger.event_queue.peak()

            # 如果当前客户端的sessionID不在session_bind中, 则跳过
            if ws_manger.session_bind.get(cache.get("session")) is None:
                # 分配随机客户端给sessionID
                logging.info("未找到对应节点，分配客户端中")
                await ws_manger.client_bind_session(cache.get("session"), ws_manger.random_choice_id())
                continue
            # 如果当前客户端的sessionID和session_bind中的sessionID不匹配, 则跳过
            elif ws_manger.get_id(websocket) != ws_manger.session_bind.get(cache.get("session")):
                await asyncio.sleep(1)
                logging.info("SessionID不匹配")
                continue
        # 未找到数据包
        except IndexError:
            await asyncio.sleep(1)
            # logging.debug("在循环里2")
            continue
        logging.debug("等待数据包")
        data = await ws_manger.event_queue.get()
        logging.debug("发送数据包")
        try:
            await websocket.send_json(data)
        except WebSocketDisconnect:
            await ws_manger.disconnect(websocket,data)
            logging.info("发送失败-->客户端断开连接")
        finally:
            heartbeat_task.cancel()



@app.post("/send_data")
async def send_data(data_packet: Dict):
    # 接收到数据包后将其放入事件队列
    check_list = ["header", "addr", "data", "session"]
    missing = []
    for item in check_list:
        if data_packet.get(item) is None:
            missing.append(item)
    else:
        if len(missing) > 0:
            return JSONResponse({"status": "数据包缺少以下字段: " + ", ".join(missing)})

    if data_packet.get("session") is None:
        data_packet["session"] = None
    await ws_manger.event_queue.put(data_packet)
    return JSONResponse({"status": "数据包进入队列"})


# @app.post("/send_data_one_client")
# async def sent_data_one(data_packet: Dict):
#     # 接收到数据包后将其放入事件队列, 加入参数指定的客户端
#     client_id = data_packet.get("client_id")
#     if client_id in clients:
#         await clients[client_id].send_json(data_packet)
#         return JSONResponse({"status": "数据包已发送"})


@app.get("/query_data")
async def query_data():
    # 打印整个事件队列
    data = {"latest_data": None, "queue": []}
    if not ws_manger.event_queue.empty():
        data["latest_data"] = ws_manger.event_queue.peak()
        data["queue"] = ws_manger.event_queue.cache
    return JSONResponse(data)


@app.post("/ws_test")
@app.get("/ws_test")
async def ws_test(request: Request):
    # 打印请求的header, body
    print("Request Headers:")
    print(dict(request.headers))
    return JSONResponse({"status": "ok"})


@app.get("/get_online_clients")
async def online_client():
    print(ws_manger.clients.keys())
    print(ws_manger.clients)
    print(ws_manger.connections)
    # 返回在线客户端的数量
    return JSONResponse({"online_clients": ws_manger.clients})
