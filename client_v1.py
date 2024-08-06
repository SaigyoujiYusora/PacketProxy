import asyncio
import websockets
import requests
import json
import aiohttp

# class Client:
#     def __init__(self, url):
#         self.url = url
#         self.queue = asyncio.Queue()
#         self.concurrency = 10
#
#     async def connect_to_server(self):
#         async with websockets.connect(self.url) as websocket:
#             while True:
#                 await self.process_messages(websocket)
#                 print("End")
#
#     async def process_messages(self, websocket):
#         print("Connected to server")
#         while True:
#             try:
#                 try:
#                     print("into try")
#                     message = await asyncio.wait_for(websocket.recv(), timeout=3)
#                     print("wocao")
#                     data_packet = json.loads(message)
#                     await self.queue.put(data_packet)
#                     await self.handle_data()
#                     print("End Loop")
#                     break
#                 except asyncio.TimeoutError:
#                     print("接收数据超时")
#                     continue
#                 except websockets.ConnectionClosed:
#                     print("连接已关闭")
#                     raise asyncio.CancelledError
#
#             except Exception as e:
#                 print(f"An error occurred: {e}")
#
#
#     async def handle_data(self):
#         print("into handle_data")
#         tasks = []
#         while not self.queue.empty():
#             for _ in range(self.concurrency):
#                 data_packet = await self.queue.get()
#                 if data_packet is None:
#                     break
#                 # task = asyncio.create_task(self.forward_data(data_packet))
#                 # tasks.append(task)
#                 await self.forward_data(data_packet)
#                 print("Added task")
#                 break
#             # await asyncio.gather(*tasks)
#             print("End handle_data")
#
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
#             except Exception as e:
#                 print(f"An error occurred: {e}")
#         print("End forward_data")
#
# async def main():
#     print("Connecting to server")
#     server_info = requests.get("http://localhost:8000/get_server_url/").json()
#     client = Client(server_info["server_url"])
#     await client.connect_to_server()
#     print("Connection closed")
#
#
# asyncio.run(main())
# asyncio.get_event_loop().run_until_complete(main())

async def forward_data(queue, semaphore):
    async with aiohttp.ClientSession() as session:
        while True:
            datapack = await queue.get()
            # 使用信号量控制并发度
            await semaphore.acquire()
            await asyncio.create_task(handle_forward_data(datapack, session, semaphore))
            queue.task_done()

async def handle_forward_data(data_packet, session, semaphore):
    try:
        headers = data_packet.get("header", {})
        url = data_packet.get("addr")
        data = data_packet.get("data")
        async with session.post(url, headers=headers, data=data) as response:
            if response.status == 200:
                print(f"Successfully forwarded: {data}")
            else:
                print(f"Failed to forward: {data}, status: {response.status}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        semaphore.release()

async def receive_data(uri, queue):
    async with websockets.connect(uri) as websocket:
        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=3)
                print(f"Received message: {message}")
                await queue.put(message)
            except asyncio.TimeoutError:
                print("Timeout waiting for message")
                # 可以在这里添加重新连接逻辑或其他操作

async def main():
    uri = "ws://your.websocket.server/endpoint"
    queue = asyncio.Queue()
    # 设置最大并发任务数量
    semaphore = asyncio.Semaphore(10)

    receiver = asyncio.create_task(receive_data(uri, queue))
    forwarder = asyncio.create_task(forward_data(queue, semaphore))

    await asyncio.gather(receiver, forwarder)

# 使用 asyncio.run() 运行异步任务
asyncio.run(main())