from argparse import ArgumentParser

from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_response import Response
from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR

# main不在项目根目录 识别项目根目录里的模块
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from conf import app_cfg, ServerConfig
from log import logger
from util import RPC, RPC_Type, RPC_Direction
from role.rpc import PutDataReq

parser = ArgumentParser(description="Raft Server")
parser.add_argument(
    "--my_id",
    type=int,
    required=False,
    help="my id",
)
args = parser.parse_args()


def raft_port() -> tuple[str, int, int]:
    """
    consensus port and data port, providing for client

    Return: tuple(int, int)
        the 1st is local address host
        the 2nd is consensus port
        the 3rd is data port
    """
    my_id: int = args.my_id
    if not my_id:
        my_id = app_cfg.my_id
    assert my_id, 'my_id不能为空'
    servers: list[ServerConfig] = app_cfg.servers
    for server in servers:
        if my_id != server.id: continue
        return server.ip, server.port, server.data_port
    exit('没有配置data_port')


routes = web.RouteTableDef()


@routes.get('/raft/get/{key}')
async def get_key(req: Request) -> Response:
    key: str = req.match_info['key']
    print(key)
    return web.json_response({'ret': f'the recv key is {key}'})


@routes.post('/raft/put')
async def put_kv(req: Request) -> Response:
    try:
        data = await req.json()
        k: str = data.get('key')
        v: str = data.get('val')
        logger.info(f'收到key={k} val={v}')
        sock: socket = socket(AF_INET, SOCK_DGRAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        try:
            host, consensus_port, _ = raft_port()
            rpc: RPC = RPC(
                direction=RPC_Direction.REQ,
                type=RPC_Type.CLIENT_PUT,
                content=PutDataReq(key=k, val=v).json()
            )
            logger.info(f'向{host}:{consensus_port}转发要存储的数据{rpc}')
            sock.sendto(f"{rpc.json()}\n".encode(), (host, consensus_port))
        except Exception as e:
            logger.error(f"socket转发失败{e}")
        return web.json_response({'ret': 'ok'})

    except Exception as e:
        return web.json_response({'ret': 'error', 'msg': str(e)}, status=400)


def main() -> None:
    app = web.Application()
    app.add_routes(routes)
    _, _, data_port = raft_port()
    web.run_app(app, port=data_port)


if __name__ == '__main__':
    main()
