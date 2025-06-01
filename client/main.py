from argparse import ArgumentParser

from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_response import Response

# main不在项目根目录 识别项目根目录里的模块
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from conf import app_cfg, ServerConfig

parser = ArgumentParser(description="Raft Server")
parser.add_argument(
    "--my_id",
    type=int,
    required=False,
    help="my id",
)
args = parser.parse_args()


def raft_data_port() -> int | None:
    """data port, providing for client"""
    my_id: int = args.my_id
    if not my_id:
        my_id = app_cfg.my_id
    assert my_id, 'my_id不能为空'
    servers: list[ServerConfig] = app_cfg.servers
    for server in servers:
        if my_id != server.id: continue
        return server.data_port
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
        print(f'收到key={k} val={v}')
        return web.json_response({'ret': 'ok'})
    except Exception as e:
        return web.json_response({'ret': 'error', 'msg': str(e)}, status=400)


def main() -> None:
    app = web.Application()
    app.add_routes(routes)
    web.run_app(app, port=raft_data_port())


if __name__ == '__main__':
    main()
