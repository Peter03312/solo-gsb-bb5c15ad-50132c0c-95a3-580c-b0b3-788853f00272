"""WebSocket 快照水位协商验证（python3 tests/test_ws_watermark.py）。

本机最小环境没有 fastapi（其在 Docker 镜像内安装）：复用 test_poll_http 里
装好的第三方包 stub 与真实 app.main，直接 await 两个 WebSocket 路由函数，
WebSocket 用仅记录发送消息的假对象顶替，验证本轮修复口径：

1. 房间连接（/ws/{code}，主持人与参与者）与观察连接（/ws/observe/{token}）
   的快照 last_event_id 一律取状态锁内本房间 MAX(events.id)，客户端提示值
   再高（旧库 / 其他房间 / 被篡改）也不抬高水位；
2. 快照之后落库的本房间新事件按全局递增 ID 正常广播给该连接，浏览器按
   “id > 快照水位”规则会照常应用（修复前水位被抬到未来，这些事件全被丢弃）；
3. 普通重连（提示值等于或低于水位）仍下发全量快照，快照水位不回退，
   之后的新事件只投递一次（不重复、不回退）；
4. 房间码/加入码无效仍以 4404 拒绝，观察凭证无效仍以 4403 拒绝——
   拒绝行为与消息结构维持原样。
"""
import asyncio
import os
import sys
import tempfile
from pathlib import Path

# test_poll_http 在导入时安装 stub 并导入真实 app.main / app.db
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_poll_http as base  # noqa: E402

main = base.main
dbmod = base.dbmod
Harness = base.Harness
run = base.run


class FakeWebSocket:
    """记录 send_json 消息的最小 WebSocket；receive_text 阻塞直到测试放行，
    放行后抛 WebSocketDisconnect，让路由走正常的 finally 清理。"""

    def __init__(self):
        self.sent = []
        self.closed_code = None
        self._release = asyncio.Event()

    async def accept(self):
        pass

    async def send_json(self, message):
        self.sent.append(message)

    async def close(self, code=None):
        self.closed_code = code

    async def receive_text(self):
        await self._release.wait()
        raise main.WebSocketDisconnect()

    def release(self):
        self._release.set()


async def connect_and_snapshot(route, *args, **kwargs):
    """运行 WS 路由直到首条消息（快照或 error）下发，返回 (task, ws)。"""
    ws = FakeWebSocket()
    task = asyncio.create_task(route(ws, *args, **kwargs))
    while not ws.sent:
        await asyncio.sleep(0)
    return task, ws


def room_max_event_id(room_id: int) -> int:
    return main.db.execute(
        "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_id = ?", (room_id,)
    ).fetchone()["m"]


def test_room_ws_snapshot_watermark_pinned_and_events_flow():
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "ws.db"))
        # 房间 A：1 个组（全局事件 1=group_added）
        room_a = run(main.create_room(main.RoomIn(name="甲房间")))
        code_a = room_a["code"]
        g_a = run(main.add_group(code_a, main.GroupIn(name="红队")))
        row_a = main.db.execute("SELECT * FROM rooms WHERE code=?", (code_a,)).fetchone()
        max_a = room_max_event_id(row_a["id"])

        # 房间 B：组 + 卡片，制造比房间 A 更高的全局事件 ID
        room_b = run(main.create_room(main.RoomIn(name="乙房间")))
        code_b = room_b["code"]
        g_b = run(main.add_group(code_b, main.GroupIn(name="蓝队")))
        card_b = run(main.add_card(code_b, main.CardIn(
            title="乙房间卡片", trigger_seconds=0)))
        row_b = main.db.execute("SELECT * FROM rooms WHERE code=?", (code_b,)).fetchone()
        max_b = room_max_event_id(row_b["id"])
        assert max_b > max_a  # 事件 ID 全库自增：房间 B 水位高于房间 A

        # --- 主持人连接房间 A，提示值来自房间 B（且再恶意抬高） ---
        inflated = max_b + 99999

        async def scenario():
            host_task, host_ws = await connect_and_snapshot(
                main.room_ws, code_a, last_event_id=inflated)
            snap = host_ws.sent[0]
            assert snap["kind"] == "snapshot"
            # 水位不被提示值抬高：就是锁内房间 A 的真实水位
            assert snap["last_event_id"] == max_a
            # 快照内容仍是房间 A 的全量状态（房间 A 的组，不见房间 B 的卡片）
            assert {g["id"] for g in snap["groups"]} == {g_a["id"]}
            assert all(c["id"] != card_b["id"] for c in snap["cards"])

            # --- 参与者（组加入码）同样口径（此时房间 A 水位仍为 max_a） ---
            player_task, player_ws = await connect_and_snapshot(
                main.room_ws, g_a["join_code"], last_event_id=inflated)
            psnap = player_ws.sent[0]
            assert psnap["kind"] == "snapshot"
            assert psnap["scope"]["role"] == "player"
            assert psnap["scope"]["group_id"] == g_a["id"]
            assert psnap["last_event_id"] == max_a  # 不被跨房间提示值抬高

            # 快照之后房间 A 产生新事件：ID 必然大于快照水位，主持人连接应收到
            async with main.state_lock:
                eid = await main.publish(
                    code_a, row_a["id"], "clock",
                    {"action": "paused", "status": "paused", "elapsed_ms": 0})
            await asyncio.sleep(0)
            assert eid > max_a  # 浏览器“id > 水位”规则会应用
            ev = host_ws.sent[1]
            assert ev["kind"] == "event" and ev["id"] == eid
            assert sum(1 for m in host_ws.sent if m.get("id") == eid) == 1  # 不重复投递

            host_ws.release()
            player_ws.release()
            await host_task
            await player_task
            return host_ws

        ws = run(scenario())
        assert ws.closed_code is None  # 正常断线，非服务端拒绝

        # --- 普通重连：提示值等于/低于真实水位，全量快照照发、水位不回退 ---
        # 注意事件 ID 全库自增：房间 A 新 clock 事件的全局 ID 大于 max_a+1，
        # 快照水位仍取本房间真实 MAX(events.id)（动态读取，不按条数推算）。
        current_max = room_max_event_id(row_a["id"])

        async def normal_reconnect(hint):
            task, ws = await connect_and_snapshot(main.room_ws, code_a,
                                                  last_event_id=hint)
            snap = ws.sent[0]
            assert snap["last_event_id"] == current_max  # 锁内本房间真实水位
            assert {g["id"] for g in snap["groups"]} == {g_a["id"]}  # 全量快照
            ws.release()
            await task

        run(normal_reconnect(current_max))  # 提示值 == 水位（普通重连）
        run(normal_reconnect(0))            # 刷新/新标签：提示值为 0
        h.close()


def test_observe_ws_snapshot_watermark_pinned_and_reveal_flows():
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "obs.db"))
        room = run(main.create_room(main.RoomIn(name="观察房")))
        code = room["code"]
        g = run(main.add_group(code, main.GroupIn(name="观察组")))
        # 一张全员卡（观察席可见其揭示），先不揭示
        card = run(main.add_card(code, main.CardIn(
            title="全员情景", trigger_seconds=0)))
        share = run(main.create_share(code, main.ShareIn(
            group_ids=[g["id"]], minutes=30)))
        token = share["token"]
        row = main.db.execute("SELECT * FROM rooms WHERE code=?", (code,)).fetchone()
        max_id = room_max_event_id(row["id"])

        # 提示值远高于本房间水位（旧库 / 其他房间）：快照水位仍钉在真实水位
        async def observer_case():
            task, ws = await connect_and_snapshot(main.observe_ws, token,
                                                  last_event_id=max_id + 99999)
            snap = ws.sent[0]
            assert snap["kind"] == "snapshot"
            assert snap["scope"]["role"] == "observer"
            assert snap["last_event_id"] == max_id
            assert snap["cards"] == []  # 尚未揭示：观察席看不到

            # 揭示全员卡（复刻 ticker 在 state_lock 内的揭示+广播路径）
            async with main.state_lock:
                main.db.execute(
                    "UPDATE cards SET revealed = 1, revealed_at_ms = ?, timed_out = 0"
                    " WHERE id = ?",
                    (0, card["id"]),
                )
                main.db.commit()
                fresh = main.db.execute(
                    "SELECT * FROM cards WHERE id=?", (card["id"],)).fetchone()
                eid = await main.publish(
                    code, row["id"], "card_revealed", main.card_public(fresh))
            await asyncio.sleep(0)
            assert eid > snap["last_event_id"]  # 修复前会被抬高的水位丢弃
            ev = ws.sent[1]
            assert ev["kind"] == "event" and ev["type"] == "card_revealed"
            assert ev["payload"]["id"] == card["id"]

            ws.release()
            await task

        run(observer_case())
        h.close()


def test_invalid_credentials_still_rejected():
    with tempfile.TemporaryDirectory() as d:
        h = Harness(os.path.join(d, "deny.db"))
        run(main.create_room(main.RoomIn(name="拒绝房")))

        async def bad_room():
            task, ws = await connect_and_snapshot(main.room_ws, "NOPE00",
                                                  last_event_id=0)
            await task
            assert ws.sent[0] == {"kind": "error",
                                  "message": "room or join code not found"}
            assert ws.closed_code == 4404

        async def bad_token():
            task, ws = await connect_and_snapshot(main.observe_ws, "not-a-token",
                                                  last_event_id=0)
            await task
            assert ws.sent[0]["kind"] == "error"
            assert ws.closed_code == 4403

        run(bad_room())
        run(bad_token())
        h.close()


if __name__ == "__main__":
    test_room_ws_snapshot_watermark_pinned_and_events_flow()
    print("ok: room ws snapshot watermark pinned; post-snapshot events applied once")
    test_observe_ws_snapshot_watermark_pinned_and_reveal_flows()
    print("ok: observe ws snapshot watermark pinned; reveal event applied")
    test_invalid_credentials_still_rejected()
    print("ok: invalid room code -> 4404, invalid observer token -> 4403")
    print("ALL WS WATERMARK TESTS PASSED")
