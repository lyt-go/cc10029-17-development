"""集成测试：冲突裁决生效清单 → 传输血缘消费 → 目标合并核对链路。

覆盖批量冲突裁决产出生效清单、单项复核更正与批量确认的竞态保护、
一轮提交同字段只生效一次、血缘只合并实际确认条目、重发原批的幂等写入。
"""
from __future__ import annotations

import time

import pytest

from app.services.conflict_resolve import ConflictResolveService
from app.services.transfer_lineage import TransferLineageService
from app.store import store


@pytest.fixture(autouse=True)
def cleanup():
    """每个测试前重置store数据，确保测试隔离（文件清理被沙箱限制，仅append不影响结果）。"""
    from app.seed import SEED_ROWS
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    yield


def _unique_batch(tag: str) -> str:
    """生成每次运行唯一的批次编号，避免磁盘产物跨运行串扰。"""
    return f"TEST-EFF-{tag}-{int(time.time() * 1000)}"


def _conflict_id(code: str) -> int:
    return [r["id"] for r in store.rows("conflict_resolve") if r.get("冲突编号") == code][0]


def _prepare_three_resolved(cn_svc: ConflictResolveService, batch: str) -> list[str]:
    """创建同批三笔冲突并分别裁决：源端优先 / 目标端优先 / 源端优先。"""
    created = cn_svc.create_conflict(batch, 3)
    assert created["conflicts_created"] == 3
    codes = created["codes"]
    cn_svc.resolve_conflict(_conflict_id(codes[0]), "source_first")
    cn_svc.resolve_conflict(_conflict_id(codes[1]), "target_first")
    cn_svc.resolve_conflict(_conflict_id(codes[2]), "source_first")
    return codes


class TestEffectiveListBatchCommit:
    """生效清单：三笔冲突一轮提交，单项复核更正后按当前值生效。"""

    def test_commit_after_single_review_uses_corrected_value(self):
        """同批三笔准备一次提交，一笔经单项复核改了目标值，确认时不得用预览值盖掉更正。"""
        cn_svc = ConflictResolveService()
        batch = _unique_batch("RACE")
        codes = _prepare_three_resolved(cn_svc, batch)

        # 准备一轮提交：三笔预览均停在字段版本1
        prepared = cn_svc.prepare_batch_commit(batch)
        assert prepared["count"] == 3
        assert all(e["field_version"] == 1 for e in prepared["entries"])

        # 单项复核：更正第二笔（目标端优先）的目标值 → 字段版本升到2，采用值跟随
        review = cn_svc.review_conflict(_conflict_id(codes[1]), "tgt_val_1_corrected")
        assert review["字段版本"] == 2
        assert review["采用值"] == "tgt_val_1_corrected"

        # 发出原来的批量确认
        commit = cn_svc.confirm_batch_commit(batch)
        assert commit["effective_count"] == 3

        # 未变化的两笔按预览生效；更正笔显式标记版本漂移，采用更正值而非预览值
        drifted = {e["conflict_code"]: e for e in commit["drifted"]}
        assert list(drifted) == [codes[1]]
        corrected = drifted[codes[1]]
        assert corrected["adopted_value"] == "tgt_val_1_corrected"
        assert corrected["field_version"] == 2
        assert corrected["preview_version"] == 1
        assert corrected["version_drift"] is True

        # 生效清单落盘：每笔可核对采用值、裁决与字段版本
        effective = {e["conflict_code"]: e for e in cn_svc.get_effective_list(batch)}
        assert len(effective) == 3
        assert effective[codes[0]]["adopted_value"] == "src_val_0"
        assert effective[codes[0]]["adjudication"] == "源端优先"
        assert effective[codes[0]]["field_version"] == 1
        assert effective[codes[1]]["adopted_value"] == "tgt_val_1_corrected"
        assert effective[codes[1]]["adjudication"] == "目标端优先"
        assert effective[codes[1]]["field_version"] == 2
        assert effective[codes[2]]["adopted_value"] == "src_val_2"

    def test_same_field_effective_once_per_round(self):
        """一轮提交里同个字段（记录主键+冲突字段）只能生效一次。"""
        cn_svc = ConflictResolveService()
        batch = _unique_batch("DEDUP")
        # 同批两笔冲突指向同一记录主键+冲突字段
        for code in ("CONF-DUP-1", "CONF-DUP-2"):
            entry, missing = cn_svc.create_entry({
                "冲突编号": code, "记录主键": "PK_DUP", "冲突字段": "field_dup",
                "源端取值": f"src_{code}", "目标取值": f"tgt_{code}",
                "裁决方式": "待裁决", "裁决依据": "", "裁决时间": "2026-11-24",
            })
            assert not missing
            entry["批次编号"] = batch
            entry["字段版本"] = 1
            cn_svc.resolve_conflict(entry["id"], "source_first")

        prepared = cn_svc.prepare_batch_commit(batch)
        assert prepared["count"] == 1
        commit = cn_svc.confirm_batch_commit(batch)
        assert commit["effective_count"] == 1
        assert len(cn_svc.get_effective_list(batch)) == 1

    def test_confirm_without_prepare_rejected(self):
        """未准备的批次不能确认提交。"""
        cn_svc = ConflictResolveService()
        batch = _unique_batch("NOPREP")
        result = cn_svc.confirm_batch_commit(batch)
        assert "error" in result


class TestLineageConsumeAndMerge:
    """血缘消费生效清单并执行目标合并，只处理实际确认条目且写入幂等。"""

    def test_consume_writes_source_adopted_and_target_range(self):
        """血缘消费生效清单，逐笔写下源值、采用值和目标端数据范围。"""
        cn_svc = ConflictResolveService()
        tl_svc = TransferLineageService()
        batch = _unique_batch("LINEAGE")
        codes = _prepare_three_resolved(cn_svc, batch)
        cn_svc.prepare_batch_commit(batch)
        commit = cn_svc.confirm_batch_commit(batch)

        # 确认时自动驱动血缘消费
        assert commit["auto_lineage"]["consumed"] == 3

        lineage_file = tl_svc._data_dir("lineage") / f"{batch}_adjudication_lineage.jsonl"
        records = {r["conflict_code"]: r for r in tl_svc._read_jsonl(lineage_file)}
        assert len(records) == 3
        first = records[codes[0]]
        assert first["source_value"] == "src_val_0"
        assert first["adopted_value"] == "src_val_0"
        assert first["target_range"] == {
            "record_key": first["record_key"], "field": "field_0", "version": 1,
        }
        assert first["confirmed"] is True

        # 重复消费不重复写入
        again = tl_svc.consume_effective_list(batch)
        assert again["consumed"] == 0
        assert len(tl_svc._read_jsonl(lineage_file)) == 3

    def test_merge_only_processes_confirmed_entries(self):
        """从血缘执行合并时只处理实际确认的条目。"""
        cn_svc = ConflictResolveService()
        tl_svc = TransferLineageService()
        batch = _unique_batch("MERGE")
        _prepare_three_resolved(cn_svc, batch)
        cn_svc.prepare_batch_commit(batch)
        cn_svc.confirm_batch_commit(batch)

        # 血缘里混入一条未确认记录，合并时必须跳过
        lineage_file = tl_svc._data_dir("lineage") / f"{batch}_adjudication_lineage.jsonl"
        tl_svc._write_jsonl(lineage_file, [{
            "batch_code": batch, "conflict_code": "CONF-UNCONFIRMED",
            "record_key": "PK_GHOST", "field": "field_ghost",
            "source_value": "ghost_src", "adopted_value": "ghost_adopted",
            "adjudication": "源端优先", "field_version": 1,
            "target_range": {"record_key": "PK_GHOST", "field": "field_ghost", "version": 1},
            "confirmed": False, "consumed_at": time.time(),
        }])

        merge = tl_svc.merge_from_lineage(batch)
        assert merge["merged"] == 3
        merge_file = tl_svc._data_dir("lineage_merge") / f"{batch}_target_merge.jsonl"
        applied = tl_svc._read_jsonl(merge_file)
        assert len(applied) == 3
        assert all(m["record_key"] != "PK_GHOST" for m in applied)

    def test_full_flow_verify_and_idempotent_recommit(self):
        """端到端：更正后确认原批 → 血缘合并 → 核对三笔裁决；重发原批每笔只生效一次。"""
        cn_svc = ConflictResolveService()
        tl_svc = TransferLineageService()
        batch = _unique_batch("E2E")
        codes = _prepare_three_resolved(cn_svc, batch)

        cn_svc.prepare_batch_commit(batch)
        cn_svc.review_conflict(_conflict_id(codes[1]), "tgt_val_1_corrected")
        commit = cn_svc.confirm_batch_commit(batch)
        assert commit["effective_count"] == 3

        # 从血缘执行合并
        merge = tl_svc.merge_from_lineage(batch)
        assert merge["merged"] == 3

        # 按目标合并结果核对三笔分别采用的裁决
        verify = tl_svc.verify_effective_merge(batch)
        assert verify["all_matched"] is True
        by_code = {c["conflict_code"]: c for c in verify["checks"]}
        assert by_code[codes[0]]["adjudication"] == "源端优先"
        assert by_code[codes[0]]["applied_value"] == "src_val_0"
        assert by_code[codes[0]]["applied_version"] == 1
        assert by_code[codes[1]]["adjudication"] == "目标端优先"
        assert by_code[codes[1]]["applied_value"] == "tgt_val_1_corrected"
        assert by_code[codes[1]]["applied_version"] == 2
        assert by_code[codes[2]]["adjudication"] == "源端优先"
        assert by_code[codes[2]]["applied_value"] == "src_val_2"

        # 重发原批并再次确认更正项：每笔只能产生一次有效写入
        cn_svc.review_conflict(_conflict_id(codes[1]), "tgt_val_1_corrected")
        cn_svc.prepare_batch_commit(batch)
        recommit = cn_svc.confirm_batch_commit(batch)
        assert recommit["effective_count"] == 0
        assert len(recommit["skipped"]) == 3
        assert len(cn_svc.get_effective_list(batch)) == 3

        remerge = tl_svc.merge_from_lineage(batch)
        assert remerge["merged"] == 0
        assert remerge["skipped"] == 3

        # 核对结果在重发后依然成立
        reverify = tl_svc.verify_effective_merge(batch)
        assert reverify["all_matched"] is True
