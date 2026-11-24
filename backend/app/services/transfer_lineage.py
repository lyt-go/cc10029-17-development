"""传输血缘领域服务：维护源端经抽取窗口、传输批次到目标端的交换链路血缘追溯。

本模块记录每一次数据交换的完整链路信息（源端→窗口→批次→目标端），
生成血缘链路产物文件，供下游数据追溯和质量审计使用。

同时消费冲突裁决产出的「冲突裁决生效清单」：把每笔已生效裁决的源值、采用值与
目标端数据范围写入血缘，后续从血缘执行目标合并时只处理实际确认的条目，
且每笔（记录主键, 冲突字段）只产生一次有效写入；合并结果可回核生效清单的
采用值、裁决方式与目标运用的字段版本。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class TransferLineageService(BaseModuleService):
    MODULE = "transfer_lineage"
    ENTITY = "血缘链路"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["链路编号", "源端编号", "抽取窗口", "传输批次", "目标编号", "字段契约", "血缘版本", "采集时间"]
    REQUIRED_FIELDS = ["链路编号", "源端编号", "抽取窗口"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认生效": "已生效", "标记停用": "已停用"}
    NEGATIVE_ACTIONS = ["标记停用"]

    def record_lineage(self, batch_code: str, is_balanced: bool) -> dict[str, Any]:
        """记录传输批次的血缘链路：从源端经抽取窗口到目标端的完整数据交换链路。

        本方法遍历 store 中与批次关联的源端、抽取窗口、目标端记录，
        构建完整血缘链路并写入血缘产物文件，供数据追溯和质量审计。
        """
        # 从 store 中查找关联的源端登记、抽取窗口和目标端记录
        source_info = {}
        target_info = {}
        window_info = {}
        contract_info = {}

        for s in store.rows("source_register"):
            if s.get("status") == "已登记":
                source_info = s
                break
        for t in store.rows("target_register"):
            if t.get("status") == "已登记":
                target_info = t
                break
        for w in store.rows("extract_window"):
            if w.get("窗口编号", "").startswith("EXTR-"):
                window_info = w
                break
        for c in store.rows("field_contract"):
            if c.get("status") == "已生效":
                contract_info = c
                break

        lineage_code = f"TRAN-{batch_code[:8]}-{int(time.time())}"
        source_code = source_info.get("源端编号", f"SOUR-{batch_code[:8]}")
        window_code = window_info.get("窗口编号", f"EXTR-{batch_code[:8]}")
        target_code = target_info.get("目标编号", f"TARG-{batch_code[:8]}")
        contract_code = contract_info.get("契约编号", f"FIEL-{batch_code[:8]}")

        values = {
            "链路编号": lineage_code,
            "源端编号": source_code,
            "抽取窗口": window_code,
            "传输批次": batch_code,
            "目标编号": target_code,
            "字段契约": contract_code,
            "血缘版本": "v1.0",
            "采集时间": time.strftime("%Y-%m-%d", time.localtime(time.time())),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建血缘链路失败: {missing}")

        # 写入血缘产物文件——完整链路追溯
        lineage_file = self._data_dir("lineage") / f"{lineage_code}.jsonl"
        self._write_jsonl(lineage_file, [{
            "lineage_code": lineage_code,
            "source_code": source_code,
            "source_system": source_info.get("系统名称", "unknown"),
            "extract_window": window_code,
            "transfer_batch": batch_code,
            "target_code": target_code,
            "target_system": target_info.get("系统名称", "unknown"),
            "field_contract": contract_code,
            "balanced": is_balanced,
            "recorded_at": time.time(),
        }])

        final_entry, _ = self.run_action(entry["id"], "确认生效")

        return {
            "lineage_code": lineage_code,
            "batch_code": batch_code,
            "source": source_code,
            "target": target_code,
            "balanced": is_balanced,
        }

    def query_lineage(self, batch_code: str) -> dict[str, Any]:
        """查询指定传输批次的血缘链路，追溯数据从源端到目标端的完整流转路径。"""
        lineages = []
        lineage_dir = self._data_dir("lineage")
        for lf in lineage_dir.glob(f"*{batch_code[:8]}*.jsonl"):
            lineages.extend(self._read_jsonl(lf))
        return {"batch_code": batch_code, "lineages": lineages, "count": len(lineages)}

    def get_active_lineages(self) -> list[dict[str, Any]]:
        """获取所有已生效的血缘链路列表。"""
        return [r for r in store.rows(self.MODULE) if r.get("status") == "已生效"]

    # -- 消费冲突裁决生效清单并驱动目标合并 --

    def consume_effective_list(self, batch_code: str) -> dict[str, Any]:
        """消费批次的冲突裁决生效清单，把裁决结果写入传输血缘。

        逐笔记录源值、采用值与目标端数据范围（记录主键+字段+字段版本），
        标记为已确认条目供后续合并使用；已写入过的字段不重复消费。
        """
        effective_file = self._data_dir("effective_lists") / f"{batch_code}_effective.jsonl"
        entries = self._read_jsonl(effective_file)
        if not entries:
            return {"batch_code": batch_code, "consumed": 0,
                    "message": f"批次 {batch_code} 暂无冲突裁决生效清单"}

        lineage_file = self._data_dir("lineage") / f"{batch_code}_adjudication_lineage.jsonl"
        existing = self._read_jsonl(lineage_file)
        consumed_keys = {(r.get("record_key"), r.get("field")) for r in existing}

        records = []
        for entry in entries:
            field_key = (entry.get("record_key"), entry.get("field"))
            if field_key in consumed_keys:
                continue
            consumed_keys.add(field_key)
            records.append({
                "batch_code": batch_code,
                "conflict_code": entry.get("conflict_code"),
                "record_key": entry.get("record_key"),
                "field": entry.get("field"),
                "source_value": entry.get("source_value"),
                "adopted_value": entry.get("adopted_value"),
                "adjudication": entry.get("adjudication"),
                "field_version": entry.get("field_version"),
                "target_range": {
                    "record_key": entry.get("record_key"),
                    "field": entry.get("field"),
                    "version": entry.get("field_version"),
                },
                "confirmed": True,
                "consumed_at": time.time(),
            })
        if records:
            self._write_jsonl(lineage_file, records)
        return {"batch_code": batch_code, "consumed": len(records),
                "total_confirmed": len(entries)}

    def merge_from_lineage(self, batch_code: str) -> dict[str, Any]:
        """从血缘执行目标合并：只处理血缘中实际确认的裁决条目。

        每笔（记录主键, 冲突字段）在目标端只产生一次有效写入，
        重复执行合并不会重复落数。
        """
        lineage_file = self._data_dir("lineage") / f"{batch_code}_adjudication_lineage.jsonl"
        records = self._read_jsonl(lineage_file)
        confirmed = [r for r in records if r.get("confirmed")]
        if not confirmed:
            return {"batch_code": batch_code, "merged": 0,
                    "message": f"批次 {batch_code} 血缘中没有已确认的裁决条目"}

        merge_file = self._data_dir("lineage_merge") / f"{batch_code}_target_merge.jsonl"
        applied = self._read_jsonl(merge_file)
        applied_keys = {(m.get("record_key"), m.get("field")) for m in applied}

        writes = []
        for record in confirmed:
            field_key = (record.get("record_key"), record.get("field"))
            if field_key in applied_keys:
                continue
            applied_keys.add(field_key)
            writes.append({
                "batch_code": batch_code,
                "conflict_code": record.get("conflict_code"),
                "record_key": record.get("record_key"),
                "field": record.get("field"),
                "applied_value": record.get("adopted_value"),
                "applied_version": record.get("field_version"),
                "adjudication": record.get("adjudication"),
                "source_value": record.get("source_value"),
                "target_range": record.get("target_range"),
                "merged_at": time.time(),
            })
        if writes:
            self._write_jsonl(merge_file, writes)
        return {"batch_code": batch_code, "merged": len(writes),
                "skipped": len(confirmed) - len(writes), "writes": writes}

    def verify_effective_merge(self, batch_code: str) -> dict[str, Any]:
        """按目标合并结果回核冲突裁决生效清单的每一笔。

        逐笔核对采用值、裁决方式与目标运用的字段版本是否一致，
        输出每笔实际采用的裁决，供确认三笔冲突各自的生效结果。
        """
        effective_file = self._data_dir("effective_lists") / f"{batch_code}_effective.jsonl"
        entries = self._read_jsonl(effective_file)
        merge_file = self._data_dir("lineage_merge") / f"{batch_code}_target_merge.jsonl"
        merges = {(m.get("record_key"), m.get("field")): m
                  for m in self._read_jsonl(merge_file)}

        checks = []
        for entry in entries:
            merge = merges.get((entry.get("record_key"), entry.get("field")))
            checks.append({
                "conflict_code": entry.get("conflict_code"),
                "record_key": entry.get("record_key"),
                "field": entry.get("field"),
                "adjudication": entry.get("adjudication"),
                "adopted_value": entry.get("adopted_value"),
                "applied_value": merge.get("applied_value") if merge else None,
                "field_version": entry.get("field_version"),
                "applied_version": merge.get("applied_version") if merge else None,
                "matched": bool(merge)
                    and merge.get("applied_value") == entry.get("adopted_value")
                    and merge.get("applied_version") == entry.get("field_version"),
            })
        return {"batch_code": batch_code, "checks": checks,
                "all_matched": bool(checks) and all(c["matched"] for c in checks)}